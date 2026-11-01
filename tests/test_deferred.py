"""Synthetic lazy-observer attribution and scope transparency controls (A7/A8)."""
import asyncio
from contextlib import closing, nullcontext
import json
import sqlite3

import pytest
import pytest_check as check

from mcp_flightrecorder import Recorder

COVERAGE = {"producers": {"model": "model", "host": "host", "mcp": "mcp"}}


def accepted(receipt):
    assert receipt.status == "accepted", f"The fixture must establish its intended scope/operation before dependent checks: {receipt.reason}."
    return receipt


def read(path, sql):
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        db.row_factory = sqlite3.Row
        return [dict(row) for row in db.execute(sql)]


def observer_factory(rec, arguments, receipts, host_steps):
    """The synthetic factory captures identity now and submits mutable data on first consumption."""
    snapshot = rec.capture_scope()

    def lazy_work():
        start = rec.start_operation(scope=snapshot, client_id="lazy-client", method="tools/call",
                                    name="lazy-tool", arguments=arguments, link_basis="host_hook")
        receipts.append(start)
        host_steps.append("started")
        try:
            yield "chunk-one"
            host_steps.append("resumed")
            yield "chunk-two"
        finally:
            host_steps.append("cleanup")
            if start.status == "accepted":
                receipts.append(rec.finish_operation(start.ticket, status="cancelled"))

    return lazy_work()


@pytest.mark.parametrize("start_context", ["other-turn", "unscoped"], ids=["execute-in-B", "execute-unscoped"])
@pytest.mark.parametrize("finish_context", ["other-turn", "unscoped"], ids=["finish-in-B", "finish-unscoped"])
def test_deferred_observer_keeps_creation_scope_and_freezes_at_submission(tmp_path, start_context, finish_context):
    rec = Recorder(tmp_path / "capture.sqlite", capture="content", coverage=COVERAGE, limits={"batch_items": 1})
    receipts, host_steps, arguments = [], [], {"version": 1}
    work = None
    try:
        with rec.turn("creation turn") as creation:
            accepted(creation.receipt)
            with rec.dispatch(action="execute", name="lazy-tool", arguments={}) as dispatch:
                accepted(dispatch.receipt)
                snapshot = rec.capture_scope()
                work = observer_factory(rec, arguments, receipts, host_steps)
        check.equal((rec.health().open_operations, rec.health().completion.used_slots), (0, 0),
                    msg="Creating an unconsumed lazy object must not start an operation or reserve terminal capacity.")
        arguments["version"] = 2
        start_scope = rec.turn("execution turn") if start_context == "other-turn" else nullcontext()
        with start_scope:
            check.equal(next(work), "chunk-one", msg="Observation must preserve the first lazily consumed host chunk.")
        start = accepted(receipts[0])
        with rec._condition:
            assert rec._condition.wait_for(lambda: ("event", start.event_id) not in rec._pending, timeout=5), (
                "The actual start must commit before checking live turn completeness."
            )
        before_close = read(tmp_path / "capture.sqlite", "SELECT turn_id,capture_state FROM turns")
        by_turn = {row["turn_id"]: row["capture_state"] for row in before_close}
        assert creation.receipt.turn_id in by_turn, "The creation turn must be persisted before inspecting its open-capture state."
        check.equal(by_turn[creation.receipt.turn_id], "unknown",
                    msg="Lexical turn exit cannot claim complete capture while its deferred operation may still emit evidence.")
        finish_scope = rec.turn("cleanup turn") if finish_context == "other-turn" else nullcontext()
        with finish_scope:
            work.close()
        terminal = accepted(receipts[1])
        check.equal(start.turn_id, snapshot.turn_id, msg="Actual lazy execution keeps the turn frozen at factory invocation.")
        check.equal(terminal.turn_id, snapshot.turn_id, msg="Terminal callbacks keep the ticket's original turn under a different cleanup context.")
        closed = rec.close()
        assert closed.writer_stopped, "The lazy observer's terminal must be committed before inspecting its persisted attribution."
        operation = read(tmp_path / "capture.sqlite", "SELECT turn_id,dispatch_event_id,status FROM operations")
        assert len(operation) == 1, "The consumed lazy object must create exactly one logical operation."
        check.equal(operation[0], {"turn_id": creation.receipt.turn_id,
                                   "dispatch_event_id": dispatch.receipt.event_id, "status": "cancelled"},
                    msg="Scope restoration cannot reattribute an operation or its cleanup to the later ambient turn/dispatch.")
        payloads = read(tmp_path / "capture.sqlite", """SELECT a.body_text FROM events e JOIN artifacts a
            ON a.artifact_id=e.payload_ref WHERE e.kind='mcp.request'""")
        assert len(payloads) == 1 and payloads[0]["body_text"] is not None, "The full-content start must retain its exposed arguments."
        check.equal(json.loads(payloads[0]["body_text"])["arguments"], {"version": 2},
                    msg="Submission must observe mutation after factory creation instead of freezing payloads too early.")
        check.equal(host_steps, ["started", "cleanup"], msg="Recording must not drain unconsumed host chunks during cleanup.")
        check.equal(rec.health().open_operations, 0, msg="One terminal commit must release the deferred operation's reservation.")
    finally:
        if work is not None:
            work.close()
        rec.close()


def test_recorder_close_stops_capture_without_cancelling_or_draining_host_work(tmp_path):
    rec = Recorder(tmp_path / "capture.sqlite", capture="content", coverage=COVERAGE)
    receipts, steps = [], []
    work = None
    try:
        with rec.turn("still running host work"):
            with rec.dispatch(action="execute", name="lazy-tool", arguments={}):
                work = observer_factory(rec, {}, receipts, steps)
        check.equal(next(work), "chunk-one", msg="The caller must retain control of the first host chunk.")
        closed = rec.close()
        assert closed.writer_stopped and closed.portable, "The post-close callback control requires a finalized standalone recording."
        before = (tmp_path / "capture.sqlite").read_bytes()
        check.equal(steps, ["started"], msg="Closing capture must neither drain nor cancel still-live host work.")
        check.equal(next(work), "chunk-two", msg="Host iteration must continue after recorder shutdown.")
        with check.raises(StopIteration, msg="The host must finish on its own iterator boundary after the remaining chunk."):
            next(work)
        check.equal(steps, ["started", "resumed", "cleanup"], msg="Host cleanup remains owned by the host's actual lifecycle.")
        assert len(receipts) == 2, "The synthetic observer must attempt exactly one late terminal callback."
        check.equal((receipts[1].status, receipts[1].reason), ("closed", "closed"),
                    msg="A late callback cannot reopen capture or mutate shutdown's incomplete observation.")
        check.equal((tmp_path / "capture.sqlite").read_bytes(), before,
                    msg="Post-shutdown callbacks must leave finalized recording bytes unchanged.")
        check.equal(read(tmp_path / "capture.sqlite", "SELECT status,end_event_id FROM operations"),
                    [{"status": "incomplete", "end_event_id": None}],
                    msg="Shutdown records unfinished capture without inventing host cancellation or success.")
        check.equal((rec.health().completion.used_slots, rec.health().open_operations), (0, 0),
                    msg="Shutdown must reclaim capture reservations while host work continues independently.")
    finally:
        if work is not None:
            work.close()
        rec.close()


def test_rejected_nested_scopes_do_not_fall_back_to_outer_attribution(tmp_path):
    rec = Recorder(tmp_path / "capture.sqlite", capture="content", coverage=COVERAGE)
    try:
        with rec.turn("outer") as outer:
            accepted(outer.receipt)
            outer_scope = rec.capture_scope()
            with rec.turn(object()) as rejected:
                check.equal(rejected.receipt.reason, "invalid_input", msg="Unsupported turn input must reject entry as a receipt.")
                wrong = rec.observe("cache.served", scope=rec.capture_scope(), facts={"owner": "host"})
                check.equal(wrong.reason, "invalid_scope",
                            msg="A rejected nested turn must disable attribution instead of falling back to its outer turn.")
            check.equal(rec.capture_scope(), outer_scope, msg="Rejected-turn exit must restore the caller's preceding valid scope.")
            with rec.dispatch(action="execute", name="outer-tool", arguments={}) as dispatch:
                accepted(dispatch.receipt)
                with rec.dispatch(action="bad-action", name="inner-tool", arguments={}) as inner:
                    check.equal(inner.receipt.reason, "invalid_input", msg="Invalid dispatch action must reject the dispatch envelope.")
                    scope = rec.capture_scope()
                    check.equal(scope.turn_id, outer.receipt.turn_id, msg="A rejected dispatch preserves the valid surrounding turn.")
                    check.is_none(scope.dispatch_event_id, msg="A rejected nested dispatch cannot inherit its outer dispatch association.")
                    start = accepted(rec.start_operation(scope=scope, client_id="client", method="tools/call", arguments={}, link_basis="none"))
                    accepted(rec.finish_operation(start.ticket, status="ok"))
                check.equal(rec.capture_scope().dispatch_event_id, dispatch.receipt.event_id,
                            msg="Rejected-dispatch exit must restore the explicit preceding dispatch context.")
        rec.close()
        operation = read(tmp_path / "capture.sqlite", "SELECT turn_id,dispatch_event_id,link_basis FROM operations")
        check.equal(operation, [{"turn_id": outer.receipt.turn_id, "dispatch_event_id": None, "link_basis": "none"}],
                    msg="An unassigned operation under a rejected dispatch must remain unassigned in persistent evidence.")
    finally:
        rec.close()


@pytest.mark.parametrize("outcome", ["error", "cancelled"], ids=["error", "cancelled"])
def test_turn_exit_preserves_host_exception_identity_and_observed_outcome(tmp_path, outcome):
    rec = Recorder(tmp_path / "capture.sqlite", capture="content", coverage=COVERAGE)
    error = RuntimeError("host error") if outcome == "error" else asyncio.CancelledError("host cancellation")
    try:
        # Scope exit must propagate the exact host exception, including BaseException cancellation.
        with pytest.raises(type(error)) as caught:
            with rec.turn("failed host turn") as turn:
                accepted(turn.receipt)
                raise error
        check.is_(caught.value, error, msg="Turn-scope restoration must not suppress or replace host failure identity.")
        accepted(turn.exit_receipt)
        rec.close()
        check.equal(read(tmp_path / "capture.sqlite", "SELECT host_outcome FROM turns"), [{"host_outcome": outcome}],
                    msg="Turn exit must record the actual host error/cancellation as an observation.")
    finally:
        rec.close()


async def scope_generator(rec, kind, finally_scopes):
    context = (rec.turn("inner turn") if kind == "turn" else
               rec.dispatch(action="execute", name="inner-tool", arguments={}))
    with context as inner:
        accepted(inner.receipt)
        try:
            yield inner
        finally:
            finally_scopes.append(rec.capture_scope())


def finish_scoped_operation(rec, scope, *, link_basis="host_hook"):
    start = accepted(rec.start_operation(scope=scope, client_id="scope-client", method="tools/call",
                                         name="scope-tool", arguments={}, link_basis=link_basis))
    terminal = accepted(rec.finish_operation(start.ticket, status="ok"))
    assert (start.turn_id, terminal.turn_id) == (scope.turn_id, scope.turn_id)
    return start, terminal


def close_scope_recording(rec):
    closed = rec.close()
    assert closed.writer_stopped and closed.portable, "Scope evidence must be committed to a portable recording."
    health = rec.health()
    assert (health.writer_errors, health.open_operations, health.completion.used_slots) == (0, 0, 0)


@pytest.mark.parametrize("kind,ending", [
    ("turn", "aclose"), ("turn", "error"), ("turn", "cancelled"),
    ("dispatch", "aclose"), ("dispatch", "error"), ("dispatch", "cancelled"),
], ids=["turn-aclose", "turn-error", "turn-cancelled", "dispatch-aclose", "dispatch-error", "dispatch-cancelled"])
def test_foreign_generator_exit_preserves_host_outcome_and_unrelated_closer(tmp_path, monkeypatch, kind, ending):
    path = tmp_path / "capture.sqlite"
    rec = Recorder(path, capture="metadata", coverage=COVERAGE)
    finally_scopes, inner_end_calls = [], []

    async def scenario():
        with rec.turn("origin turn") as outer:
            accepted(outer.receipt)
            with rec.dispatch(action="execute", name="outer-tool", arguments={}) as dispatch:
                accepted(dispatch.receipt)
                predecessor = rec.capture_scope()
                gen = scope_generator(rec, kind, finally_scopes)
                inner = await anext(gen)
                inner_scope = rec.capture_scope()
                assert inner_scope == inner.scope and inner_scope != predecessor
                if kind == "turn" and ending == "aclose":
                    exit_turn = rec._exit_turn

                    def counted_exit(scope, outcome):
                        if scope.turn_id == inner_scope.turn_id:
                            inner_end_calls.append((scope, outcome))
                        return exit_turn(scope, outcome)

                    monkeypatch.setattr(rec, "_exit_turn", counted_exit)

                async def closer():
                    with rec.turn("unrelated closer turn") as closer_turn:
                        accepted(closer_turn.receipt)
                        with rec.dispatch(action="execute", name="closer-tool", arguments={}) as closer_dispatch:
                            accepted(closer_dispatch.receipt)
                            closer_scope = rec.capture_scope()
                            assert closer_scope.turn_id not in (predecessor.turn_id, inner_scope.turn_id)
                            assert closer_scope.dispatch_event_id not in (
                                predecessor.dispatch_event_id, inner_scope.dispatch_event_id)
                            if ending == "aclose":
                                assert await gen.aclose() is None
                            else:
                                error = (KeyError("host error") if ending == "error" else
                                         asyncio.CancelledError("host cancellation"))
                                try:
                                    await gen.athrow(error)
                                except BaseException as caught:
                                    assert caught is error, "Foreign scope cleanup must preserve the exact host exception."
                                else:
                                    pytest.fail("Generator unwinding must propagate the host exception to its closer.")
                            assert finally_scopes == [closer_scope]
                            assert rec.capture_scope() == closer_scope, "Cleanup must not overwrite an unrelated closer scope."
                        accepted(closer_dispatch.exit_receipt)
                    accepted(closer_turn.exit_receipt)
                    return closer_turn, closer_dispatch

                closer_turn, closer_dispatch = await asyncio.create_task(closer())
                assert rec.capture_scope() == predecessor, "The origin must retire its own closed prefix on lookup."
                start, terminal = finish_scoped_operation(rec, rec.capture_scope())
                accepted(inner.exit_receipt)
                assert inner.exit_receipt.turn_id == inner.scope.turn_id
            accepted(dispatch.exit_receipt)
        accepted(outer.exit_receipt)
        return predecessor, outer, dispatch, inner, closer_turn, closer_dispatch, start, terminal

    try:
        predecessor, outer, dispatch, inner, closer_turn, closer_dispatch, start, terminal = asyncio.run(scenario())
        close_scope_recording(rec)
        assert read(path, "SELECT operation_id,turn_id,dispatch_event_id,status FROM operations") == [{
            "operation_id": start.operation_id, "turn_id": predecessor.turn_id,
            "dispatch_event_id": predecessor.dispatch_event_id, "status": "ok"}]
        turns = {row["turn_id"]: row for row in read(path, "SELECT turn_id,ended_at,host_outcome FROM turns")}
        expected_turns = {outer.receipt.turn_id, closer_turn.receipt.turn_id}
        if kind == "turn":
            expected_turns.add(inner.receipt.turn_id)
            assert turns[inner.receipt.turn_id]["host_outcome"] == ("cancelled" if ending == "cancelled" else "error")
        assert set(turns) == expected_turns
        assert all(row["ended_at"] is not None for row in turns.values())
        assert turns[outer.receipt.turn_id]["host_outcome"] == "returned"
        assert turns[closer_turn.receipt.turn_id]["host_outcome"] == "returned"
        entries = [(dispatch.receipt.event_id, "host.dispatch", outer.receipt.turn_id)]
        if kind == "dispatch":
            assert inner.exit_receipt.event_id is None, "Dispatch exit is receipt-only, with no terminal observation."
            entries.append((inner.receipt.event_id, "host.dispatch", outer.receipt.turn_id))
        entries.extend([(closer_dispatch.receipt.event_id, "host.dispatch", closer_turn.receipt.turn_id),
                        (start.event_id, "mcp.request", predecessor.turn_id),
                        (terminal.event_id, "mcp.response", predecessor.turn_id)])
        assert read(path, "SELECT event_id,kind,turn_id FROM events ORDER BY sequence") == [
            {"event_id": event_id, "kind": event_kind, "turn_id": turn_id} for event_id, event_kind, turn_id in entries]
        if kind == "turn" and ending == "aclose":
            assert inner_end_calls == [(inner.scope, "error")], "Exactly one owner must submit the inner turn-end update."
    finally:
        rec.close()


@pytest.mark.parametrize("case", ["turn-with-predecessor", "dispatch-with-predecessor", "unscoped-turn"])
def test_foreign_generator_exit_normalizes_inherited_closer(tmp_path, case):
    path = tmp_path / "capture.sqlite"
    rec = Recorder(path, capture="metadata", coverage=COVERAGE)
    finally_scopes = []

    async def scenario():
        unscoped = case == "unscoped-turn"
        with (nullcontext() if unscoped else rec.turn("origin turn")) as outer:
            if outer is not None:
                accepted(outer.receipt)
            with (nullcontext() if unscoped else
                  rec.dispatch(action="execute", name="outer-tool", arguments={})) as dispatch:
                if dispatch is not None:
                    accepted(dispatch.receipt)
                predecessor = rec.capture_scope()
                kind = "dispatch" if case == "dispatch-with-predecessor" else "turn"
                gen = scope_generator(rec, kind, finally_scopes)
                inner = await anext(gen)
                inner_scope = rec.capture_scope()
                assert inner_scope == inner.scope and inner_scope != predecessor

                async def closer():
                    assert rec.capture_scope() == inner_scope
                    assert await gen.aclose() is None
                    assert finally_scopes == [inner_scope]
                    assert rec.capture_scope() == predecessor, "An inherited closer must normalize immediately after retirement."

                await asyncio.create_task(closer())
                assert rec.capture_scope() == predecessor
                accepted(inner.exit_receipt)
                if unscoped:
                    assert (predecessor.turn_id, predecessor.dispatch_event_id) == (None, None)
                    receipt = accepted(rec.observe("cache.served", scope=rec.capture_scope(), facts={"owner": "host"}))
                else:
                    receipt, _ = finish_scoped_operation(rec, rec.capture_scope())
            if dispatch is not None:
                accepted(dispatch.exit_receipt)
        if outer is not None:
            accepted(outer.exit_receipt)
        return predecessor, inner, receipt

    try:
        predecessor, inner, receipt = asyncio.run(scenario())
        close_scope_recording(rec)
        if case == "unscoped-turn":
            assert read(path, "SELECT event_id,kind,turn_id FROM events") == [
                {"event_id": receipt.event_id, "kind": "cache.served", "turn_id": None}]
            assert read(path, "SELECT operation_id FROM operations") == []
        else:
            assert read(path, "SELECT operation_id,turn_id,dispatch_event_id,status FROM operations") == [{
                "operation_id": receipt.operation_id, "turn_id": predecessor.turn_id,
                "dispatch_event_id": predecessor.dispatch_event_id, "status": "ok"}]
        turns = {row["turn_id"]: row for row in read(path, "SELECT turn_id,ended_at,host_outcome FROM turns")}
        assert all(row["ended_at"] is not None for row in turns.values())
        if case != "dispatch-with-predecessor":
            assert turns[inner.receipt.turn_id]["host_outcome"] == "error"
        else:
            assert inner.exit_receipt.event_id is None
    finally:
        rec.close()


def test_foreign_closed_prefix_preserves_explicit_lazy_snapshot(tmp_path):
    path = tmp_path / "capture.sqlite"
    rec = Recorder(path, capture="metadata", coverage=COVERAGE)
    receipts, host_steps = [], []
    work = None

    async def scenario():
        nonlocal work
        with rec.turn("origin turn") as outer:
            accepted(outer.receipt)
            with rec.dispatch(action="execute", name="outer-tool", arguments={}) as outer_dispatch:
                accepted(outer_dispatch.receipt)
                predecessor = rec.capture_scope()

                async def generator():
                    nonlocal work
                    with rec.turn("lazy creation turn") as inner_turn:
                        accepted(inner_turn.receipt)
                        with rec.dispatch(action="execute", name="lazy-tool", arguments={}) as inner_dispatch:
                            accepted(inner_dispatch.receipt)
                            snapshot = rec.capture_scope()
                            work = observer_factory(rec, {}, receipts, host_steps)
                            yield inner_turn, inner_dispatch, snapshot

                gen = generator()
                inner_turn, inner_dispatch, snapshot = await anext(gen)
                assert (snapshot.turn_id, snapshot.dispatch_event_id) == (
                    inner_turn.receipt.turn_id, inner_dispatch.receipt.event_id)

                async def closer():
                    assert rec.capture_scope() == snapshot
                    assert await gen.aclose() is None
                    assert rec.capture_scope() == predecessor

                await asyncio.create_task(closer())
                assert rec.capture_scope() == predecessor, "Both retired frames must be skipped to reach the outer predecessor."
                accepted(inner_turn.exit_receipt)
                accepted(inner_dispatch.exit_receipt)
                health = rec.health()
                assert (health.open_operations, health.completion.used_slots) == (0, 0)
                assert receipts == [] and host_steps == [], "Unconsumed work owns no operation or completion reservation."
                with rec.turn("execution turn B") as execution:
                    accepted(execution.receipt)
                    assert next(work) == "chunk-one"
                    assert rec.capture_scope().turn_id == execution.receipt.turn_id
                accepted(execution.exit_receipt)
                with rec.turn("cleanup turn C") as cleanup:
                    accepted(cleanup.receipt)
                    work.close()
                    assert rec.capture_scope().turn_id == cleanup.receipt.turn_id
                accepted(cleanup.exit_receipt)
                assert rec.capture_scope() == predecessor
            accepted(outer_dispatch.exit_receipt)
        accepted(outer.exit_receipt)
        return snapshot, inner_turn

    try:
        snapshot, inner_turn = asyncio.run(scenario())
        assert len(receipts) == 2
        start, terminal = map(accepted, receipts)
        assert (start.turn_id, terminal.turn_id) == (snapshot.turn_id, snapshot.turn_id)
        assert host_steps == ["started", "cleanup"], "Closing lazy work must not resume or drain its next host chunk."
        close_scope_recording(rec)
        assert read(path, "SELECT operation_id,turn_id,dispatch_event_id,status,end_event_id FROM operations") == [{
            "operation_id": start.operation_id, "turn_id": snapshot.turn_id,
            "dispatch_event_id": snapshot.dispatch_event_id, "status": "cancelled", "end_event_id": terminal.event_id}]
        assert read(path, """SELECT e.event_id,e.kind,e.operation_id,e.turn_id FROM operations o
            JOIN events e ON e.event_id=o.end_event_id""") == [{
                "event_id": terminal.event_id, "kind": "mcp.response", "operation_id": start.operation_id,
                "turn_id": snapshot.turn_id}]
        assert read(path, "SELECT event_id FROM events WHERE kind='mcp.response'") == [{"event_id": terminal.event_id}]
        turns = {row["turn_id"]: row for row in read(path, "SELECT turn_id,ended_at,host_outcome FROM turns")}
        assert turns[inner_turn.receipt.turn_id]["host_outcome"] == "error"
        assert all(row["ended_at"] is not None for row in turns.values())
    finally:
        if work is not None:
            work.close()
        rec.close()


def test_normal_exit_keeps_inherited_child_scope(tmp_path):
    path = tmp_path / "capture.sqlite"
    rec = Recorder(path, capture="metadata", coverage=COVERAGE)

    async def scenario():
        ready, release = asyncio.Event(), asyncio.Event()
        with rec.turn("parent turn") as parent:
            accepted(parent.receipt)
            with rec.dispatch(action="execute", name="parent-tool", arguments={}) as dispatch:
                accepted(dispatch.receipt)
                snapshot = rec.capture_scope()

                async def child():
                    inherited = rec.capture_scope()
                    ready.set()
                    await release.wait()
                    assert inherited == snapshot
                    assert rec.capture_scope() == snapshot, "Ordinary parent exit must not retire a copied child's scope."
                    return accepted(rec.observe("cache.served", scope=rec.capture_scope(), facts={"owner": "host"}))

                task = asyncio.create_task(child())
                await ready.wait()
            accepted(dispatch.exit_receipt)
        accepted(parent.exit_receipt)
        try:
            unscoped = rec.capture_scope()
            assert (unscoped.turn_id, unscoped.dispatch_event_id) == (None, None)
        finally:
            release.set()
        receipt = await task
        return parent, dispatch, snapshot, receipt

    try:
        parent, dispatch, snapshot, receipt = asyncio.run(scenario())
        assert receipt.turn_id == snapshot.turn_id == parent.receipt.turn_id
        close_scope_recording(rec)
        assert read(path, "SELECT event_id,kind,turn_id FROM events ORDER BY sequence") == [
            {"event_id": dispatch.receipt.event_id, "kind": "host.dispatch", "turn_id": snapshot.turn_id},
            {"event_id": receipt.event_id, "kind": "cache.served", "turn_id": snapshot.turn_id}]
        turns = read(path, "SELECT turn_id,ended_at,host_outcome FROM turns")
        assert len(turns) == 1 and turns[0]["turn_id"] == snapshot.turn_id
        assert turns[0]["ended_at"] is not None and turns[0]["host_outcome"] == "returned"
    finally:
        rec.close()


@pytest.mark.parametrize("predecessor", ["disabled-turn", "rejected-dispatch"])
def test_foreign_closed_dispatch_stops_at_rejected_predecessor(tmp_path, predecessor):
    path = tmp_path / "capture.sqlite"
    rec = Recorder(path, capture="metadata", coverage=COVERAGE)

    async def scenario():
        with rec.turn("outer turn") as outer:
            accepted(outer.receipt)
            with rec.dispatch(action="execute", name="outer-tool", arguments={}) as outer_dispatch:
                accepted(outer_dispatch.receipt)
                outer_scope = rec.capture_scope()
                context = (rec.turn(object()) if predecessor == "disabled-turn" else
                           rec.dispatch(action="bad-action", name="rejected-tool", arguments={}))
                with context as rejected:
                    assert (rejected.receipt.status, rejected.receipt.reason) == ("rejected", "invalid_input")
                    rejected_scope = rec.capture_scope()
                    assert rejected_scope.dispatch_event_id is None
                    assert rejected_scope.turn_id == (None if predecessor == "disabled-turn" else outer.receipt.turn_id)

                    async def generator():
                        with rec.dispatch(action="execute", name="inner-tool", arguments={}) as inner:
                            if predecessor == "disabled-turn":
                                assert (inner.receipt.status, inner.receipt.reason) == ("rejected", "invalid_scope")
                            else:
                                accepted(inner.receipt)
                            yield inner

                    gen = generator()
                    inner = await anext(gen)
                    inner_scope = rec.capture_scope()

                    async def closer():
                        assert rec.capture_scope() == inner_scope
                        assert await gen.aclose() is None
                        assert rec.capture_scope() == rejected_scope

                    await asyncio.create_task(closer())
                    accepted(inner.exit_receipt)
                    assert inner.exit_receipt.turn_id == inner.scope.turn_id
                    assert inner.exit_receipt.event_id is None
                    assert rec.capture_scope() == rejected_scope, "Normalization must stop at the live rejected predecessor."
                    if predecessor == "disabled-turn":
                        receipt = rec.observe("cache.served", scope=rec.capture_scope(), facts={"owner": "host"})
                        assert (receipt.status, receipt.reason, receipt.event_id) == ("rejected", "invalid_scope", None)
                    else:
                        receipt, _ = finish_scoped_operation(rec, rec.capture_scope(), link_basis="none")
                if predecessor == "disabled-turn":
                    assert (rejected.exit_receipt.status, rejected.exit_receipt.reason) == ("rejected", "invalid_scope")
                else:
                    accepted(rejected.exit_receipt)
                    assert rejected.exit_receipt.turn_id == outer.receipt.turn_id
                    assert rejected.exit_receipt.event_id is None
                assert rec.capture_scope() == outer_scope
            accepted(outer_dispatch.exit_receipt)
        accepted(outer.exit_receipt)
        return outer, outer_dispatch, inner, receipt

    try:
        outer, outer_dispatch, inner, receipt = asyncio.run(scenario())
        close_scope_recording(rec)
        turns = read(path, "SELECT turn_id,ended_at,host_outcome FROM turns")
        assert len(turns) == 1 and turns[0]["turn_id"] == outer.receipt.turn_id
        assert turns[0]["ended_at"] is not None and turns[0]["host_outcome"] == "returned"
        dispatches = [{"event_id": outer_dispatch.receipt.event_id, "kind": "host.dispatch"}]
        if predecessor == "disabled-turn":
            assert read(path, "SELECT event_id,kind FROM events ORDER BY sequence") == dispatches
            assert read(path, "SELECT operation_id FROM operations") == []
        else:
            dispatches.append({"event_id": inner.receipt.event_id, "kind": "host.dispatch"})
            assert read(path, "SELECT event_id,kind FROM events WHERE kind='host.dispatch' ORDER BY sequence") == dispatches
            assert read(path, "SELECT operation_id,turn_id,dispatch_event_id,link_basis,status FROM operations") == [{
                "operation_id": receipt.operation_id, "turn_id": outer.receipt.turn_id,
                "dispatch_event_id": None, "link_basis": "none", "status": "ok"}]
    finally:
        rec.close()
