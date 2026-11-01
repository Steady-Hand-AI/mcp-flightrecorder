"""Installed-core admission, commit visibility, and reservation ownership (A4/A6)."""
from contextlib import closing, contextmanager
import sqlite3
import threading

import pytest
import pytest_check as check

from mcp_flightrecorder import Recorder
from mcp_flightrecorder import _storage

COVERAGE = {"producers": {"model": "model", "host": "host", "mcp": "mcp"}}


@contextmanager
def recording(tmp_path, **limits):
    rec = Recorder(tmp_path / "capture.sqlite", capture="content", coverage=COVERAGE, limits=limits)
    try:
        yield rec
    finally:
        rec.close(timeout=5)


def accepted(receipt):
    assert receipt.status == "accepted", f"Fixture observation must be admitted before dependent checks: {receipt.reason}."
    return receipt


def rows(path, sql, parameters=()):
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        db.row_factory = sqlite3.Row
        return [dict(row) for row in db.execute(sql, parameters)]


def gate_persistence(rec, monkeypatch, kind):
    reached, release = threading.Event(), threading.Event()
    original = rec._persist_bundle
    captured = []

    def held(connection, bundle):
        if bundle.kind == kind and not reached.is_set():
            captured.append(bundle)
            reached.set()
            assert release.wait(5), "The test must release its writer hold without leaving a background owner blocked."
        return original(connection, bundle)

    monkeypatch.setattr(rec, "_persist_bundle", held)
    return reached, release, captured


def test_rejected_admission_publishes_no_identity_or_reservation(tmp_path, monkeypatch):
    with recording(tmp_path, ordinary_items=1, batch_items=1) as rec:
        reached, release, captured = gate_persistence(rec, monkeypatch, "cache.served")
        try:
            first = accepted(rec.observe("cache.served", scope=rec.capture_scope(), facts={"owner": "host"}))
            assert reached.wait(5), "The first admitted envelope must be writer-held for the capacity rejection control."
            rejected = rec.start_operation(scope=rec.capture_scope(), client_id="client", method="tools/call", arguments={})
            check.equal(rejected.status, "rejected", msg="A full ordinary pool must reject a new start before publishing it.")
            check.equal(rejected.reason, "capacity", msg="Ordinary admission refusal must identify capacity rather than a fake execution error.")
            for field in ("event_id", "decision_id", "model_call_id", "operation_id", "ticket"):
                check.is_none(getattr(rejected, field), msg=f"Rejected start must publish no {field} prerequisite or reservation ticket.")
            with rec._condition:
                pending_keys = set(rec._pending)
                committed_keys = set(rec._committed)
            check.equal(pending_keys, {("event", first.event_id)},
                        msg="Rejected admission cannot publish hidden pending identities beyond the previously admitted event.")
            check.equal(committed_keys, set(),
                        msg="Neither the held event nor a rejected start may appear committed before the writer commits.")
            health = rec.health()
            check.equal(health.ordinary.used_slots, 1, msg="Writer-held ordinary work remains charged after dequeue.")
            check.equal(health.ordinary.used_bytes, captured[0].ordinary_charge,
                        msg="Ordinary bytes include the immutable envelope, body, and pending identity exactly once.")
            check.equal(health.completion.used_slots, 0, msg="Rejected starts cannot leak a terminal reservation.")
            check.equal(health.completion.used_bytes, 0, msg="Rejected starts cannot charge the separate terminal-byte pool.")
            check.equal(health.open_operations, 0, msg="Open-operation health counts only admitted starts.")
            check.equal(health.dropped_events, 1, msg="One rejected start counts one event, not event plus operation rows.")
        finally:
            release.set()
        closed = rec.close()
        assert closed.writer_stopped, "The writer must be joined before persisted rejection consequences are inspected."
        stored = rows(tmp_path / "capture.sqlite", "SELECT event_id FROM events")
        check.equal(stored, [{"event_id": first.event_id}], msg="Rejected admission must leave no event or prerequisite identity in storage.")
        check.equal(rows(tmp_path / "capture.sqlite", "SELECT count(*) AS n FROM operations")[0]["n"], 0,
                    msg="Rejected starts cannot leave a running operation row.")


def test_savepoint_release_is_not_commit_or_capacity_release(tmp_path, monkeypatch):
    with recording(tmp_path, batch_items=1) as rec:
        reached, release = threading.Event(), threading.Event()
        original = rec._commit_batch

        def held_commit(connection):
            reached.set()
            assert release.wait(5), "The commit-control gate must be explicitly released by its owning test."
            return original(connection)

        monkeypatch.setattr(rec, "_commit_batch", held_commit)
        try:
            item = accepted(rec.observe("cache.served", scope=rec.capture_scope(), facts={"owner": "host"}))
            assert reached.wait(5), "The writer must release the bundle SAVEPOINT and reach outer COMMIT before inspection."
            key = ("event", item.event_id)
            with rec._condition:
                pending, promoted = key in rec._pending, key in rec._committed
            check.is_true(pending, msg="SAVEPOINT success must retain the event's pending prerequisite identity.")
            check.is_false(promoted, msg="Only successful outer COMMIT may promote a prerequisite to committed identity.")
            check.equal(rows(tmp_path / "capture.sqlite", "SELECT count(*) AS n FROM events")[0]["n"], 0,
                        msg="A separate reader cannot see the uncommitted SAVEPOINT contents.")
            check.equal(rec.health().ordinary.used_slots, 1,
                        msg="SAVEPOINT release cannot release writer-held capacity before transaction success.")
        finally:
            release.set()
        closed = rec.close()
        assert closed.writer_stopped, "Closure must join the owner before the committed side of the visibility check."
        check.equal(rows(tmp_path / "capture.sqlite", "SELECT event_id FROM events"), [{"event_id": item.event_id}],
                    msg="Successful outer COMMIT must make the admitted event durable to a new reader.")
        with rec._condition:
            still_pending, committed = key in rec._pending, key in rec._committed
        check.is_false(still_pending, msg="Successful outer COMMIT removes the event's no-longer-pending identity.")
        check.is_true(committed, msg="Successful outer COMMIT publishes the event in the bounded committed identity cache.")
        check.equal(rec.health().ordinary.used_slots, 0, msg="Committed work releases its ordinary slot exactly once.")
        check.equal(rec.health().ordinary.used_bytes, 0, msg="Committed work releases its immutable buffers and pending storage.")


def test_fifo_prerequisites_and_evicted_committed_lookup_use_owned_reader(tmp_path, monkeypatch):
    with recording(tmp_path, batch_items=1, committed_cache_items=1) as rec:
        request = accepted(rec.observe("model.request", scope=rec.capture_scope(), payload={"messages": []},
                                       facts={"messages": []}))
        decision = accepted(rec.observe("model.tool_call", scope=rec.capture_scope(), payload={"arguments": {}},
                                        facts={"name": "tool", "model_call_id": request.model_call_id}))
        accepted(rec.observe("model.response", scope=rec.capture_scope(), payload={},
                             facts={"model_call_id": request.model_call_id}))
        reached, release, _ = gate_persistence(rec, monkeypatch, "cache.served")
        accepted(rec.observe("cache.served", scope=rec.capture_scope(), facts={"owner": "host"}))
        assert reached.wait(5), "A later FIFO envelope must reach persistence only after the decision prefix committed."
        read_threads = []
        original_connect = _storage.connect

        def owned_reader(path, *, readonly=False, timeout=1.0):
            if readonly:
                read_threads.append(threading.current_thread())
            return original_connect(path, readonly=readonly, timeout=timeout)

        monkeypatch.setattr(_storage, "connect", owned_reader)
        try:
            with rec._condition:
                absent = ("decision", decision.decision_id) not in rec._committed
            assert absent, "The one-item committed cache must have evicted the earlier decision for the fallback control."
            start = accepted(rec.start_operation(scope=rec.capture_scope(), client_id="client", method="tools/call",
                                                 arguments={}, decision_id=decision.decision_id, link_basis="propagated"))
            accepted(rec.finish_operation(start.ticket, status="ok", result={}, resolution="remote"))
            check.is_true(threading.current_thread() in read_threads,
                          msg="Evicted identity lookup must open a short read connection on the caller that owns it.")
        finally:
            release.set()
        rec.close()
        events = rows(tmp_path / "capture.sqlite", "SELECT event_id,sequence FROM events ORDER BY sequence")
        ids = [event["event_id"] for event in events]
        check.is_true(ids.index(request.event_id) < ids.index(decision.event_id) < ids.index(start.event_id),
                      msg="Admitted dependencies must retain request-before-decision-before-operation FIFO order.")
        operation = rows(tmp_path / "capture.sqlite", "SELECT model_call_id,decision_id FROM operations")
        assert len(operation) == 1, "Exactly one admitted propagated operation is required before inspecting its attribution."
        check.equal(operation[0]["model_call_id"], request.model_call_id,
                    msg="Committed-cache eviction must not lose the decision's original model-call attribution.")
        check.equal(operation[0]["decision_id"], decision.decision_id,
                    msg="SQLite fallback must preserve the explicit decision occurrence, without name matching.")


def test_reserved_terminal_enters_full_ordinary_pool_and_releases_once(tmp_path, monkeypatch):
    with recording(tmp_path, ordinary_items=1, ordinary_bytes=32768, completion_items=2,
                   completion_bytes=8192, batch_items=1) as rec:
        reached, release, captured = gate_persistence(rec, monkeypatch, "mcp.request")
        try:
            start = accepted(rec.start_operation(scope=rec.capture_scope(), client_id="client", method="tools/call", arguments={}))
            assert reached.wait(5), "The start must remain writer-held while its reserved terminal enters a full ordinary pool."
            before = rec.health()
            check.equal((before.ordinary.used_slots, before.completion.used_slots, before.completion.used_bytes), (1, 1, 4096),
                        msg="An admitted start owns one ordinary slot and one separate 4,096-byte terminal reservation.")
            terminal = accepted(rec.finish_operation(start.ticket, status="ok", result={"text": "x" * 100000},
                                                     resolution="remote", text_paths=["/text"]))
            check.is_true(terminal.losses,
                        msg="Insufficient optional body capacity must be explicit while the reserved terminal remains admitted.")
            held = rec.health()
            check.equal(held.ordinary.used_slots, 1, msg="A reserved terminal must not require a second ordinary item slot.")
            check.equal((held.completion.used_slots, held.completion.used_bytes, held.open_operations), (1, 4096, 1),
                        msg="Transferring a reservation into FIFO retains ownership through terminal COMMIT.")
            duplicate = rec.finish_operation(start.ticket, status="ok")
            check.equal((duplicate.status, duplicate.reason), ("rejected", "duplicate_completion"),
                        msg="A second terminal attempt must not reuse the same reservation.")
            check.equal(rec.health().completion.used_slots, 1, msg="Duplicate completion cannot release the first terminal's owned slot.")
            check.equal(rec.health().dropped_events, 1, msg="An admitted omitted body adds no drops; the duplicate terminal adds one.")
        finally:
            release.set()
        closed = rec.close()
        assert closed.writer_stopped, "The terminal's persistence owner must finish before lifecycle assertions."
        operation = rows(tmp_path / "capture.sqlite", "SELECT status,end_event_id FROM operations")
        assert len(operation) == 1, "The fixture must retain exactly one logical operation."
        check.equal(operation[0], {"status": "ok", "end_event_id": terminal.event_id},
                    msg="Capacity pressure cannot prevent the reserved atomic terminal transition.")
        health = rec.health()
        check.equal((health.ordinary.used_slots, health.ordinary.used_bytes, health.completion.used_slots,
                     health.completion.used_bytes, health.open_operations), (0, 0, 0, 0, 0),
                    msg="Successful terminal commit and close must release every capacity owner exactly once.")
        check.equal(closed.run_close_state, "incomplete", msg="Capacity-only body omission marks capture incomplete despite a valid terminal.")
        check.equal(health.writer_errors, 0, msg="Pressure and duplicate terminal input are not writer storage errors.")


def test_abandoned_starts_exhaust_only_completion_capacity_and_close_reclaims(tmp_path):
    with recording(tmp_path, completion_items=1, completion_bytes=4096) as rec:
        start = accepted(rec.start_operation(scope=rec.capture_scope(), client_id="client", method="tools/call", arguments={}))
        refused = rec.start_operation(scope=rec.capture_scope(), client_id="client", method="tools/call", arguments={})
        check.equal((refused.status, refused.reason), ("rejected", "capacity"),
                    msg="An abandoned start can exhaust operation capture until its reservation is released.")
        ordinary = accepted(rec.observe("cache.served", scope=rec.capture_scope(), facts={"owner": "host"}))
        bad_terminal = rec.finish_operation(start.ticket, status="not-a-terminal")
        check.equal((bad_terminal.status, bad_terminal.reason), ("rejected", "invalid_input"),
                    msg="Invalid terminal input must return a receipt without consuming the still-valid ticket.")
        health = rec.health()
        check.equal((health.completion.used_slots, health.completion.used_bytes, health.open_operations), (1, 4096, 1),
                    msg="Invalid completion leaves its original reservation available for retry or close.")
        closed = rec.close()
        assert closed.writer_stopped, "Orderly close must own abandonment handling before final capacity checks."
        op = rows(tmp_path / "capture.sqlite", "SELECT status,end_event_id,duration_ms FROM operations")
        assert len(op) == 1, "Rejected starts must not create another operation row."
        check.equal(op[0], {"status": "incomplete", "end_event_id": None, "duration_ms": None},
                    msg="Shutdown reclaims abandoned reservations without fabricating terminal evidence.")
        check.is_true(any(row["event_id"] == ordinary.event_id for row in rows(tmp_path / "capture.sqlite", "SELECT event_id FROM events")),
                      msg="Exhausted completion capacity must not block ordinary evidence admission.")
        check.equal(rec.health().dropped_events, 2, msg="Rejected start and invalid terminal each count once; synthetic shutdown adds none.")
        check.equal((rec.health().open_operations, rec.health().completion.used_slots, rec.health().completion.used_bytes), (0, 0, 0),
                    msg="Close must release the last abandoned reservation without leaking ownership.")


@pytest.mark.parametrize("cap", [1, 4095], ids=["one-byte", "below-one-reservation"])
def test_small_positive_completion_caps_admit_zero_operations_without_hidden_minimum(tmp_path, cap):
    with recording(tmp_path, completion_bytes=cap) as rec:
        start = rec.start_operation(scope=rec.capture_scope(), client_id="client", method="tools/call", arguments={})
        check.equal(start.reason, "capacity", msg="A valid small completion cap must decline starts rather than silently raise the cap.")
        accepted(rec.observe("cache.served", scope=rec.capture_scope(), facts={"owner": "host"}))
        check.equal(rec.health().completion.max_bytes, cap, msg="Health must expose the exact configured positive byte cap.")
        check.equal((rec.health().completion.used_bytes, rec.health().completion.used_slots), (0, 0),
                    msg="Rejected zero-capacity starts must acquire no partial reservation.")


@pytest.mark.parametrize("bound,expected_commits", [
    ("none", [3]), ("items", [2, 3]), ("bytes", [1, 2, 3]),
])
def test_expired_deadline_drains_queued_fifo_within_batch_limits(tmp_path, monkeypatch, bound, expected_commits):
    """Expired coalescing waits must not split an already queued, bounded FIFO batch."""
    ready, release, committed_all = threading.Event(), threading.Event(), threading.Event()
    original_next = _storage.Writer._next_batch
    gated = False

    def hold_first_batch(writer):
        nonlocal gated
        if not gated:
            gated = True
            ready.set()
            assert release.wait(5), "The deterministic backlog control must release the writer it owns."
        return original_next(writer)

    monkeypatch.setattr(_storage.Writer, "_next_batch", hold_first_batch)
    rec = Recorder(tmp_path / "capture.sqlite", capture="content", coverage=COVERAGE,
                   limits={"batch_items": 2 if bound == "items" else 64, "batch_delay_ms": 20})
    original_clock = _storage.time
    committed_counts = []
    original_commit = rec._commit_batch

    def count_committed_batch(connection):
        total = connection.execute("SELECT count(*) FROM events").fetchone()[0]
        original_commit(connection)
        committed_counts.append(total)
        if total == 3:
            committed_all.set()

    class AdvancedWriterClock:
        @staticmethod
        def monotonic():
            return original_clock.monotonic() + 10

    monkeypatch.setattr(rec, "_commit_batch", count_committed_batch)
    try:
        assert ready.wait(5), "The writer must wait before dequeueing the deliberately accumulated public observations."
        receipts = [accepted(rec.observe("cache.served", scope=rec.capture_scope(),
                                        facts={"owner": "host"})) for _ in range(3)]
        if bound == "bytes":
            with rec._condition:
                # Every single bundle fits; no adjacent pair can share a batch.
                rec._limits["batch_bytes"] = max(bundle.size for bundle in rec._queue)
        monkeypatch.setattr(_storage, "time", AdvancedWriterClock)
        release.set()
        assert committed_all.wait(5), "Every admitted envelope must still commit when the batching deadline is already expired."
        check.equal(committed_counts, expected_commits,
                    msg="Drain available FIFO work to item/byte caps; an expired deadline only forbids waiting.")
        check.equal(rows(tmp_path / "capture.sqlite", "SELECT event_id FROM events ORDER BY sequence"),
                    [{"event_id": receipt.event_id} for receipt in receipts],
                    msg="Batching must retain admission order and every accepted event.")
    finally:
        monkeypatch.setattr(_storage, "time", original_clock)
        release.set()
        rec.close()
