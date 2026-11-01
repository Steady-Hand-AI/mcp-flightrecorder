"""Installed-core transactional rejection and host transparency controls (A5/A8)."""
import asyncio
from contextlib import closing, contextmanager
import logging
import sqlite3
import threading

import pytest
import pytest_check as check

from mcp_flightrecorder import Recorder
from mcp_flightrecorder._types import LogicalRejection

COVERAGE = {"producers": {"model": "model", "host": "host", "mcp": "mcp"}}


@contextmanager
def recording(tmp_path, **limits):
    rec = Recorder(tmp_path / "capture.sqlite", capture="content", coverage=COVERAGE, limits=limits)
    try:
        yield rec
    finally:
        rec.close(timeout=5)


def accepted(receipt):
    assert receipt.status == "accepted", f"The intended valid setup must be admitted before fault injection: {receipt.reason}."
    return receipt


def query(path, statement):
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        db.row_factory = sqlite3.Row
        return [dict(row) for row in db.execute(statement)]


def cache(rec):
    return accepted(rec.observe("cache.served", scope=rec.capture_scope(), facts={"owner": "host"}))


def assert_reclaimed(rec):
    health = rec.health()
    check.equal((health.ordinary.used_bytes, health.ordinary.used_slots, health.completion.used_bytes,
                 health.completion.used_slots, health.open_operations), (0, 0, 0, 0, 0),
                msg="Commit, discard, and shutdown must jointly release every accepted capacity owner exactly once.")
    with rec._condition:
        pending_count = len(rec._pending)
    check.equal(pending_count, 0, msg="No discarded or closed prerequisite may remain pending in memory.")


@pytest.mark.parametrize("failed_kind", ["mcp.request", "mcp.response"], ids=["start", "terminal"])
def test_lifecycle_bundle_is_atomic_even_after_its_sql_was_applied(tmp_path, monkeypatch, failed_kind):
    with recording(tmp_path, batch_items=1) as rec:
        applied, release = threading.Event(), threading.Event()
        original = rec._persist_bundle

        def fail_after_sql(connection, bundle):
            original(connection, bundle)
            if bundle.kind == failed_kind:
                applied.set()
                assert release.wait(5), "The test must release its injected lifecycle failure while preserving writer ownership."
                raise sqlite3.OperationalError("controlled lifecycle write failure")

        monkeypatch.setattr(rec, "_persist_bundle", fail_after_sql)
        start = accepted(rec.start_operation(scope=rec.capture_scope(), client_id="client", method="tools/call", arguments={}))
        try:
            if failed_kind == "mcp.request":
                assert applied.wait(5), "The start event and running row must both be staged before testing rollback."
                terminal = accepted(rec.finish_operation(start.ticket, status="ok", result={}))
            else:
                terminal = accepted(rec.finish_operation(start.ticket, status="ok", result={}))
                assert applied.wait(5), "The terminal event and operation update must both be staged before testing rollback."
                visible = query(tmp_path / "capture.sqlite", "SELECT status,end_event_id FROM operations")
                assert len(visible) == 1, "The earlier start must have committed before its terminal transaction begins."
                check.equal(visible[0], {"status": "running", "end_event_id": None},
                            msg="Uncommitted terminal SQL cannot become visible independently of its event.")
                check.equal((rec.health().open_operations, rec.health().completion.used_slots,
                             rec.health().completion.used_bytes), (1, 1, 4096),
                            msg="A writer-held terminal still owns its operation reservation before outer COMMIT or discard.")
        finally:
            release.set()
        closed = rec.close()
        assert closed.writer_stopped, "Failure handling must join its writer before persisted lifecycle assertions."
        events = query(tmp_path / "capture.sqlite", "SELECT event_id FROM events")
        operations = query(tmp_path / "capture.sqlite", "SELECT status,end_event_id,duration_ms FROM operations")
        if failed_kind == "mcp.request":
            check.equal(events, [], msg="A failed start transaction must not leave its event or dependent terminal behind.")
            check.equal(operations, [], msg="A failed start transaction must roll back its running operation row.")
            check.equal(rec.health().dropped_events, 2,
                        msg="The failed start and already-admitted terminal suffix each count one discarded envelope.")
        else:
            check.equal(events, [{"event_id": start.event_id}],
                        msg="A failed terminal transaction must preserve its committed start and remove the terminal event.")
            check.equal(operations, [{"status": "incomplete", "end_event_id": None, "duration_ms": None}],
                        msg="Close may mark the surviving start incomplete but cannot invent a terminal outcome or duration.")
            check.equal(rec.health().dropped_events, 1, msg="Only the failed terminal envelope is discarded after a committed start.")
        check.equal(rec.health().writer_errors, 1, msg="A single failed writer transaction counts once, including its cleanup.")
        assert_reclaimed(rec)


def test_isolated_identity_free_logical_rejection_preserves_prefix_and_suffix(tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.WARNING, logger="mcp_flightrecorder")
    with recording(tmp_path, batch_items=4, batch_delay_ms=10000) as rec:
        original = rec._persist_bundle

        def reject_turn_end(connection, bundle):
            if bundle.kind == "turn.end":
                assert not bundle.identities, "The isolated-rejection control may expose no new prerequisite identities."
                raise LogicalRejection("turn_update_lost")
            return original(connection, bundle)

        monkeypatch.setattr(rec, "_persist_bundle", reject_turn_end)
        with rec.turn("turn whose close update is rejected") as turn:
            accepted(turn.receipt)
            prefix = cache(rec)
        suffix = cache(rec)
        closed = rec.close()
        assert closed.writer_stopped, "The batch owner must finish before checking its surviving FIFO portions."
        check.equal(query(tmp_path / "capture.sqlite", "SELECT event_id FROM events ORDER BY sequence"),
                    [{"event_id": prefix.event_id}, {"event_id": suffix.event_id}],
                    msg="An isolated identity-free rejection must preserve both validated neighboring events.")
        check.equal(rec.health().writer_errors, 0, msg="A recognized logical refusal is not an arbitrary storage failure.")
        check.equal(rec.health().dropped_events, 0, msg="A rejected turn-row update is not a discarded event envelope.")
        check.equal(rec.health().reason_counts.get("turn_update_lost", 0), 1,
                    msg="The lost turn update must remain an explicit health observation.")
        check.equal(closed.run_close_state, "incomplete", msg="A missing turn end prevents a clean recording claim.")
        assert any(record.levelno == logging.WARNING and "recorder_close " in record.getMessage()
                   and "state=incomplete" in record.getMessage() for record in caplog.records), (
            "A loss discovered after admission must remain visible even when the database is portable."
        )
        assert_reclaimed(rec)


@pytest.mark.parametrize("failed_kind", ["model.request", "turn.start"], ids=["model-request", "turn-start"])
def test_rejected_admitted_prerequisite_discards_accepted_suffix(tmp_path, monkeypatch, failed_kind):
    with recording(tmp_path, batch_items=4 if failed_kind == "model.request" else 5, batch_delay_ms=10000) as rec:
        original = rec._persist_bundle

        def reject_prerequisite(connection, bundle):
            if bundle.kind == failed_kind:
                original(connection, bundle)
                raise LogicalRejection("prerequisite_lost")
            return original(connection, bundle)

        monkeypatch.setattr(rec, "_persist_bundle", reject_prerequisite)
        prefix = cache(rec)
        if failed_kind == "model.request":
            request = accepted(rec.observe("model.request", scope=rec.capture_scope(), payload={"messages": []}, facts={"messages": []}))
            accepted(rec.observe("model.tool_call", scope=rec.capture_scope(), payload={"arguments": {}},
                                 facts={"name": "tool", "model_call_id": request.model_call_id}))
            cache(rec)
            discarded_events = 3
            rolled_back_keys = (("model", request.model_call_id),)
        else:
            with rec.turn("turn whose prerequisite insert is rejected") as turn:
                accepted(turn.receipt)
                start = accepted(rec.start_operation(scope=rec.capture_scope(), client_id="client",
                                                     method="tools/call", arguments={}))
                terminal = accepted(rec.finish_operation(start.ticket, status="ok", result={"value": 1}))
            accepted(turn.exit_receipt)
            discarded_events = 2
            rolled_back_keys = (("event", start.event_id), ("event", terminal.event_id))
        closed = rec.close()
        assert closed.writer_stopped, "Prerequisite-loss handling must finish before checking the committed prefix."
        check.equal(query(tmp_path / "capture.sqlite", "SELECT event_id FROM events ORDER BY sequence"),
                    [{"event_id": prefix.event_id}],
                    msg="A rejected admitted prerequisite commits only the validated prefix and discards every accepted suffix envelope.")
        check.equal(rec.health().dropped_events, discarded_events,
                    msg="Each discarded event envelope counts once; rejected turn-row changes do not count as events.")
        if failed_kind == "turn.start":
            check.equal(query(tmp_path / "capture.sqlite", "SELECT turn_id FROM turns"), [],
                        msg="The rejected turn insert must roll back even though turn starts no longer publish a cache identity.")
            check.equal(query(tmp_path / "capture.sqlite", "SELECT operation_id FROM operations"), [],
                        msg="The already-admitted operation and its terminal must be discarded with the lost turn prerequisite.")
            check.equal((start.ticket.phase, start.ticket.reserved), ("discarded", False),
                        msg="Discarding both operation envelopes must release their shared completion reservation exactly once.")
        check.equal(rec.health().writer_errors, 0, msg="Known prerequisite loss must not be misreported as SQLite failure.")
        check.is_true(rec.health().state != "accepting", msg="Published prerequisite loss must permanently stop new capture admission.")
        with rec._condition:
            committed_rejected_identity = any(key in rec._committed for key in rolled_back_keys)
        check.is_false(committed_rejected_identity,
                       msg="Rolled-back prerequisites and discarded dependent identities must never reach the committed cache.")
        late = rec.observe("cache.served", scope=rec.capture_scope(), facts={"owner": "host"})
        check.equal((late.status, late.reason), ("rejected", "prerequisite_lost"),
                    msg="A later valid observation must be refused because prerequisite loss permanently stopped admission.")
        assert_reclaimed(rec)


@pytest.mark.parametrize("fault", ["integrity", "commit"], ids=["arbitrary-integrity-error", "outer-commit-error"])
def test_storage_failure_rolls_back_whole_batch_and_promotes_nothing(tmp_path, monkeypatch, fault):
    with recording(tmp_path, batch_items=3, batch_delay_ms=10000) as rec:
        if fault == "integrity":
            original = rec._persist_bundle
            seen = []

            def reject_middle(connection, bundle):
                original(connection, bundle)
                seen.append(bundle)
                if len(seen) == 2:
                    raise sqlite3.IntegrityError("controlled unexpected constraint error")

            monkeypatch.setattr(rec, "_persist_bundle", reject_middle)
        else:
            def fail_commit(connection):
                raise sqlite3.OperationalError("controlled outer commit error")
            monkeypatch.setattr(rec, "_commit_batch", fail_commit)
        receipts = [cache(rec) for _ in range(3)]
        closed = rec.close()
        assert closed.writer_stopped, "Fatal storage failure must relinquish writer ownership before database inspection."
        check.equal(query(tmp_path / "capture.sqlite", "SELECT event_id FROM events"), [],
                    msg="Unexpected storage or outer-COMMIT failure invalidates the whole uncommitted batch, including a valid prefix.")
        with rec._condition:
            promoted = [receipt.event_id for receipt in receipts if ("event", receipt.event_id) in rec._committed]
        check.equal(promoted, [], msg="No identity from the failed outer transaction may be published as committed.")
        check.equal(rec.health().dropped_events, 3, msg="Each failed-batch envelope is discarded exactly once.")
        check.equal(rec.health().writer_errors, 1, msg="One failed transaction, including rollback, counts as one writer error.")
        check.equal(closed.run_close_state, "incomplete", msg="A failed capture cannot report a clean recording.")
        assert_reclaimed(rec)


@pytest.mark.parametrize("mode", ["enabled", "failed"], ids=["capture-enabled", "writer-failed"])
@pytest.mark.parametrize("outcome", ["value", "error", "cancelled"], ids=["original-value", "original-exception", "original-cancellation"])
def test_host_harness_preserves_original_outcome(tmp_path, monkeypatch, mode, outcome):
    rec = Recorder(tmp_path / "capture.sqlite", capture="content", coverage=COVERAGE)
    original_value = object()
    original_error = RuntimeError("host's own exception") if outcome == "error" else asyncio.CancelledError("host's own cancellation")
    try:
        if mode == "failed":
            def fail(connection, bundle):
                raise sqlite3.OperationalError("controlled capture failure")
            monkeypatch.setattr(rec, "_persist_bundle", fail)
            cache(rec)
            with rec._condition:
                assert rec._condition.wait_for(lambda: rec._state == "failed", timeout=5), (
                    "The failure control must stop its writer before host transparency is exercised."
                )

        async def host():
            if outcome == "value":
                return original_value
            raise original_error

        captured_starts = []

        async def observed_host():
            start = rec.start_operation(scope=rec.capture_scope(), client_id="client",
                                        method="tools/call", arguments={})
            captured_starts.append(start)
            try:
                returned = await host()
            except BaseException as error:
                if start.status == "accepted":
                    rec.finish_operation(start.ticket, status="cancelled" if isinstance(error, asyncio.CancelledError) else "local_error")
                raise
            else:
                if start.status == "accepted":
                    rec.finish_operation(start.ticket, status="ok", result={"observed": "result"})
                return returned

        if outcome == "value":
            check.is_(asyncio.run(observed_host()), original_value,
                      msg="Recorder presence or failure must preserve the exact host return object.")
        else:
            # The harness must raise the actual original host exception or cancellation before inspecting identity.
            with pytest.raises(type(original_error)) as caught:
                asyncio.run(observed_host())
            check.is_(caught.value, original_error,
                      msg="Capture must not replace the host's original exception or cancellation instance.")
        assert len(captured_starts) == 1, "The host harness must attempt capture once at its actual invocation."
        if mode == "failed":
            check.equal((captured_starts[0].status, captured_starts[0].reason), ("rejected", "writer_failed"),
                        msg="An already-failed recorder must return an explicit failure receipt without replacing host work.")
            check.is_none(captured_starts[0].ticket, msg="Failed-writer admission cannot create a new terminal reservation.")
        rec.close()
        expected_errors = 1 if mode == "failed" else 0
        check.equal(rec.health().writer_errors, expected_errors,
                    msg="Host failures are observed outcomes; only the injected writer transaction increments writer errors.")
        if mode == "enabled":
            operation = query(tmp_path / "capture.sqlite", "SELECT status FROM operations")
            expected_status = {"value": "ok", "error": "local_error", "cancelled": "cancelled"}[outcome]
            check.equal(operation, [{"status": expected_status}],
                        msg="Capture records the corresponding observed host outcome without changing it.")
    finally:
        rec.close()


def test_invalid_unavailable_and_closed_attempts_have_bounded_receipts_and_health(tmp_path):
    coverage = {"producers": {"model": None, "host": "host", "mcp": "mcp"}}
    rec = Recorder(tmp_path / "capture.sqlite", capture="content", coverage=coverage)
    try:
        invalid = rec.observe("not-a-kind", scope=rec.capture_scope(), payload={"private": "DO_NOT_ECHO"})
        unavailable = rec.observe("model.request", scope=rec.capture_scope(), payload={"messages": []}, facts={"messages": []})
        check.equal((invalid.status, invalid.reason), ("rejected", "invalid_input"),
                    msg="Expected validation failure must return an input receipt instead of escaping into the host.")
        check.equal((unavailable.status, unavailable.reason), ("rejected", "unavailable_boundary"),
                    msg="A declared unavailable model producer cannot emit authoritative model evidence.")
        check.equal(rec.health().dropped_events, 2, msg="Each rejected envelope counts once regardless of its potential artifacts.")
        check.equal(rec.health().writer_errors, 0, msg="Validation and unavailable coverage are not writer errors.")
        closed = rec.close()
        assert closed.writer_stopped and closed.portable, "Closed diagnostics are compared only after a verified standalone file exists."
        before = (tmp_path / "capture.sqlite").read_bytes()
        late = rec.observe("cache.served", scope=rec.capture_scope(), facts={"owner": "host"})
        check.equal((late.status, late.reason), ("closed", "closed"),
                    msg="Post-close attempts must return a bounded closed receipt without reopening capture.")
        check.equal(rec.health().dropped_events, 3, msg="The post-close rejected envelope remains visible in memory health.")
        check.equal((tmp_path / "capture.sqlite").read_bytes(), before,
                    msg="Post-close diagnostics must not mutate the finalized database or extend its recording interval.")
        check.is_not_in("DO_NOT_ECHO", repr(invalid) + repr(rec.health()),
                        msg="Diagnostic receipts and counters must not embed rejected payload content.")
        persisted = query(tmp_path / "capture.sqlite", "SELECT dropped_events FROM runs")
        check.equal(persisted, [{"dropped_events": 2}],
                    msg="Persisted closure health must remain at the recorded interval, excluding later in-memory diagnostics.")
    finally:
        rec.close()
