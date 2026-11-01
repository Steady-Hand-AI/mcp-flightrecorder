"""Invalid public scopes reject one envelope without stopping later capture."""
from contextlib import closing
import json
import sqlite3

import pytest
from pytest_check import check

from mcp_flightrecorder import Recorder


COVERAGE = {
    "producers": {"model": "scope.model", "host": "scope.host", "mcp": "scope.mcp"},
    "model_complete": True,
}


@pytest.mark.parametrize(("surface", "invalid_scope"), [
    pytest.param("observe", None, id="observe-none"),
    pytest.param("observe", object(), id="observe-object"),
    pytest.param("start_operation", None, id="start-none"),
    pytest.param("start_operation", object(), id="start-object"),
])
def test_invalid_public_scope_rejects_once_and_preserves_capture(tmp_path, surface, invalid_scope):
    path = tmp_path / "scope.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    try:
        before = recorder.health()
        assert before.state == "accepting", "Scope validation requires a successfully initialized accepting recorder"
        if surface == "observe":
            rejected = recorder.observe("host.tool_result", scope=invalid_scope, payload={"value": 1})
        else:
            rejected = recorder.start_operation(
                scope=invalid_scope, client_id="client", method="tools/call", arguments={}
            )
        after = recorder.health()

        check.equal((rejected.status, rejected.reason), ("rejected", "invalid_scope"),
                    msg=f"{surface} must classify an explicit invalid scope as producer rejection, preserving capture")
        for field in ("event_id", "turn_id", "model_call_id", "decision_id", "operation_id", "ticket"):
            check.is_none(getattr(rejected, field),
                          msg=f"Rejected {surface} must publish no {field} that later observations could depend on")
        check.equal(after.dropped_events - before.dropped_events, 1,
                    msg="One invalid public attempt must count exactly one rejected event envelope")
        check.equal(after.reason_counts.get("invalid_scope", 0) - before.reason_counts.get("invalid_scope", 0), 1,
                    msg="The rejected envelope must add exactly one invalid_scope diagnostic")
        check.equal({reason: count for reason, count in after.reason_counts.items() if count}, {"invalid_scope": 1},
                    msg="Scope rejection must not be mislabeled as writer failure or add unrelated loss reasons")
        check.equal(after.state, "accepting",
                    msg="Malformed public attribution must not disable subsequent valid evidence capture")
        check.equal(after.writer_errors, 0,
                    msg="Rejecting an invalid producer scope is not a SQLite writer error")
        check.equal(after.open_operations, 0,
                    msg="An invalid start must not create an operation requiring a terminal observation")
        check.equal((after.completion.used_slots, after.completion.used_bytes), (0, 0),
                    msg="Invalid scopes must not allocate a completion slot or terminal byte reservation")
        check.equal((after.ordinary.used_slots, after.ordinary.used_bytes), (0, 0),
                    msg="A rejected envelope must not leave ordinary queued or writer-held capacity")

        unscoped = recorder.capture_scope()
        check.is_none(unscoped.turn_id,
                      msg="Recorder-issued scope outside a turn is the valid unscoped value; explicit None is not")
        followup = recorder.observe("host.tool_result", scope=unscoped, payload={"value": 2})
        assert followup.status == "accepted", "A valid unscoped follow-up must remain admissible after scope rejection"
        check.is_none(followup.reason, msg="A valid follow-up receipt must have no rejection reason")
        check.is_none(followup.turn_id, msg="Ordinary unscoped capture must retain its null turn attribution")

        with recorder.turn("Continue after invalid producer scope.") as turn:
            assert turn.receipt.status == "accepted", "Internal turn entry must remain valid when no public scope argument is supplied"
            scope = recorder.capture_scope()
            check.equal(scope.turn_id, turn.receipt.turn_id,
                        msg="An admitted turn must install its recorder-issued task-local scope")
            started = recorder.start_operation(
                scope=scope, client_id="client", method="tools/call", arguments={"value": 3}
            )
            assert started.status == "accepted", "A valid scoped start must remain admissible after the invalid envelope"
            assert started.ticket is not None, "The valid start must supply a ticket before its completion is attempted"
            finished = recorder.finish_operation(
                started.ticket, status="ok", result={"text": "done"},
                resolution="local", text_paths=["/text"],
            )
            assert finished.status == "accepted", "Ticket-based completion must remain valid without a public scope argument"
            check.equal(finished.operation_id, started.operation_id,
                        msg="The accepted completion must finish the operation identified by its valid ticket")
        assert turn.exit_receipt.status == "accepted", "Turn exit must remain an admitted row transition after scope rejection"
        check.is_none(recorder.capture_scope().turn_id,
                      msg="Exiting the valid control turn must restore ordinary unscoped capture")
    finally:
        closed = recorder.close()

    assert closed.writer_stopped and closed.portable, "The rejected-input recording must drain and close before read-only inspection"
    check.equal(closed.run_close_state, "incomplete",
                msg="Continuing after invalid input must preserve the earlier evidence loss instead of claiming a clean run")
    health = recorder.health()
    check.equal((health.dropped_events, health.writer_errors), (1, 0),
                msg="Valid follow-up and lifecycle controls must not add losses or turn input rejection into a writer error")
    check.equal((health.state, health.open_operations, health.ordinary.used_slots,
                 health.ordinary.used_bytes, health.completion.used_slots, health.completion.used_bytes),
                ("stopped", 0, 0, 0, 0, 0),
                msg="Successful closure must reclaim every valid operation reservation and queued capacity charge")

    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as reader:
        events = reader.execute("SELECT event_id,kind,turn_id FROM events ORDER BY sequence").fetchall()
        check.equal(events, [
            (followup.event_id, "host.tool_result", None),
            (started.event_id, "mcp.request", turn.receipt.turn_id),
            (finished.event_id, "mcp.response", turn.receipt.turn_id),
        ], msg="Only the valid unscoped follow-up and scoped start/finish may persist; rejection must invent no event")
        payload = reader.execute(
            "SELECT a.body_text FROM events e JOIN artifacts a ON a.artifact_id=e.payload_ref "
            "WHERE e.event_id=?", (followup.event_id,),
        ).fetchone()
        assert payload is not None, "The accepted unscoped follow-up must persist its retained payload after close"
        check.equal(json.loads(payload[0]), {"value": 2},
                    msg="Post-rejection capture must persist the actual valid follow-up content")
        operations = reader.execute("SELECT operation_id,turn_id,status FROM operations").fetchall()
        check.equal(operations, [(started.operation_id, turn.receipt.turn_id, "ok")],
                    msg="Only the valid control start may create an operation, and its ticket must complete that row")
        turns = reader.execute("SELECT turn_id,host_outcome,capture_state FROM turns").fetchall()
        check.equal(turns, [(turn.receipt.turn_id, "returned", "partial")],
                    msg="Turn entry/exit must persist while unassignable earlier run loss prevents a completeness claim")
        run = reader.execute("SELECT close_state,dropped_events,writer_errors FROM runs").fetchall()
        check.equal(run, [("incomplete", 1, 0)],
                    msg="Persisted run health must retain the one rejected envelope without reporting writer failure")
