"""A5/A6: an identity-free duplicate transition is safely isolated.

The public API rejects duplicate completion before admission. This test therefore
injects a private replay control with no event, identities, or operation ticket.
It is charged as an ordinary attempted envelope and runs through the actual
writer transaction, SAVEPOINT, and running-row update guard. It is not evidence
that a second public terminal can be admitted.
"""
from contextlib import closing
import sqlite3
import threading

import pytest_check as check

from mcp_flightrecorder import Recorder
from mcp_flightrecorder._capture import encode
from mcp_flightrecorder._types import Bundle, CaptureResult


COVERAGE = {"producers": {"model": "model", "host": "host", "mcp": "mcp"}}


def accepted(receipt):
    assert receipt.status == "accepted", (
        f"The valid neighboring observation must be admitted before duplicate-transition injection: {receipt.reason}."
    )
    return receipt


def read_rows(path, statement):
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as reader:
        return reader.execute(statement).fetchall()


def test_identity_free_duplicate_completion_preserves_prefix_suffix_and_reservations(tmp_path, monkeypatch):
    path = tmp_path / "duplicate-transition.sqlite"
    rec = Recorder(path, capture="content", coverage=COVERAGE)
    completed, release, replay_committed = threading.Event(), threading.Event(), threading.Event()
    original_commit = rec._commit_bundles
    replay_source = []

    def hold_after_first_terminal_commit(bundles):
        original_commit(bundles)
        if not completed.is_set():
            terminal = next((bundle for bundle in bundles if bundle.kind == "mcp.response"), None)
            if terminal is not None:
                replay_source.append(terminal.decode())
                completed.set()
                assert release.wait(5), "The test must release its own post-COMMIT writer barrier during cleanup."
        elif any(bundle.receipt.event_id == suffix.event_id for bundle in bundles):
            replay_committed.set()

    monkeypatch.setattr(rec, "_commit_bundles", hold_after_first_terminal_commit)
    try:
        first = accepted(rec.start_operation(scope=rec.capture_scope(), client_id="client", method="tools/call", arguments={}))
        still_open = accepted(rec.start_operation(scope=rec.capture_scope(), client_id="client", method="tools/list", arguments={}))
        terminal = accepted(rec.finish_operation(first.ticket, status="ok", result={"original": "result"}))
        assert completed.wait(5), "The first terminal must really COMMIT and release its ticket before replaying its update."
        baseline = rec.health()
        check.equal((baseline.completion.used_bytes, baseline.completion.used_slots, baseline.open_operations),
                    (4096, 1, 1), msg="Only the unrelated still-running operation may own a reservation after the first terminal commits.")

        original_next_batch = rec._writer._next_batch
        selected_batch = []

        def select_replay_batch():
            if not selected_batch:
                with rec._condition:
                    assert len(rec._queue) == 3, "The controlled batch requires exactly the valid prefix, identity-free replay, and valid suffix."
                    selected_batch.extend(rec._queue.popleft() for _ in range(3))
                    return list(selected_batch)
            return original_next_batch()

        # Select one deterministic batch after the writer-held barrier. Batch
        # timing is covered separately; here the real transaction must contain
        # both valid neighbors around the actual duplicate running-row update.
        monkeypatch.setattr(rec._writer, "_next_batch", select_replay_batch)
        prefix = accepted(rec.observe("cache.served", scope=rec.capture_scope(), facts={"owner": "host"}))
        source = replay_source[0]
        unexposed_artifact = dict(source["artifacts"][0], artifact_id="a_duplicate_transition_probe")
        replay = Bundle(
            encoded=encode({"run_id": rec._run_id, "artifacts": [unexposed_artifact],
                            "operation_update": source["operation_update"]}),
            bodies=(), kind="mcp.response.replay", turn_id=None, identities=(),
            receipt=CaptureResult("accepted"), is_event=True, losses=(), partial=False,
            ticket=None, terminal=False, optional=b"",
        )
        with rec._condition:
            assert rec._has_capacity(replay), "The synthetic replay must own real ordinary capacity before it enters the FIFO."
            rec._publish(replay)
        suffix = accepted(rec.observe("cache.served", scope=rec.capture_scope(), facts={"owner": "host"}))
        release.set()
        assert replay_committed.wait(5), "A safe duplicate transition must allow the valid suffix to reach outer COMMIT."

        check.equal(read_rows(path, "SELECT event_id FROM events ORDER BY sequence"),
                    [(first.event_id,), (still_open.event_id,), (terminal.event_id,), (prefix.event_id,), (suffix.event_id,)],
                    msg="The original terminal and both valid neighbors must persist without an invented duplicate event.")
        check.equal(read_rows(path, "SELECT operation_id,status,end_event_id FROM operations ORDER BY method"),
                    [(first.operation_id, "ok", terminal.event_id), (still_open.operation_id, "running", None)],
                    msg="Duplicate rollback must preserve the existing terminal and leave the unrelated operation running.")
        check.equal(read_rows(path, "SELECT artifact_id FROM artifacts WHERE artifact_id='a_duplicate_transition_probe'"), [],
                    msg="ROLLBACK TO the duplicate's SAVEPOINT must remove the unexposed artifact staged before its transition failed.")
        health = rec.health()
        check.equal((health.state, health.dropped_events, health.writer_errors), ("accepting", 1, 0),
                    msg="One safe duplicate attempt is a counted loss while valid admission continues without a storage error.")
        check.equal(health.reason_counts["duplicate_completion"], 1,
                    msg="The actual running-row guard must report duplicate completion exactly once.")
        check.equal((health.ordinary.used_bytes, health.ordinary.used_slots), (0, 0),
                    msg="Both committed neighbors and the discarded replay must release their charged ordinary storage once.")
        check.equal((health.completion.used_bytes, health.completion.used_slots, health.open_operations),
                    (4096, 1, 1), msg="An identity-free duplicate must not release the unrelated active operation's reservation.")

        accepted(rec.finish_operation(still_open.ticket, status="ok", result={}))
        closed = rec.close()
        assert closed.writer_stopped, "The real remaining terminal must finish before inspecting final reservation ownership."
        final = rec.health()
        check.equal((final.completion.used_bytes, final.completion.used_slots, final.open_operations), (0, 0, 0),
                    msg="The unrelated operation must release its own reservation on its real terminal COMMIT, without a leak or double release.")
        check.equal((closed.run_close_state, final.dropped_events, final.writer_errors), ("incomplete", 1, 0),
                    msg="The isolated duplicate loss must survive orderly closure without being counted again as storage failure.")
        with rec._condition:
            pending_count = len(rec._pending)
        check.equal(pending_count, 0, msg="No identity from the committed neighbors or discarded replay may remain pending after close.")
    finally:
        release.set()
        rec.close(timeout=5)
