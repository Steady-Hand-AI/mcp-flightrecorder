"""Operational logs preserve read/copy results, cleanup and content boundaries."""
import json
import logging
from pathlib import Path
import sqlite3
import sys
import time

import pytest

from mcp_flightrecorder import Recorder, inspect
from mcp_flightrecorder.cli import main


COVERAGE = {"producers": {"model": "logging.model", "host": "logging.host", "mcp": "logging.mcp"},
            "model_complete": True}


@pytest.fixture
def recording(tmp_path):
    path = tmp_path / "PATH_SECRET.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    try:
        with recorder.turn("BODY_SECRET") as turn:
            assert turn.receipt.status == "accepted"
    finally:
        closed = recorder.close()
    assert closed.portable
    return path


def test_reader_logs_distinguish_operational_failure_without_echoing_content(recording, tmp_path, caplog):
    caplog.set_level(logging.DEBUG, logger="mcp_flightrecorder._inspect_common")
    result = inspect.query(recording, "SELECT ? AS value", ["SQL_SECRET"])
    assert result["data"]["rows"] == [["SQL_SECRET"]]
    refusals = [
        inspect.check(recording, "ID_SECRET"),
        inspect.query(recording, "DROP TABLE SQL_SECRET"),
        inspect.query(recording, "SELECT 'SQL_SECRET'", no_content=True),
        inspect.runs(recording, limit=0),
    ]
    assert [item["error"]["code"] for item in refusals] == [
        "unknown_run", "sql_rejected", "content_disabled", "invalid_request"
    ]
    failed = inspect.runs(tmp_path / "MISSING_PATH_SECRET.sqlite")
    assert failed["error"]["code"] == "recording_unreadable"
    records = [record for record in caplog.records if record.name == "mcp_flightrecorder._inspect_common"]
    assert [record.levelno for record in records] == [logging.DEBUG] * 5 + [logging.WARNING]
    assert "recording_unreadable" in records[-1].getMessage()
    for record in records:
        assert record.exc_info is None and record.stack_info is None
        serialized = str(record.__dict__)
        assert all(secret not in serialized for secret in ("PATH_SECRET", "BODY_SECRET", "SQL_SECRET", "ID_SECRET"))


@pytest.mark.parametrize("operation", ["reader", "snapshot"])
@pytest.mark.parametrize("expired", [False, True], ids=["storage-failure", "expired-deadline"])
def test_storage_logging_preserves_deadline_precedence_and_releases_connections(
        recording, tmp_path, monkeypatch, caplog, capsys, operation, expired):
    caplog.set_level(logging.WARNING, logger="mcp_flightrecorder")
    real_connect = sqlite3.connect
    owners, closed_at_log, faulted = [], [], []
    clock = [10.0]

    class FaultConnection(sqlite3.Connection):
        def execute(self, sql, parameters=()):
            trigger = "SELECT 1" if operation == "reader" else "SELECT name FROM sqlite_master LIMIT 1"
            if sql == trigger:
                faulted.append(True)
                if expired:
                    clock[0] = 11.0
                error = sqlite3.OperationalError("EXCEPTION_SECRET")
                error.sqlite_errorcode = sqlite3.SQLITE_IOERR
                error.sqlite_errorname = "SQLITE_IOERR"
                raise error
            return super().execute(sql, parameters)

        def close(self):
            super().close()
            self.closed = True

    def connect(*args, **kwargs):
        connection = real_connect(*args, factory=FaultConnection, **kwargs)
        connection.closed = False
        owners.append(connection)
        return connection

    class CleanupObserver(logging.Handler):
        def emit(self, record):
            closed_at_log.append(bool(owners) and all(owner.closed for owner in owners))

    logger = logging.getLogger("mcp_flightrecorder._inspect_common" if operation == "reader"
                               else "mcp_flightrecorder._snapshot")
    observer = CleanupObserver()
    logger.addHandler(observer)
    try:
        with monkeypatch.context() as patch:
            patch.setattr(sqlite3, "connect", connect)
            patch.setattr(time, "monotonic", lambda: clock[0])
            if operation == "reader":
                result = inspect.query(recording, "SELECT 1", timeout=0.5)
            else:
                output = tmp_path / "OUTPUT_SECRET.sqlite"
                patch.setattr(sys, "argv", ["fr", "snapshot", str(recording), str(output), "--timeout", "0.5"])
                status = main()
                captured = capsys.readouterr()
                assert captured.err == ""
                result = json.loads(captured.out)
                assert status == (1 if expired else 3)
                assert not output.exists()
    finally:
        logger.removeHandler(observer)
    expected = ("query_timeout", "deadline_expired") if expired else ("recording_unreadable", "storage_failure")
    assert (result["error"]["code"], result["error"]["reason"]) == expected
    assert result["data"] is None
    assert faulted == [True] and closed_at_log == [True]
    records = [record for record in caplog.records if record.name == logger.name]
    assert len(records) == 1 and records[0].levelno == logging.WARNING
    assert all(value in records[0].getMessage() for value in expected)
    assert "SECRET" not in str(records[0].__dict__) and records[0].exc_info is None


def test_failing_logging_handler_does_not_change_read_or_snapshot_results(recording, tmp_path, monkeypatch, caplog, capsys):
    caplog.set_level(logging.DEBUG, logger="mcp_flightrecorder")
    attempts = []

    class BrokenHandler(logging.Handler):
        def emit(self, record):
            attempts.append(record.name)
            raise RuntimeError("HANDLER_SECRET")

    logger = logging.getLogger("mcp_flightrecorder")
    handler = BrokenHandler()
    logger.addHandler(handler)
    try:
        assert inspect.query(recording, "SELECT 7")["data"]["rows"] == [[7]]
        assert inspect.runs(tmp_path / "missing.sqlite")["error"]["code"] == "recording_unreadable"
        output = tmp_path / "portable.sqlite"
        monkeypatch.setattr(sys, "argv", ["fr", "snapshot", str(recording), str(output)])
        assert main() == 0
        captured = capsys.readouterr()
        assert captured.err == ""
        result = json.loads(captured.out)
        assert result["ok"] and result["data"]["verified"]
        assert inspect.runs(output)["ok"]
    finally:
        logger.removeHandler(handler)
    assert "mcp_flightrecorder._inspect_common" in attempts
    assert "mcp_flightrecorder._snapshot" in attempts


@pytest.mark.parametrize("interrupted", [False, True], ids=["storage-error", "keyboard-interrupt"])
def test_snapshot_cleanup_failure_is_logged_once_even_during_interrupt(
        recording, tmp_path, monkeypatch, caplog, capsys, interrupted):
    caplog.set_level(logging.DEBUG, logger="mcp_flightrecorder._snapshot")
    output = tmp_path / "OUTPUT_SECRET.sqlite"
    import shutil
    import mcp_flightrecorder._snapshot as snapshot_module
    real_connect = sqlite3.connect
    real_cleanup = snapshot_module.tempfile.TemporaryDirectory.cleanup
    owners, closed_at_log = [], []

    class FaultConnection(sqlite3.Connection):
        def backup(self, target, **options):
            if interrupted:
                raise KeyboardInterrupt
            raise sqlite3.OperationalError("EXCEPTION_SECRET")

        def close(self):
            super().close()
            self.closed = True

    def connect(*args, **kwargs):
        connection = real_connect(*args, factory=FaultConnection, **kwargs)
        connection.closed = False
        owners.append(connection)
        return connection

    staged = []

    def fail_private_cleanup(directory):
        staged.append(Path(directory.name))
        raise PermissionError("CLEANUP_SECRET")

    class CleanupObserver(logging.Handler):
        def emit(self, record):
            closed_at_log.append(bool(owners) and all(owner.closed for owner in owners))

    logger = logging.getLogger("mcp_flightrecorder._snapshot")
    observer = CleanupObserver()
    logger.addHandler(observer)
    try:
        with monkeypatch.context() as patch:
            patch.setattr(sqlite3, "connect", connect)
            patch.setattr(snapshot_module.tempfile.TemporaryDirectory, "cleanup", fail_private_cleanup)
            patch.setattr(sys, "argv", ["fr", "snapshot", str(recording), str(output)])
            status = main()
    finally:
        logger.removeHandler(observer)
    captured = capsys.readouterr()
    assert captured.err == ""
    if interrupted:
        assert status == 130 and captured.out == ""
    else:
        result = json.loads(captured.out)
        assert status == 3 and result["error"]["reason"] == "output_cleanup_failed"
    assert not output.exists() and closed_at_log == [True]
    assert len(staged) == 1, "the private-stage cleanup failure must be exercised once"
    records = [record for record in caplog.records if record.name == logger.name]
    assert len(records) == 1 and records[0].levelno == logging.ERROR
    assert "output_cleanup_failed" in records[0].getMessage()
    assert "SECRET" not in str(records[0].__dict__) and records[0].exc_info is None
    for directory in staged:
        if directory.exists():
            shutil.rmtree(directory)
    assert inspect.runs(recording)["ok"]


@pytest.mark.parametrize("payload,reason", [
    ({"arguments_raw": "{"}, "invalid_raw_arguments"),
    ({"arguments": {"x": 1}, "arguments_raw": '{"x":2}'}, "conflicting_arguments"),
], ids=["invalid-raw-arguments", "conflicting-arguments"])
def test_public_argument_comparison_preserves_recorded_unavailability_reason(tmp_path, payload, reason):
    path = tmp_path / "comparison.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    run_id = recorder.capture_scope().run_id
    try:
        with recorder.turn("comparison") as turn:
            request = recorder.observe("model.request", scope=recorder.capture_scope(),
                                       payload={"messages": []}, facts={"messages": []})
            assert request.status == "accepted"
            decision = recorder.observe("model.tool_call", scope=recorder.capture_scope(), payload=payload,
                                        facts={"model_call_id": request.model_call_id, "name": "tool"})
            assert decision.status == "accepted"
            with recorder.dispatch(action="refuse", name="tool", arguments={"x": 1},
                                   decision_event_ids=[decision.event_id]) as dispatch:
                assert dispatch.receipt.status == "accepted"
    finally:
        closed = recorder.close()
    assert closed.portable
    result = inspect.explain(path, run_id, turn.receipt.turn_id, request.model_call_id)
    assert result["ok"]
    assert result["data"]["detail"]["dispatches"] == [{
        "decision_event_id": decision.event_id, "dispatch_event_id": dispatch.receipt.event_id,
        "forwarded_changed": None, "comparison_reason": reason,
    }]


def test_public_result_comparison_preserves_missing_text_paths_reason(tmp_path):
    path = tmp_path / "text-comparison.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    run_id = recorder.capture_scope().run_id
    try:
        with recorder.turn("comparison") as turn:
            request = recorder.observe("model.request", scope=recorder.capture_scope(),
                                       payload={"messages": []}, facts={"messages": []})
            assert request.status == "accepted"
            decision = recorder.observe("model.tool_call", scope=recorder.capture_scope(), payload={"arguments": {}},
                                        facts={"model_call_id": request.model_call_id, "name": "tool"})
            assert decision.status == "accepted"
            start = recorder.start_operation(scope=recorder.capture_scope(), client_id="client", method="tools/call",
                                             name="tool", arguments={}, decision_id=decision.decision_id, link_basis="propagated")
            assert start.status == "accepted"
            terminal = recorder.finish_operation(start.ticket, status="ok", result={"text": "BODY_SECRET"})
            assert terminal.status == "accepted"
            followup = recorder.observe("model.request", scope=recorder.capture_scope(),
                                       payload={"messages": [{"role": "tool", "content": "BODY_SECRET"}]},
                                       facts={"messages": [{"bindings": [{"decision_event_id": decision.event_id,
                                              "pointer": "/content", "result_event_id": terminal.event_id,
                                              "text_paths": [""]}], "unresolved": [], "complete": True}]})
            assert followup.status == "accepted"
    finally:
        closed = recorder.close()
    assert closed.portable
    result = inspect.explain(path, run_id, turn.receipt.turn_id, followup.model_call_id)
    assert result["ok"]
    answers = result["data"]["detail"]["input_answers"]
    assert len(answers) == 1 and answers[0]["association_state"] == "linked"
    assert answers[0]["text_equal"] is None and answers[0]["comparison_reason"] == "text_paths_unavailable"

