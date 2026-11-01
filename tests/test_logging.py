"""Operational logs stay bounded, content-free and outside capture ownership."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import logging
import sqlite3
import subprocess
import sys
import threading

import pytest

from mcp_flightrecorder import Recorder


COVERAGE = {"producers": {"model": "model", "host": "host", "mcp": "mcp"}}


def test_unconfigured_library_is_silent_and_does_not_configure_root(tmp_path):
    script = '''
import logging
import sys
root = logging.getLogger()
original = (tuple(root.handlers), root.level)
from mcp_flightrecorder import Recorder, inspect
rec = Recorder(sys.argv[1], capture="metadata",
               coverage={"producers": {"model": None, "host": "host", "mcp": None}})
try:
    receipt = rec.observe("invalid", scope=rec.capture_scope(), payload="PRIVATE")
    assert receipt.reason == "invalid_input"
finally:
    closed = rec.close()
assert closed.portable and closed.run_close_state == "incomplete"
assert inspect.runs(sys.argv[1] + ".missing")["error"]["code"] == "recording_unreadable"
assert (tuple(root.handlers), root.level) == original
'''
    result = subprocess.run([sys.executable, "-I", "-B", "-c", script,
                             str(tmp_path / "recording.sqlite")],
                            capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
    assert result.stdout == result.stderr == ""


def test_concurrent_capacity_loss_logs_once_and_handler_can_read_health(tmp_path, caplog):
    caplog.set_level(logging.INFO, logger="mcp_flightrecorder")
    rec = Recorder(tmp_path / "PATH_SECRET.sqlite", capture="content", coverage=COVERAGE,
                   limits={"completion_bytes": 1})
    logger = logging.getLogger("mcp_flightrecorder._recorder")
    completed, readers = [], []

    class HealthHandler(logging.Handler):
        def emit(self, record):
            if not record.getMessage().startswith("capture_loss "):
                return
            done = threading.Event()

            def read_health():
                rec.health()
                done.set()

            thread = threading.Thread(target=read_health)
            readers.append(thread)
            thread.start()
            completed.append(done.wait(2))

    handler = HealthHandler()
    logger.addHandler(handler)
    barrier = threading.Barrier(4)

    def attempt():
        barrier.wait(timeout=5)
        return rec.start_operation(scope=rec.capture_scope(), client_id="CLIENT_SECRET",
                                   method="METHOD_SECRET", arguments={"private": "BODY_SECRET"})

    try:
        with ThreadPoolExecutor(max_workers=4) as pool:
            receipts = list(pool.map(lambda _: attempt(), range(4)))
        assert all((item.status, item.reason) == ("rejected", "capacity") for item in receipts)
        assert rec.health().dropped_events == 4 and rec.health().open_operations == 0
    finally:
        logger.removeHandler(handler)
        for thread in readers:
            thread.join(5)
        closed = rec.close()
    assert completed == [True], "Application handlers must run outside the admission lock."
    assert closed.portable and closed.run_close_state == "incomplete"
    losses = [record for record in caplog.records if record.getMessage().startswith("capture_loss ")]
    assert len(losses) == 1 and losses[0].levelno == logging.WARNING
    assert "reason=capacity" in losses[0].getMessage()
    closes = [record for record in caplog.records if record.getMessage().startswith("recorder_close ")]
    assert len(closes) == 1 and closes[0].levelno == logging.WARNING
    assert "portable=True" in closes[0].getMessage() and "state=incomplete" in closes[0].getMessage()
    for record in caplog.records:
        if record.name.startswith("mcp_flightrecorder"):
            assert "SECRET" not in str(record.__dict__)
            assert record.exc_info is None and record.stack_info is None


@pytest.mark.parametrize("capture,reason", [("content", "capacity"), ("metadata", "capture_metadata")])
def test_capacity_omission_warns_but_declared_metadata_retention_does_not(tmp_path, caplog, capture, reason):
    caplog.set_level(logging.INFO, logger="mcp_flightrecorder")
    path = tmp_path / "retention.sqlite"
    rec = Recorder(path, capture=capture, coverage=COVERAGE, limits={"ordinary_bytes": 4096})
    try:
        receipt = rec.observe("cache.served", scope=rec.capture_scope(),
                              payload="BODY_SECRET" * 10000, facts={"owner": "host"})
        assert receipt.status == "accepted"
        assert rec.health().dropped_events == 0
    finally:
        closed = rec.close()
    assert closed.portable
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("SELECT body_text,omitted_reason FROM artifacts").fetchall() == [(None, reason)]
    warnings = [record for record in caplog.records if record.name.startswith("mcp_flightrecorder")
                and record.levelno >= logging.WARNING]
    if capture == "content":
        assert "capacity" in receipt.losses and closed.run_close_state == "incomplete"
        assert any("capture_loss " in record.getMessage() and "status=accepted" in record.getMessage()
                   for record in warnings)
    else:
        assert receipt.losses == ("body_omitted",) and closed.run_close_state == "clean"
        assert warnings == []
    assert all("BODY_SECRET" not in str(record.__dict__) for record in caplog.records)


@pytest.mark.parametrize("writer_fails", [False, True], ids=["healthy-writer", "failed-writer"])
def test_broken_application_handler_cannot_change_capture_or_writer_shutdown(tmp_path, monkeypatch, caplog, writer_fails):
    caplog.set_level(logging.DEBUG, logger="mcp_flightrecorder")
    attempts = []

    class BrokenHandler(logging.Handler):
        def emit(self, record):
            attempts.append(record)
            raise RuntimeError("HANDLER_SECRET")

    logger = logging.getLogger("mcp_flightrecorder")
    handler = BrokenHandler()
    logger.addHandler(handler)
    rec = None
    try:
        rec = Recorder(tmp_path / "PATH_SECRET.sqlite", capture="content", coverage=COVERAGE)
        if writer_fails:
            def fail_commit(connection):
                raise sqlite3.OperationalError("EXCEPTION_SECRET")
            monkeypatch.setattr(rec, "_commit_batch", fail_commit)
        receipt = rec.observe("cache.served", scope=rec.capture_scope(),
                              payload="BODY_SECRET", facts={"owner": "host"})
        assert receipt.status == "accepted"
        closed = rec.close()
        assert closed.writer_stopped and closed.portable
        assert closed.run_close_state == ("incomplete" if writer_fails else "clean")
        assert rec.health().writer_errors == int(writer_fails)
        assert rec.health().dropped_events == int(writer_fails)
    finally:
        logger.removeHandler(handler)
        if rec is not None:
            rec.close()
    assert any(record.getMessage().startswith("recorder_started ") for record in attempts)
    failures = [record for record in attempts if record.getMessage().startswith("writer_failed ")]
    assert len(failures) == int(writer_fails)
    if writer_fails:
        assert failures[0].levelno == logging.ERROR and "stage=transaction" in failures[0].getMessage()
    assert any(record.getMessage().startswith("recorder_close ") for record in attempts)
    assert all("SECRET" not in str(record.__dict__) and record.exc_info is None for record in attempts)
