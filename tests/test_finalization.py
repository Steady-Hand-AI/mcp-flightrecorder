"""A9 regression: successful SQLite work cannot outrun retry-owner exit."""
from contextlib import closing
import logging
import sqlite3
import threading

import pytest_check as check

from mcp_flightrecorder import Recorder, _storage


COVERAGE = {"producers": {"model": "model", "host": "host", "mcp": "mcp"}}


def test_retry_success_is_not_portable_until_retry_owner_exits(tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.DEBUG, logger="mcp_flightrecorder")
    path = tmp_path / "retry-owner.sqlite"
    rec = Recorder(path, capture="metadata", coverage=COVERAGE, limits={"busy_timeout_ms": 20})
    reader = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, isolation_level=None)
    announced, release = threading.Event(), threading.Event()
    results, failures, retry_owners = [], [], []
    caller = None
    try:
        reader.execute("BEGIN")
        assert reader.execute("SELECT count(*) FROM events").fetchone()[0] == 0, (
            "The real reader must hold an earlier WAL snapshot before the new observation commits."
        )
        observation = rec.observe("cache.served", scope=rec.capture_scope(), facts={"owner": "host"})
        assert observation.status == "accepted", "The real busy-reader control requires one admitted observation."
        initial = rec.close(timeout=1)
        assert initial.writer_stopped and not initial.portable and initial.reason == "busy", (
            "A real held reader must block initial WAL finalization before exercising the fresh-owner retry window."
        )
        reader.execute("ROLLBACK")
        reader.close()
        reader = None
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as before:
            original_end = before.execute("SELECT ended_at FROM runs").fetchone()[0]

        real_retry = _storage.retry_finalization

        def hold_successful_retry(recorder, deadline):
            retry_owners.append(threading.current_thread())
            try:
                real_retry(recorder, deadline)
            except BaseException as error:
                failures.append(error)
            finally:
                announced.set()
            if not release.wait(5):
                failures.append(RuntimeError("The test-owned retry-exit barrier was not released"))

        def first_finalize_caller():
            try:
                results.append(rec.finalize(timeout=5))
            except BaseException as error:
                failures.append(error)

        monkeypatch.setattr(_storage, "retry_finalization", hold_successful_retry)
        caller = threading.Thread(target=first_finalize_caller, name="test-first-finalize")
        caller.start()
        assert announced.wait(5), "The real fresh-owner retry must finish SQLite verification before its exit is deliberately held."
        assert not failures, f"The retry window must follow successful real finalization, not an injected storage error: {failures}."
        with rec._condition:
            internal_success = rec._portable
            retry_alive = rec._retry_thread.is_alive()
        assert internal_success and retry_alive, (
            "The regression requires the exact window where SQLite success is recorded but its retry owner has not exited."
        )

        simultaneous_close = rec.close(timeout=1)
        check.equal((simultaneous_close.writer_stopped, simultaneous_close.portable, simultaneous_close.reason),
                    (True, False, "writer_running"),
                    msg="A joined original writer cannot make a recording portable while its successful retry owner is still alive.")
        simultaneous_retry = rec.finalize(timeout=0.01)
        check.equal((simultaneous_retry.writer_stopped, simultaneous_retry.portable, simultaneous_retry.reason),
                    (True, False, "timeout"),
                    msg="A concurrent finalizer must wait on the same live retry owner and report its bounded wait without false portability.")
        check.equal(len(retry_owners), 1,
                    msg="Overlapping finalize calls must share the existing retry instead of opening a second storage owner.")
        assert any("storage_finalized " in record.getMessage() for record in caplog.records)
        assert not any("portable=True" in record.getMessage() for record in caplog.records), (
            "Internal storage success must not produce a public portability log before its owner exits."
        )

        release.set()
        caller.join(5)
        assert not caller.is_alive(), "The test-owned finalizer caller must complete after the retry owner is released."
        assert not failures and len(results) == 1, f"The joined retry must return exactly one successful closure result: {failures}."
        check.equal((results[0].writer_stopped, results[0].portable, results[0].reason), (True, True, None),
                    msg="Portability becomes reportable only after the verified retry owner has actually exited and been joined.")
        check.is_false(retry_owners[0].is_alive(), msg="The successful portability receipt must have no remaining retry owner.")
        repeated = rec.finalize(timeout=1)
        check.equal((repeated.writer_stopped, repeated.portable, repeated.reason), (True, True, None),
                    msg="Repeated finalization after joined success must retain the verified portable result.")
        check.equal(len(retry_owners), 1, msg="Repeated verified success must not start another finalization connection.")
        portable_logs = [record for record in caplog.records if "portable=True" in record.getMessage()]
        assert len(portable_logs) == 1 and portable_logs[0].levelno == logging.INFO, (
            "A joined clean retry logs portability once; unchanged repeat calls do not flood the logger."
        )
        check.equal(rec.health().writer_errors, 0,
                    msg="Expected reader contention and a timed-out waiter must not be counted as failed storage attempts.")
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as after:
            check.equal(after.execute("PRAGMA journal_mode").fetchone()[0].lower(), "delete",
                        msg="The eventual joined result must retain actual DELETE mode for a fresh read-only connection.")
            check.equal(after.execute("SELECT event_id FROM events").fetchall(), [(observation.event_id,)],
                        msg="The retry ownership guard must preserve the observation committed before reader contention.")
            check.equal(after.execute("SELECT ended_at FROM runs").fetchone()[0], original_end,
                        msg="Portability retries cannot extend the recording interval after logical closure.")
    finally:
        release.set()
        if reader is not None:
            reader.close()
        if caller is not None:
            caller.join(5)
        rec.close(timeout=5)
