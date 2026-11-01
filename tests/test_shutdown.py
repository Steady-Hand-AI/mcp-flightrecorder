"""Real SQLite closure, retry ownership, and asynchronous wait controls (A9)."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time

import pytest
import pytest_check as check

from mcp_flightrecorder import Recorder
from mcp_flightrecorder import _storage

COVERAGE = {"producers": {"model": "model", "host": "host", "mcp": "mcp"}}


def accepted(receipt):
    assert receipt.status == "accepted", f"The shutdown fixture must first admit its intended observation: {receipt.reason}."
    return receipt


def block_writer(rec, monkeypatch):
    reached, release = threading.Event(), threading.Event()
    original = rec._persist_bundle

    def held(connection, bundle):
        reached.set()
        assert release.wait(5), "Every controlled writer hold must be released by its test owner."
        return original(connection, bundle)

    monkeypatch.setattr(rec, "_persist_bundle", held)
    accepted(rec.observe("cache.served", scope=rec.capture_scope(), facts={"owner": "host"}))
    assert reached.wait(5), "The writer must hold admitted data before a concurrent shutdown/wait control begins."
    return release


def track_connections(monkeypatch):
    original = _storage.connect
    records = []
    lock = threading.Lock()

    class OwnedConnection:
        def __init__(self, connection, record):
            object.__setattr__(self, "connection", connection)
            object.__setattr__(self, "record", record)

        def __getattr__(self, name):
            value = getattr(self.connection, name)
            if not callable(value):
                return value

            def call(*args, **kwargs):
                current = threading.current_thread()
                with lock:
                    self.record["uses"].append((name, current))
                assert current is self.record["owner"], "Each SQLite connection must be used and closed only by its creating thread."
                result = value(*args, **kwargs)
                if name == "close":
                    self.record["closed"] = True
                return result
            return call

        def __setattr__(self, name, value):
            setattr(self.connection, name, value)

    def connect(path, *, readonly=False, timeout=1.0):
        record = {"owner": threading.current_thread(), "readonly": readonly, "uses": [], "closed": False}
        connection = original(path, readonly=readonly, timeout=timeout)
        with lock:
            records.append(record)
        return OwnedConnection(connection, record)

    monkeypatch.setattr(_storage, "connect", connect)
    return records


def test_writer_initialization_use_finalization_and_close_stay_on_owner_thread(tmp_path, monkeypatch):
    connections = track_connections(monkeypatch)
    caller = threading.current_thread()
    rec = Recorder(tmp_path / "capture.sqlite", capture="content", coverage=COVERAGE)
    try:
        accepted(rec.observe("cache.served", scope=rec.capture_scope(), facts={"owner": "host"}))
        closed = rec.close()
        assert closed.writer_stopped and closed.portable, "Owner-thread qualification requires an actually joined portable close."
        writers = [record for record in connections if not record["readonly"]]
        assert len(writers) == 1, "One recorder must have exactly one initial write-connection owner."
        check.is_not(writers[0]["owner"], caller, msg="The constructor's caller must not initialize or borrow the writer's connection.")
        check.is_true(any(name == "executescript" for name, _ in writers[0]["uses"]),
                      msg="Schema initialization belongs to the same tracked writer connection as persistence.")
        check.is_true(all(record["closed"] for record in connections),
                      msg="Portable close requires explicit closure of writer and verification-reader connections.")
        check.is_true(all(owner is record["owner"] for record in connections for _, owner in record["uses"]),
                      msg="No SQLite connection may transfer across threads during initialization, writes, verification, or close.")
        with closing(sqlite3.connect((tmp_path / "capture.sqlite").as_uri() + "?mode=ro", uri=True)) as reader:
            check.equal(reader.execute("PRAGMA journal_mode").fetchone()[0], "delete",
                        msg="Thread ownership does not replace the actual persisted DELETE-mode requirement.")
            check.equal(reader.execute("SELECT count(*) FROM events").fetchone()[0], 1,
                        msg="A fresh read must retain the admitted observation after writer shutdown.")
    finally:
        rec.close()


def test_real_held_reader_failure_preserves_rows_and_retries_on_fresh_owner(tmp_path, monkeypatch):
    connections = track_connections(monkeypatch)
    path = tmp_path / "capture.sqlite"
    rec = Recorder(path, capture="content", coverage=COVERAGE, limits={"busy_timeout_ms": 20})
    reader = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
    try:
        reader.execute("BEGIN")
        assert reader.execute("SELECT close_state FROM runs").fetchone()[0] == "open", (
            "The real held reader must establish an earlier committed WAL snapshot before new recording writes."
        )
        event = accepted(rec.observe("cache.served", scope=rec.capture_scope(), facts={"owner": "host"}))
        closed = rec.close(timeout=1)
        assert closed.writer_stopped, "Expected checkpoint busy must still close and join the writer owner."
        check.is_false(closed.portable, msg="A held old reader must prevent a false standalone-portability claim.")
        check.equal(closed.reason, "busy", msg="The real checkpoint refusal must identify its held-reader cause.")
        check.equal(closed.run_close_state, "clean", msg="Logical closure can be clean while file portability is temporarily blocked.")
        check.is_true(Path(str(path) + "-wal").exists(), msg="Busy finalization must retain the WAL containing committed evidence.")
        again = rec.finalize(timeout=1)
        check.is_false(again.portable, msg="A retry cannot claim portability while the same reader still holds its old snapshot.")
        check.equal(again.reason, "busy", msg="An expected retry busy condition remains distinct from a storage error.")
        reader.rollback()
        reader.close()
        ready = rec.finalize(timeout=2)
        assert ready.writer_stopped and ready.portable, "Releasing the reader must allow a fresh-owner retry to verify a standalone file."
        check.is_none(ready.reason, msg="A successful actual checkpoint, DELETE transition, and fresh read need no failure diagnostic.")
        writers = [record for record in connections if not record["readonly"]]
        assert len(writers) >= 3, "Initial closure, busy retry, and successful retry must use their own real connections."
        check.is_true(all(record["closed"] for record in connections),
                      msg="Every writer/retry/verification connection must be explicitly closed before portability is reported.")
        check.is_true(all(record["owner"] is not writers[0]["owner"] for record in writers[1:]),
                      msg="Retry owners must be fresh thread objects, even if the OS later reuses numeric thread IDs.")
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as verified:
            check.equal(verified.execute("SELECT event_id FROM events").fetchall(), [(event.event_id,)],
                        msg="Checkpoint failure and retries must preserve the original admitted event.")
            check.equal(verified.execute("PRAGMA journal_mode").fetchone()[0], "delete",
                        msg="Successful retry must persist DELETE journal mode.")
            ended_at = verified.execute("SELECT ended_at FROM runs").fetchone()[0]
        repeat = rec.finalize(timeout=1)
        check.is_true(repeat.portable, msg="Already-DELETE repeated finalization must remain a valid success.")
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as verified:
            check.equal(verified.execute("SELECT ended_at FROM runs").fetchone()[0], ended_at,
                        msg="Finalization retries must not extend or rewrite the logical recording interval.")
        check.equal(rec.health().writer_errors, 0, msg="Expected busy checkpoints are diagnostics, not failed writer transactions.")
        check.is_true(rec.health().reason_counts.get("finalize_busy", 0) >= 1,
                      msg="The held-reader failure must remain visible in bounded in-memory finalization diagnostics.")
    finally:
        reader.close()
        rec.close()


def test_running_writer_cannot_be_reported_joined_and_repeated_close_resumes_wait(tmp_path, monkeypatch):
    rec = Recorder(tmp_path / "capture.sqlite", capture="content", coverage=COVERAGE, limits={"batch_items": 1})
    release = block_writer(rec, monkeypatch)
    try:
        premature = rec.finalize(timeout=1)
        check.is_false(premature.writer_stopped, msg="Finalization cannot transfer or finalize a still-running owner's connection.")
        check.is_false(premature.portable, msg="A live writer cannot produce a verified standalone-file claim.")
        check.equal(premature.reason, "writer_running", msg="Premature finalize must explain that ownership has not ended.")
        timed_out = rec.close(timeout=0.01)
        check.is_false(timed_out.writer_stopped, msg="A finite close wait cannot report the still-gated writer joined.")
        check.is_false(timed_out.portable, msg="Timeout is never evidence of successful finalization.")
        check.equal(timed_out.reason, "timeout", msg="Exhausted caller waiting must retain an explicit timeout result.")
        check.equal(rec.health().ordinary.used_slots, 1,
                    msg="Timed-out close cannot release capacity still owned by the blocked writer.")
        late = rec.observe("cache.served", scope=rec.capture_scope(), facts={"owner": "host"})
        check.equal(late.status, "closed", msg="Shutdown starts by stopping new admission even before its writer has joined.")
    finally:
        release.set()
    try:
        joined = rec.close(timeout=5)
        assert joined.writer_stopped, "A later close call must resume waiting for the same writer and eventually join it."
        ready = rec.finalize(timeout=2)
        check.is_true(ready.portable, msg="A fresh finalization budget can complete portability after the earlier caller timeout.")
        check.equal(rec.health().ordinary.used_slots, 0, msg="The eventually committed held bundle must release its capacity.")
    finally:
        rec.close()


def test_concurrent_and_repeated_close_callers_share_one_shutdown(tmp_path, monkeypatch):
    rec = Recorder(tmp_path / "capture.sqlite", capture="content", coverage=COVERAGE, limits={"batch_items": 1})
    release = block_writer(rec, monkeypatch)
    rendezvous = threading.Barrier(4)

    def close_caller():
        rendezvous.wait(timeout=5)
        return rec.close(timeout=5)

    try:
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = [pool.submit(close_caller) for _ in range(3)]
            rendezvous.wait(timeout=5)
            with rec._condition:
                assert rec._condition.wait_for(lambda: rec._state != "accepting", timeout=5), (
                    "At least one concurrent caller must initiate shutdown before the writer gate is released."
                )
            release.set()
            results = [future.result(timeout=5) for future in futures]
        check.is_true(all(result.writer_stopped and result.portable for result in results),
                      msg="Every concurrent caller must observe the same joined, verified shutdown.")
        check.equal({result.run_close_state for result in results}, {"clean"},
                    msg="Concurrent close calls must not synthesize duplicate or conflicting logical closures.")
        again = rec.close()
        check.is_true(again.writer_stopped and again.portable, msg="Repeated close must preserve the completed shutdown result.")
        check.equal((rec.health().writer_errors, rec.health().ordinary.used_slots, rec.health().completion.used_slots), (0, 0, 0),
                    msg="Concurrent shutdown must neither double-release capacity nor manufacture storage failures.")
    finally:
        release.set()
        rec.close()


def test_cancelled_aclose_waiter_does_not_cancel_shutdown_or_block_event_loop(tmp_path, monkeypatch, run_sdk_async):
    rec = Recorder(tmp_path / "capture.sqlite", capture="content", coverage=COVERAGE, limits={"batch_items": 1})
    release = block_writer(rec, monkeypatch)
    entered = threading.Event()
    original_close = rec.close
    original_join = rec._join_writer

    def noticed_join(deadline):
        entered.set()
        return original_join(deadline)

    # Async shutdown now starts before executor admission; this existing control
    # still cancels only after the real join worker has actually begun running.
    monkeypatch.setattr(rec, "_join_writer", noticed_join)

    async def exercise():
        waiter = asyncio.create_task(rec.aclose(timeout=5))
        assert await asyncio.to_thread(entered.wait, 2), "Async close must begin shutdown while allowing the event loop to keep running."
        assert not waiter.done(), "The async waiter must yield while its writer is deliberately held, instead of blocking until timeout."
        check.is_false(release.is_set(), msg="The event loop remains responsive before any test-owned release of writer work.")
        waiter.cancel()
        # Cancelling the awaiting task must propagate its cancellation without cancelling recorder shutdown.
        with pytest.raises(asyncio.CancelledError):
            await waiter
        check.is_true(rec.health().state != "accepting", msg="Cancellation of a waiter must not restart capture admission.")
        release.set()
        completed = await asyncio.to_thread(original_close, 5)
        check.is_true(completed.writer_stopped and completed.portable,
                      msg="Shutdown must continue independently and reach a joined portable result after waiter cancellation.")
        check.equal(rec.health().writer_errors, 0, msg="Cancelling an asynchronous waiter is not a storage failure.")

    try:
        run_sdk_async(exercise(), timeout=4)
    finally:
        release.set()
        original_close()


@pytest.mark.parametrize("checkpoint", [(0, -1, -1), (0, 3, -1)], ids=["no-applicable-wal", "negative-progress"])
def test_negative_checkpoint_counters_supplement_real_sqlite_controls(checkpoint):
    class Cursor:
        def __init__(self, row):
            self.row = row

        def fetchone(self):
            return self.row

    class Connection:
        in_transaction = False

        def __init__(self):
            self.statements = []

        def execute(self, statement):
            self.statements.append(statement)
            if statement == "PRAGMA journal_mode":
                return Cursor(("wal",))
            if statement == "PRAGMA wal_checkpoint(TRUNCATE)":
                return Cursor(checkpoint)
            if statement.startswith("PRAGMA busy_timeout="):
                return Cursor(())
            raise AssertionError("Invalid WAL progress must not proceed to a DELETE transition")

    connection = Connection()
    reason = _storage.finalize_connection(connection, deadline=time.monotonic() + 1, busy_timeout_ms=10)
    check.equal(reason, "invalid_checkpoint", msg="Unexpected negative WAL counters cannot establish checkpoint success or data loss.")
    check.is_not_in("PRAGMA journal_mode=DELETE", connection.statements,
                    msg="Invalid checkpoint evidence must stop the portability transition before changing journal mode.")


def test_already_delete_mode_is_real_and_does_not_require_wal_progress(tmp_path):
    path = tmp_path / "delete.sqlite"
    with closing(sqlite3.connect(path)) as db:
        db.execute("CREATE TABLE retained(value INTEGER)")
        db.execute("INSERT INTO retained VALUES(7)")
        db.commit()
        assert db.execute("PRAGMA journal_mode").fetchone()[0] == "delete", "The control must start in actual DELETE mode."
        reason = _storage.finalize_connection(db, deadline=time.monotonic() + 1, busy_timeout_ms=10)
        check.is_none(reason, msg="Already-DELETE databases must not require an inapplicable WAL checkpoint.")
        check.equal(db.execute("SELECT value FROM retained").fetchall(), [(7,)],
                    msg="Mode-aware finalization must preserve committed DELETE-mode rows.")


@pytest.mark.skipif(os.name != "posix", reason="POSIX unprivileged standalone-media probe")
def test_core_closed_delete_opens_on_unprivileged_readonly_media():
    # A direct temporary directory keeps all ancestors traversable for the child.
    with tempfile.TemporaryDirectory(prefix="flightrec-core-portability-") as temporary:
        directory = Path(temporary)
        directory.chmod(0o755)
        path = directory / "capture.sqlite"
        rec = Recorder(path, capture="content", coverage=COVERAGE)
        try:
            accepted(rec.observe("cache.served", scope=rec.capture_scope(), facts={"owner": "host"}))
            result = rec.close()
            assert result.writer_stopped and result.portable, "The media probe requires an actually finalized installed-core recording."
            path.chmod(0o444)
            directory.chmod(0o555)
            script = '''import json,os,sqlite3,sys
from contextlib import closing
if os.geteuid()==0:
    try:
        os.setgroups([]); os.setgid(65534); os.setuid(65534)
    except PermissionError:
        print(json.dumps({'drop_unavailable':True})); raise SystemExit(0)
with closing(sqlite3.connect(sys.argv[1],uri=True)) as db:
    print(json.dumps({'uid':os.geteuid(),'events':db.execute('SELECT count(*) FROM events').fetchone()[0],
                      'mode':db.execute('PRAGMA journal_mode').fetchone()[0]}))
'''
            process = subprocess.run([sys.executable, "-I", "-B", "-c", script, path.as_uri() + "?mode=ro"],
                                     capture_output=True, text=True, cwd=directory, timeout=10)
            assert process.returncode == 0, f"The unprivileged core reader must execute successfully before decoding its report: {process.stderr}."
            report = json.loads(process.stdout)
            if report.get("drop_unavailable"):
                pytest.skip("Unprivileged core read requires an ordinary user or allowed UID drop")
            assert report["uid"] != 0, "A root reader would bypass the read-only-media restriction under test."
            check.equal(report["events"], 1, msg="An unprivileged standalone reader must see the admitted installed-core event.")
            check.equal(report["mode"], "delete", msg="The standalone core file must retain verified DELETE journal mode.")
        finally:
            directory.chmod(0o755)
            if path.exists():
                path.chmod(0o644)
            rec.close()
