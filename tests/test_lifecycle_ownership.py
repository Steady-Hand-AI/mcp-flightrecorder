"""Process ownership and narrowly owned failed-construction cleanup."""
import asyncio
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

from mcp_flightrecorder import Recorder, RecorderError
from mcp_flightrecorder import _recorder, _storage
from mcp_flightrecorder._types import CaptureRejected


COVERAGE = {"producers": {"model": "model", "host": "host", "mcp": "mcp"}}
SIDECARS = ("", "-wal", "-shm", "-journal")


def recorder(path):
    return Recorder(path, capture="content", coverage=COVERAGE)


def files(path):
    return {suffix: hashlib.sha256(Path(str(path) + suffix).read_bytes()).hexdigest()
            for suffix in SIDECARS if Path(str(path) + suffix).exists()}


class Forbidden:
    def __enter__(self):
        pytest.fail("Wrong-process use reached an inherited recorder lock")

    def __getattr__(self, name):
        pytest.fail("Wrong-process use reached inherited coordination or context state")


def forbidden(*args, **kwargs):
    pytest.fail("Wrong-process use reached recorder logging or storage")


@pytest.mark.parametrize("surface", [
    "observe", "start", "finish", "scope", "health", "close", "aclose", "finalize",
    "turn-enter", "dispatch-enter", "turn-exit", "dispatch-exit",
])
def test_foreign_process_rejects_before_owned_resources(tmp_path, monkeypatch, surface):
    path = tmp_path / "parent.sqlite"
    rec = recorder(path)
    turn = rec.turn("parent turn")
    turn.__enter__()
    dispatch = rec.dispatch(action="execute", name="answer", arguments={})
    dispatch.__enter__()
    scope = rec.capture_scope()
    started = rec.start_operation(scope=scope, client_id="client", method="tools/call", arguments={})
    assert started.status == "accepted"
    assert rec.finish_operation(started.ticket, status="ok", result={}).status == "accepted"
    new_turn = rec.turn("foreign turn")
    new_dispatch = rec.dispatch(action="execute", name="answer", arguments={})
    assert rec.close().writer_stopped
    before = files(path)
    previous_health = rec.health()
    try:
        with monkeypatch.context() as patch:
            # Simulate only the ownership boundary; never fork a threaded recorder.
            patch.setattr(rec, "_creator_pid", os.getpid() + 1, raising=False)
            patch.setattr(rec, "_condition", Forbidden())
            patch.setattr(rec, "_retry_lock", Forbidden())
            patch.setattr(rec, "_writer_thread", Forbidden())
            patch.setattr(rec, "_scope", Forbidden())
            patch.setattr(_recorder, "emit", forbidden)
            patch.setattr(_storage, "connect", forbidden)
            patch.setattr(_storage, "retry_finalization", forbidden)
            if surface == "observe":
                result = rec.observe("cache.served", scope=scope, facts={"owner": "host"})
            elif surface == "start":
                result = rec.start_operation(scope=scope, client_id="c", method="tools/call", arguments={})
            elif surface == "finish":
                result = rec.finish_operation(started.ticket, status="ok", result={})
            elif surface == "scope":
                result = rec.capture_scope()
                assert result._disabled and result.turn_id is None and result.dispatch_event_id is None
                return
            elif surface == "health":
                result = rec.health()
                assert result.state == "wrong_process"
                assert result.reason_counts["wrong_process"] == 1
                assert result.dropped_events == previous_health.dropped_events
                return
            elif surface in {"close", "finalize", "aclose"}:
                # Ownership is checked even before caller timeout validation.
                result = (asyncio.run(rec.aclose(timeout="invalid")) if surface == "aclose"
                          else getattr(rec, surface)(timeout="invalid"))
                assert (result.writer_stopped, result.run_close_state, result.portable, result.reason) == (
                    False, None, False, "wrong_process")
                return
            elif surface.endswith("-enter"):
                result = (new_turn if surface == "turn-enter" else new_dispatch).__enter__().receipt
            else:
                context = turn if surface == "turn-exit" else dispatch
                original = RuntimeError("host failure")
                assert context.__exit__(type(original), original, None) is False
                result = context.exit_receipt
            assert (result.status, result.reason, result.event_id) == ("rejected", "wrong_process", None)
    finally:
        assert files(path) == before, "Foreign-process calls must leave every parent recording byte untouched"
        dispatch.__exit__(None, None, None)
        turn.__exit__(None, None, None)


def test_retained_reader_does_not_touch_inherited_connection(tmp_path, monkeypatch):
    rec = recorder(tmp_path / "reader.sqlite")
    assert rec.close().writer_stopped
    reader = _recorder._IdentityReadScope(rec)
    reader.connection = Forbidden()
    with monkeypatch.context() as patch:
        patch.setattr(rec, "_creator_pid", os.getpid() + 1, raising=False)
        patch.setattr(rec, "_condition", Forbidden())
        patch.setattr(_storage, "connect", forbidden)
        with pytest.raises(CaptureRejected, match="wrong_process"):
            reader.lookup("event", "not-an-event")
        assert reader.__exit__(RuntimeError, RuntimeError("host failure"), None) is False


def test_fresh_subprocess_constructs_uses_and_closes_its_own_recorder(tmp_path):
    path = tmp_path / "worker.sqlite"
    script = """import json, sys
from mcp_flightrecorder import Recorder
rec = Recorder(sys.argv[1], capture='content', coverage={'producers': {'model': None, 'host': 'worker', 'mcp': None}})
with rec.turn('worker'):
    receipt = rec.observe('cache.served', scope=rec.capture_scope(), facts={'owner': 'host'})
closed = rec.close()
print(json.dumps({'status': receipt.status, 'state': closed.run_close_state, 'portable': closed.portable}))
"""
    result = subprocess.run([sys.executable, "-I", "-B", "-c", script, str(path)],
                            cwd=tmp_path, text=True, capture_output=True, timeout=15)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"status": "accepted", "state": "clean", "portable": True}
    with closing(sqlite3.connect(path)) as db:
        assert db.execute("SELECT count(*) FROM events").fetchone()[0] == 1


@pytest.mark.parametrize("failure", ["connect", "sqlite-full"])
def test_owned_failed_initialization_is_removed_after_handles_close(tmp_path, monkeypatch, failure):
    path = tmp_path / "failed.sqlite"
    original = _storage.connect

    def failing_connect(*args, **kwargs):
        if failure == "connect":
            raise sqlite3.OperationalError("controlled connection refusal")
        connection = original(*args, **kwargs)
        connection.execute("PRAGMA max_page_count=1")
        return connection

    with monkeypatch.context() as patch:
        patch.setattr(_storage, "connect", failing_connect)
        with pytest.raises(RecorderError) as error:
            recorder(path)
        if failure == "sqlite-full":
            assert error.value.__cause__.sqlite_errorcode == sqlite3.SQLITE_FULL
    assert files(path) == {}, "Only this failed initialization's owned files should have been removed"
    rec = recorder(path)
    assert rec.close().portable, "A removed failed initialization must not leave an unusable reserved pathname"


@pytest.mark.parametrize("suffix", SIDECARS)
def test_preexisting_destination_or_sidecar_is_never_removed(tmp_path, suffix):
    path = tmp_path / "existing.sqlite"
    existing = Path(str(path) + suffix)
    existing.write_bytes(b"pre-existing bytes")
    before = files(path)
    with pytest.raises(FileExistsError):
        recorder(path)
    assert files(path) == before


@pytest.mark.parametrize("replacement", ["main", "sidecar", "close-error"])
def test_cleanup_retains_replaced_or_uncertain_files(tmp_path, monkeypatch, replacement):
    path = tmp_path / "failed.sqlite"
    original = _storage.connect
    preserved = b"unrelated replacement bytes"

    class Connection:
        def __init__(self):
            self.inner = original(path, timeout=1)

        def executescript(self, script):
            raise sqlite3.OperationalError("controlled initialization failure")

        def close(self):
            self.inner.close()
            if replacement == "main":
                other = tmp_path / "replacement"
                other.write_bytes(preserved)
                os.replace(other, path)
            elif replacement == "sidecar":
                Path(str(path) + "-wal").write_bytes(preserved)
            else:
                raise sqlite3.OperationalError("controlled close uncertainty")

    monkeypatch.setattr(_storage, "connect", lambda *a, **kw: Connection())
    with pytest.raises(RecorderError, match="retained.*fresh path"):
        recorder(path)
    assert path.exists()
    if replacement == "main":
        assert path.read_bytes() == preserved
    elif replacement == "sidecar":
        assert Path(str(path) + "-wal").read_bytes() == preserved


@pytest.mark.parametrize("failure", ["after-initialize", "commit-acknowledgement"])
def test_committed_run_survives_later_initialization_exception(tmp_path, monkeypatch, failure):
    path = tmp_path / "valid.sqlite"
    original = _storage.Writer._initialize
    original_connect = _storage.connect

    def fail_after_commit(writer):
        original(writer)
        raise sqlite3.OperationalError("controlled failure after committed run")

    class Connection:
        def __init__(self, *args, **kwargs):
            self.inner = original_connect(*args, **kwargs)

        def __getattr__(self, name):
            return getattr(self.inner, name)

        def execute(self, statement, *args):
            result = self.inner.execute(statement, *args)
            if statement == "COMMIT":
                raise sqlite3.OperationalError("controlled failure after actual commit")
            return result

    if failure == "after-initialize":
        monkeypatch.setattr(_storage.Writer, "_initialize", fail_after_commit)
    else:
        monkeypatch.setattr(_storage, "connect", Connection)
    with pytest.raises(RecorderError, match="retained.*fresh path"):
        recorder(path)
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        assert db.execute("SELECT count(*) FROM runs").fetchone()[0] == 1


def test_replaced_reservation_is_not_opened_as_the_new_recording(tmp_path, monkeypatch):
    path, replacement = tmp_path / "requested.sqlite", tmp_path / "other.sqlite"
    other = recorder(replacement)
    assert other.close().portable
    preserved = replacement.read_bytes()
    original_reserve, original_connect = _storage.Writer._reserve_path, _storage.connect
    opens = []

    def replace_after_reservation(writer):
        original_reserve(writer)
        os.replace(replacement, path)

    def track_open(*args, **kwargs):
        opens.append(args[0])
        return original_connect(*args, **kwargs)

    monkeypatch.setattr(_storage.Writer, "_reserve_path", replace_after_reservation)
    monkeypatch.setattr(_storage, "connect", track_open)
    with pytest.raises(RecorderError, match="retained.*fresh path"):
        recorder(path)
    assert opens == [], "A changed exclusive-create identity must be refused before opening SQLite"
    assert path.read_bytes() == preserved


def test_unreadable_owned_remnant_is_retained_with_actionable_error(tmp_path, monkeypatch):
    path = tmp_path / "uncertain.sqlite"
    value = b"unreadable failed initialization"

    def fail_connect(*args, **kwargs):
        path.write_bytes(value)
        raise sqlite3.OperationalError("controlled initialization failure")

    monkeypatch.setattr(_storage, "connect", fail_connect)
    with pytest.raises(RecorderError, match="retained.*fresh path"):
        recorder(path)
    assert path.read_bytes() == value
