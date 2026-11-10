"""Single-owner SQLite persistence and bounded, mode-aware finalization."""
from contextlib import closing
from importlib.resources import files
import json
import logging
import os
from pathlib import Path
import sqlite3
import stat
import time

from ._types import LogicalRejection
from ._logging import emit


_logger = logging.getLogger(__name__)


def connect(path, *, readonly=False, timeout):
    uri = Path(path).absolute().as_uri() + ("?mode=ro" if readonly else "?mode=rw")
    return sqlite3.connect(uri, uri=True, timeout=timeout,
                           isolation_level=None, check_same_thread=True)


def insert_row(connection, table, row, *, conflict=None):
    """Insert a prepared row; table/column names come only from core builders."""
    columns = ",".join(row)
    placeholders = ",".join("?" for _ in row)
    suffix = "" if conflict is None else f" ON CONFLICT({conflict}) DO NOTHING"
    connection.execute(f"INSERT INTO {table} ({columns}) VALUES ({placeholders}){suffix}",
                       tuple(row.values()))


def _check_answer_membership(connection, link):
    details = json.loads(link["details_json"])
    if link["relation"] != "answers" or details.get("binding_format") != "message_binding/2":
        return
    witness = link["witness_event_id"]
    member = connection.execute("""SELECT 1 FROM request_members
        WHERE run_id=? AND request_event_id=? AND message_artifact_id=?
        AND message_pointer || ? = ? LIMIT 1""",
        (link["run_id"], witness, details["message_artifact_id"],
         details["relative_pointer"], link["target_pointer"])).fetchone()
    if member is None or witness != link["target_event_id"]:
        raise LogicalRejection("prerequisite_lost")


def _end_turn(connection, value):
    cursor = connection.execute("""UPDATE turns SET ended_at=?, host_outcome=?
        WHERE run_id=? AND turn_id=? AND ended_at IS NULL""",
        (value["ended_at"], value["host_outcome"], value["run_id"], value["turn_id"]))
    if cursor.rowcount != 1:
        raise LogicalRejection("turn_update_lost")


def _end_operation(connection, value):
    fields = {key: item for key, item in value.items() if key not in ("run_id", "operation_id")}
    cursor = connection.execute(
        "UPDATE operations SET " + ",".join(f"{key}=?" for key in fields)
        + " WHERE run_id=? AND operation_id=? AND status='running'",
        (*fields.values(), value["run_id"], value["operation_id"]))
    if cursor.rowcount != 1:
        raise LogicalRejection("duplicate_completion")


def persist_bundle(connection, bundle, sequence):
    """Participate in the writer's savepoint; never commit here."""
    value = bundle.decode()
    for artifact in value.get("artifacts", ()):
        insert_row(connection, "artifacts", artifact, conflict="artifact_id")
    if "turn" in value:
        insert_row(connection, "turns", value["turn"])
    if "event" in value:
        event = value["event"]
        event["sequence"] = sequence
        insert_row(connection, "events", event)
    if "operation" in value:
        insert_row(connection, "operations", value["operation"])
    if "operation_update" in value:
        _end_operation(connection, value["operation_update"])
    if "turn_end" in value:
        _end_turn(connection, value["turn_end"])
    for link in value.get("links", ()):
        _check_answer_membership(connection, link)
        insert_row(connection, "links", link, conflict="link_id")
    if bundle.partial and bundle.turn_id is not None:
        connection.execute("UPDATE turns SET capture_state='partial' WHERE run_id=? AND turn_id=?",
                           (value["run_id"], bundle.turn_id))


def _remaining_wait(connection, deadline, busy_timeout_ms):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        return False
    milliseconds = max(1, int(min(remaining, busy_timeout_ms / 1000) * 1000))
    connection.execute(f"PRAGMA busy_timeout={milliseconds}")
    return True


def _sqlite_reason(error):
    code = getattr(error, "sqlite_errorcode", None)
    if code is not None and code & 255 in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
        return "busy"
    return "storage_error"


def finalize_connection(connection, *, deadline, busy_timeout_ms):
    """Return readiness reason, without closing or claiming joined ownership."""
    if connection.in_transaction:
        return "storage_error"
    try:
        if not _remaining_wait(connection, deadline, busy_timeout_ms):
            return "timeout"
        mode = connection.execute("PRAGMA journal_mode").fetchone()[0].lower()
        if mode == "delete":
            return None
        if mode != "wal":
            return "unexpected_mode"
        if not _remaining_wait(connection, deadline, busy_timeout_ms):
            return "timeout"
        checkpoint = tuple(connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone())
        if len(checkpoint) != 3 or any(type(value) is not int for value in checkpoint):
            return "invalid_checkpoint"
        busy, log, checkpointed = checkpoint
        if log < 0 or checkpointed < 0:
            return "invalid_checkpoint"
        if busy != 0 or log != checkpointed:
            return "busy"
        if not _remaining_wait(connection, deadline, busy_timeout_ms):
            return "timeout"
        returned = connection.execute("PRAGMA journal_mode=DELETE").fetchone()[0].lower()
        return None if returned == "delete" else "unexpected_mode"
    except sqlite3.Error as error:
        return _sqlite_reason(error)


def verify_portable(path, *, deadline):
    """A separate connection proves the closed DELETE database actually reads."""
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        return "timeout"
    try:
        with closing(connect(path, readonly=True, timeout=min(remaining, 1.0))) as reader:
            if reader.execute("PRAGMA journal_mode").fetchone()[0].lower() != "delete":
                return "unexpected_mode"
            reader.execute("SELECT run_id, close_state FROM runs").fetchall()
            reader.execute("SELECT name FROM sqlite_schema").fetchall()
        return None
    except sqlite3.Error as error:
        return _sqlite_reason(error)
    except OSError:
        return "storage_error"


class Writer:
    """The thread running this object owns every write-connection operation."""
    def __init__(self, recorder):
        self.recorder = recorder
        self.connection = None
        self.sequence = 0
        self._created_file = None
        self._reservation_closed = False
        self._initial_run_committed = False

    def _reserve_path(self):
        path = self.recorder._path
        if not path.parent.is_dir():
            raise FileNotFoundError("The recording's parent directory must already exist")
        if any(os.path.lexists(str(path) + suffix) for suffix in ("", "-wal", "-shm", "-journal")):
            raise FileExistsError("The recording or a SQLite sidecar already exists")
        descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            created = os.fstat(descriptor)
            self._created_file = (created.st_dev, created.st_ino)
        finally:
            os.close(descriptor)
            self._reservation_closed = True

    def _owned_file_stat(self):
        current = self.recorder._path.lstat()
        if not stat.S_ISREG(current.st_mode) or (current.st_dev, current.st_ino) != self._created_file:
            return None
        return current

    def _cleanup_failed_initialization(self, connection_closed):
        """Remove only our unchanged, run-free main file after releasing handles.

        SQLite normally removes its own sidecars on close. A remaining sidecar
        has no proven creation identity here, so retain all remnants instead.
        """
        if self._created_file is None:
            return None
        if not connection_closed or not self._reservation_closed or self._initial_run_committed:
            return "retained"
        path = self.recorder._path

        def sidecars_exist():
            return any(os.path.lexists(str(path) + suffix) for suffix in ("-wal", "-shm", "-journal"))

        try:
            before = self._owned_file_stat()
            if before is None or sidecars_exist():
                return "retained"
            if before.st_size:
                # No journal/lock side effects while checking closed remnants.
                uri = path.as_uri() + "?mode=ro&immutable=1"
                with closing(sqlite3.connect(uri, uri=True, isolation_level=None)) as reader:
                    has_runs = reader.execute("SELECT 1 FROM sqlite_schema WHERE type='table' AND name='runs'").fetchone()
                    if has_runs and reader.execute("SELECT 1 FROM runs LIMIT 1").fetchone():
                        return "retained"
            after = self._owned_file_stat()
            if after is None or sidecars_exist() or (
                    before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
                    after.st_size, after.st_mtime_ns, after.st_ctime_ns):
                return "retained"
            path.unlink()
            return "removed"
        except (OSError, sqlite3.Error):
            return "retained"

    def _initialize(self):
        self._reserve_path()
        rec = self.recorder
        if self._owned_file_stat() is None:
            raise OSError("The newly reserved recording was replaced")
        self.connection = connect(rec._path, timeout=rec._limits["busy_timeout_ms"] / 1000)
        self.connection.executescript(files("mcp_flightrecorder").joinpath("schema.sql").read_text())
        self.connection.execute(f"PRAGMA busy_timeout={rec._limits['busy_timeout_ms']}")
        self.connection.execute("BEGIN IMMEDIATE")
        insert_row(self.connection, "runs", rec._initial_run())
        self.connection.execute("COMMIT")
        self._initial_run_committed = True

    def run(self):
        rec = self.recorder
        if not rec._in_creator_process():
            return
        try:
            self._initialize()
        except Exception as error:
            rec._initialization_failed(error)
            closed = self._close_connection() is None
            rec._init_cleanup = self._cleanup_failed_initialization(closed)
            rec._ready.set()
            return
        rec._ready.set()
        stage = "drain"
        try:
            self._drain()
            stage = "closure"
            self._record_closure()
        except Exception:
            rec._writer_failure(())
            emit(_logger, logging.ERROR, "writer_failed run_id=%s stage=%s", rec._run_id, stage)
        finally:
            self._finish_storage()

    def _next_batch(self):
        rec = self.recorder
        with rec._condition:
            rec._condition.wait_for(lambda: rec._queue or rec._state != "accepting")
            if not rec._queue:
                return None
            first = rec._queue.popleft()
            batch, size = [first], first.size
            deadline = first.admitted_at + rec._limits["batch_delay_ms"] / 1000
            while len(batch) < rec._limits["batch_items"] and size < rec._limits["batch_bytes"]:
                # Expiry bounds coalescing waits, not draining work already
                # queued. FIFO and item/byte caps still bound each transaction.
                if rec._queue:
                    candidate = rec._queue[0]
                    if size + candidate.size > rec._limits["batch_bytes"]:
                        break
                    batch.append(rec._queue.popleft())
                    size += candidate.size
                    continue
                remaining = deadline - time.monotonic()
                if rec._state != "accepting" or remaining <= 0:
                    break
                rec._condition.wait(remaining)
            return batch

    def _drain(self):
        while True:
            batch = self._next_batch()
            if batch is None:
                return
            if not self._transaction(batch):
                return

    def _savepoint(self, bundle):
        connection = self.connection
        connection.execute("SAVEPOINT capture_bundle")
        try:
            self.sequence += int(bundle.is_event)
            self.recorder._persist_bundle(connection, bundle)
        except LogicalRejection:
            connection.execute("ROLLBACK TO capture_bundle")
            connection.execute("RELEASE capture_bundle")
            raise
        connection.execute("RELEASE capture_bundle")

    def _transaction(self, batch):
        rec = self.recorder
        successful, discarded = [], []
        keep_running = True
        try:
            self.connection.execute("BEGIN IMMEDIATE")
            for index, bundle in enumerate(batch):
                try:
                    self._savepoint(bundle)
                    successful.append(bundle)
                except LogicalRejection as error:
                    if bundle.identities or bundle.kind == "turn.start":
                        discarded = batch[index:]
                        rec._prerequisite_failure(discarded)
                        keep_running = False
                        break
                    rec._discard_bundle(bundle, error.reason)
                    discarded.append(bundle)
            rec._commit_batch(self.connection)
            rec._commit_bundles(successful)
            return keep_running
        except Exception:
            try:
                if self.connection.in_transaction:
                    self.connection.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            discarded_ids = {id(bundle) for bundle in discarded}
            failed = [bundle for bundle in batch if id(bundle) not in discarded_ids]
            rec._writer_failure(failed)
            emit(_logger, logging.ERROR, "writer_failed run_id=%s stage=transaction failed_batch_bundles=%d",
                 rec._run_id, len(failed))
            return False

    def _mark_turn_losses(self, snapshot):
        rec, connection = self.recorder, self.connection
        if snapshot["global_loss"]:
            connection.execute("UPDATE turns SET capture_state='partial' WHERE run_id=?", (rec._run_id,))
        else:
            connection.executemany("UPDATE turns SET capture_state='partial' WHERE run_id=? AND turn_id=?",
                                   [(rec._run_id, turn_id) for turn_id in snapshot["loss_turns"]])

    def _unfinished_models(self):
        return self.connection.execute("""SELECT q.turn_id
            FROM events AS q
            WHERE q.run_id = ? AND q.kind = 'model.request'
              AND (q.model_call_id IS NULL OR q.model_call_id NOT IN (
                SELECT r.model_call_id FROM events AS r
                WHERE r.run_id = ? AND r.kind = 'model.response'
                  AND r.model_call_id IS NOT NULL
              ))""", (self.recorder._run_id, self.recorder._run_id)).fetchall()

    def _finish_turns(self, unfinished):
        run_id, connection = self.recorder._run_id, self.connection
        if any(row[0] is None for row in unfinished):
            connection.execute("UPDATE turns SET capture_state='partial' WHERE run_id=?", (run_id,))
        else:
            connection.executemany("UPDATE turns SET capture_state='partial' WHERE run_id=? AND turn_id=?",
                                   [(run_id, row[0]) for row in unfinished])
        connection.execute("""UPDATE turns SET capture_state=CASE
            WHEN ended_at IS NULL OR capture_state='partial' OR EXISTS
              (SELECT 1 FROM operations o WHERE o.run_id=turns.run_id AND o.turn_id=turns.turn_id
               AND o.status='incomplete') THEN 'partial' ELSE 'complete' END WHERE run_id=?""", (run_id,))

    def _record_closure(self):
        rec, connection = self.recorder, self.connection
        snapshot = rec._closure_snapshot()
        connection.execute("BEGIN IMMEDIATE")
        running = connection.execute("SELECT turn_id FROM operations WHERE run_id=? AND status='running'",
                                     (rec._run_id,)).fetchall()
        connection.execute("UPDATE operations SET status='incomplete' WHERE run_id=? AND status='running'",
                           (rec._run_id,))
        self._mark_turn_losses(snapshot)
        unfinished = self._unfinished_models()
        self._finish_turns(unfinished + running)
        partial = connection.execute("SELECT 1 FROM turns WHERE run_id=? AND capture_state='partial' LIMIT 1",
                                     (rec._run_id,)).fetchone() is not None
        incomplete = snapshot["capture_loss"] or bool(running or unfinished) or partial
        close_state = "incomplete" if incomplete else "clean"
        policy = rec._policy_with_health(snapshot)
        connection.execute("""UPDATE runs SET ended_at=?, dropped_events=?, writer_errors=?,
            close_state=?, policy_json=? WHERE run_id=?""",
            (rec._utc_now(), snapshot["dropped_events"], snapshot["writer_errors"], close_state,
             json.dumps(policy, ensure_ascii=False, separators=(",", ":")), rec._run_id))
        connection.execute("COMMIT")
        rec._closure_committed(close_state)

    def _close_connection(self):
        if self.connection is None:
            return None
        try:
            self.connection.close()
            return None
        except sqlite3.Error:
            return "storage_error"
        finally:
            self.connection = None

    def _finish_storage(self):
        rec = self.recorder
        if not rec._in_creator_process():
            return
        deadline = rec._shutdown_deadline or (time.monotonic() + 5.0)
        reason = "storage_error"
        if self.connection is not None:
            if self.connection.in_transaction:
                try:
                    self.connection.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
            reason = finalize_connection(self.connection, deadline=deadline,
                                         busy_timeout_ms=rec._limits["busy_timeout_ms"])
            reason = self._close_connection() or reason
        if reason is None:
            reason = verify_portable(rec._path, deadline=deadline)
        rec._finalization_finished(reason)


def retry_finalization(recorder, deadline):
    """Run only on the fresh retry thread, never borrow a writer connection."""
    if not recorder._in_creator_process():
        return
    reason, connection = "timeout", None
    try:
        remaining = deadline - time.monotonic()
        if remaining > 0:
            connection = connect(recorder._path, timeout=min(remaining, 1.0))
            reason = finalize_connection(connection, deadline=deadline,
                                         busy_timeout_ms=recorder._limits["busy_timeout_ms"])
    except sqlite3.Error as error:
        reason = _sqlite_reason(error)
    except OSError:
        reason = "storage_error"
    finally:
        if connection is not None:
            try:
                connection.close()
            except sqlite3.Error:
                reason = "storage_error"
    if reason is None:
        reason = verify_portable(recorder._path, deadline=deadline)
    recorder._finalization_finished(reason)
