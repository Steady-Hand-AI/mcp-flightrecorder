"""Owned read snapshots, bounded diagnostics and the inspection JSON envelope."""
from collections import OrderedDict
import json
import logging
import math
import os
from pathlib import Path
import sqlite3
import sys
import time

from ._logging import emit


_logger = logging.getLogger(__name__)

FORMAT = "mfr.inspect/1"
DEFAULT_OUTPUT_BYTES = 65_536
MAX_OUTPUT_BYTES = 1_048_576
DISPLAY_BYTES = 512
DIAGNOSTIC_LIMIT = 20

TABLE_COLUMNS = {
    "runs": "run_id started_at ended_at environment_json policy_json coverage_json dropped_events writer_errors close_state".split(),
    "artifacts": "artifact_id run_id media_type representation body_text stored_sha256 stored_bytes observed_bytes redacted truncated omitted_reason policy_id".split(),
    "turns": "run_id turn_id conversation_id started_at ended_at prompt_ref dataset_item_id repeat_index conditions_json condition_key host_outcome capture_state coverage_json".split(),
    "events": "event_id run_id turn_id sequence event_version kind boundary provenance source_id observed_at observed_mono_ms model_call_id decision_id operation_id parent_operation_id client_id server_key method name state payload_ref catalog_ref details_json".split(),
    "findings": "finding_id run_id turn_id target_event_id analysis_id checker checker_version config_json config_hash metric result score reason evidence_ids_json created_at".split(),
    "links": "link_id run_id witness_event_id source_event_id target_event_id relation source_slot source_pointer target_slot target_pointer details_json".split(),
    "operations": "run_id operation_id turn_id model_call_id decision_id dispatch_event_id link_basis client_id server_key method name status resolution input_rounds start_event_id end_event_id started_at ended_at duration_ms error_code".split(),
}
VIEW_COLUMNS = {
    "observed_events": TABLE_COLUMNS["events"],
    "request_members": "run_id request_event_id turn_id model_call_id request_sequence manifest_ref message_index message_path message_pointer message_artifact_id artifact_missing omitted_reason redacted truncated".split(),
    "answer_occurrences": "run_id request_event_id turn_id model_call_id request_sequence message_index message_artifact_id link_id source_event_id binding_witness_event_id binding_request_event_id target_pointer details_json association_state".split(),
}


class InspectionError(Exception):
    """One expected public refusal, with safe fixed diagnostic wording."""
    def __init__(self, code, reason, message):
        super().__init__(message)
        self.code, self.reason, self.message = code, reason, message


def invalid(reason, message):
    raise InspectionError("invalid_request", reason, message)


def validate_int(value, low, high, name):
    if type(value) is not int or not low <= value <= high:
        invalid("invalid_" + name, name + " must be an integer in its documented range.")


def validate_bool(value, name):
    if type(value) is not bool:
        invalid("invalid_" + name, name + " must be a boolean.")


def validate_id(value, name):
    if type(value) is not str or not value:
        invalid("invalid_" + name, name + " must be a nonempty string.")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        invalid("invalid_" + name, name + " must be valid UTF-8 text.")


def validate_timeout(timeout):
    if type(timeout) not in (int, float) or not 0 < timeout <= 30 or not math.isfinite(timeout):
        invalid("invalid_timeout", "timeout must be finite, positive and at most 30 seconds.")
    return float(timeout)


def validate_path(path):
    if not isinstance(path, (str, os.PathLike)):
        invalid("invalid_path", "A local recording file path is required.")
    try:
        value = os.fspath(path)
    except (TypeError, ValueError):
        invalid("invalid_path", "A local recording file path is required.")
    if type(value) is not str or not value or value.startswith("file:") or "\x00" in value:
        invalid("invalid_path", "Use a local file path, not a SQLite URI.")
    try:
        value.encode("utf-8")
        candidate = Path(value)
        try:
            return candidate.resolve(strict=True)
        except FileNotFoundError:
            return candidate.resolve(strict=False)
    except (OSError, RuntimeError, UnicodeEncodeError):
        invalid("invalid_path", "The recording path cannot be resolved.")


def json_value(text):
    try:
        return json.loads(text)
    except (TypeError, ValueError, RecursionError):
        return None


def json_object(text):
    value = json_value(text)
    return value if type(value) is dict else {}


def encode(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")


def envelope(operation, scope, data):
    return {"format": FORMAT, "operation": operation, "ok": True, "scope": scope,
            "data": data, "unknowns": [], "cutoffs": [],
            "diagnostics_omitted": {"unknowns": 0, "cutoffs": 0},
            "error": None, "untrusted_content": True}


def error_envelope(operation, code, reason, message):
    result = envelope(operation, {}, None)
    result.update(ok=False, error={"code": code, "reason": reason, "message": message})
    # Errors never echo arbitrary IDs, SQL, body content or exception reprs.
    if len(encode(result)) > 1024:
        result["error"]["message"] = "The requested inspection could not be completed."
    return result


_CACHE_MAX_BYTES = 8_388_608
_CACHE_MAX_ENTRIES = 4096
_CACHE_ENTRY_BYTES = 1_048_576
_CACHE_ENTRY_OVERHEAD = 256


class _ReadCache:
    """Bound retained immutable facts, not output arrays or total process memory."""
    def __init__(self, check_deadline):
        self.check_deadline = check_deadline
        self.entries = OrderedDict()
        self.bytes = 0
        self.high_water_bytes = self.high_water_entries = 0
        self.max_bytes = _CACHE_MAX_BYTES
        self.max_entries = _CACHE_MAX_ENTRIES
        self.max_entry_bytes = _CACHE_ENTRY_BYTES
        self.minima_disabled = False

    def _size(self, value):
        self.check_deadline()
        size = sys.getsizeof(value)
        if isinstance(value, sqlite3.Row):
            # sqlite3.Row owns a value tuple and column-name metadata. Count
            # them conservatively even if the cursor shares those objects.
            size += self._size(tuple(value)) + self._size(tuple(value.keys()))
        elif isinstance(value, tuple):
            size += sum(self._size(item) for item in value)
        return size

    def lookup(self, key):
        self.check_deadline()
        if key not in self.entries:
            return False, None
        value, _ = self.entries[key]
        self.entries.move_to_end(key)
        return True, value

    def store(self, key, value):
        self.check_deadline()
        cost = self._size(key) + self._size(value) + _CACHE_ENTRY_OVERHEAD
        if cost > min(self.max_bytes, self.max_entry_bytes) or self.max_entries < 1:
            if key[0] == "member_min":
                self.minima_disabled = True
            return
        if key in self.entries:
            _, previous_cost = self.entries.pop(key)
            self.bytes -= previous_cost
        while self.entries and (self.bytes + cost > self.max_bytes or len(self.entries) >= self.max_entries):
            self.check_deadline()
            old_key, (_, old_cost) = self.entries.popitem(last=False)
            self.bytes -= old_cost
            if old_key[0] == "member_min":
                self.minima_disabled = True
        self.entries[key] = (value, cost)
        self.bytes += cost
        self.high_water_bytes = max(self.high_water_bytes, self.bytes)
        self.high_water_entries = max(self.high_water_entries, len(self.entries))

    def get(self, key, load, *, admit=None):
        found, value = self.lookup(key)
        if found:
            return value
        value = load()
        self.check_deadline()
        if admit is None or admit(value):
            self.store(key, value)
        elif key[0] == "member_min":
            self.minima_disabled = True
        return value

    def clear(self):
        self.entries.clear()
        self.bytes = 0


class Context:
    """Own the deadline and diagnostics of one call, never a shared session."""
    def __init__(self, operation, scope, max_output_bytes=DEFAULT_OUTPUT_BYTES, no_content=False, timeout=10.0):
        self.operation = operation
        self.scope = scope
        self.run_id = scope.get("run_id")
        self.max_output_bytes = max_output_bytes
        self.no_content = no_content
        self.deadline = time.monotonic() + timeout
        self.connection = None
        self.journal_mode = None
        self.unknowns = []
        self.cutoffs = []
        self.diagnostics_omitted = {"unknowns": 0, "cutoffs": 0}
        self._cache = _ReadCache(self.check_deadline)
        self._member_through = {}

    def check_deadline(self):
        if time.monotonic() >= self.deadline:
            raise InspectionError("query_timeout", "deadline_expired", "The inspection deadline expired.")

    def _diagnostic(self, kind, code, path, ids, count):
        entries = getattr(self, kind)
        if len(entries) >= DIAGNOSTIC_LIMIT:
            self.diagnostics_omitted[kind] += 1
        else:
            entries.append({"code": code, "path": path, "ids": ids or {}, "count": count})

    def unknown(self, code, path="", ids=None, count=None):
        self._diagnostic("unknowns", code, path, ids, count)

    def cutoff(self, code, path="", ids=None, count=None):
        self._diagnostic("cutoffs", code, path, ids, count)


def display(value, ctx, path, identity=False):
    if type(value) is not str or identity:
        return value
    raw = value.encode("utf-8")
    if len(raw) <= DISPLAY_BYTES:
        return value
    clipped = raw[:DISPLAY_BYTES].decode("utf-8", errors="ignore")
    ctx.cutoff("string_limit", path, count=len(raw) - len(clipped.encode("utf-8")))
    return clipped


def _drop_diagnostic(ctx):
    for kind in ("unknowns", "cutoffs"):
        entries = getattr(ctx, kind)
        for index in range(len(entries) - 1, -1, -1):
            if kind == "cutoffs" and entries[index]["code"] == "byte_limit":
                continue
            entries.pop(index)
            ctx.diagnostics_omitted[kind] += 1
            return True
    return False


def _pointer_exists(data, pointer):
    if not pointer:
        return True
    if not pointer.startswith("/"):
        return False
    value = data
    for token in pointer[1:].split("/"):
        key = token.replace("~1", "/").replace("~0", "~")
        if isinstance(value, dict) and key in value:
            value = value[key]
        elif isinstance(value, list) and key.isdigit() and int(key) < len(value):
            value = value[int(key)]
        else:
            return False
    return True


def _prune_diagnostics(ctx, data):
    for kind in ("unknowns", "cutoffs"):
        entries = getattr(ctx, kind)
        retained = [item for item in entries if _pointer_exists(data, item["path"])]
        ctx.diagnostics_omitted[kind] += len(entries) - len(retained)
        entries[:] = retained


def _context_envelope(ctx, data):
    result = envelope(ctx.operation, ctx.scope, data)
    result["unknowns"] = ctx.unknowns
    result["cutoffs"] = ctx.cutoffs
    result["diagnostics_omitted"] = ctx.diagnostics_omitted
    return result


def finish_envelope(ctx, data, shrink=None):
    result = _context_envelope(ctx, data)
    clipped = False
    while True:
        ctx.check_deadline()
        _prune_diagnostics(ctx, data)
        size = len(encode(result))
        ctx.check_deadline()
        if size <= ctx.max_output_bytes:
            return result
        if not clipped:
            ctx.cutoff("byte_limit", "", count=None)
            clipped = True
        if shrink is not None and shrink():
            continue
        if _drop_diagnostic(ctx):
            continue
        raise InspectionError("limit_exceeded", "required_output", "The required intact output cannot fit its byte budget.")


def verify_schema(connection):
    if connection.execute("PRAGMA user_version").fetchone()[0] != 6:
        raise InspectionError("incompatible_schema", "schema_version", "This reader requires database schema version 6.")
    names = {row["name"]: row["type"] for row in connection.execute("SELECT name,type FROM sqlite_master WHERE type IN ('table','view')")}
    for expected_type, definitions in (("table", TABLE_COLUMNS), ("view", VIEW_COLUMNS)):
        for name, required in definitions.items():
            if names.get(name) != expected_type:
                raise InspectionError("incompatible_schema", "missing_schema_object", "A required database table or view is unavailable.")
            columns = {row["name"] for row in connection.execute('PRAGMA table_info("' + name + '")')}
            if not set(required) <= columns:
                raise InspectionError("incompatible_schema", "missing_schema_column", "A required database column is unavailable.")


def open_read(ctx, path):
    ctx.check_deadline()
    remaining = max(0, ctx.deadline - time.monotonic())
    connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=min(1.0, remaining))
    ctx.connection = connection
    connection.row_factory = sqlite3.Row
    connection.set_progress_handler(lambda: int(time.monotonic() >= ctx.deadline), 1000)
    connection.execute("PRAGMA query_only=ON")
    connection.execute("BEGIN")
    ctx.journal_mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0]).lower()
    verify_schema(connection)
    ctx.check_deadline()


def storage_error(ctx, error):
    if ctx is not None:
        try:
            ctx.check_deadline()
        except InspectionError as expired:
            return expired
        name = getattr(error, "sqlite_errorname", "")
        if ctx.journal_mode == "wal" and name in {"SQLITE_READONLY_CANTINIT", "SQLITE_READONLY_RECOVERY", "SQLITE_CANTOPEN_DIRTYWAL"}:
            return InspectionError("recording_open_wal", "wal_read_failure", "The WAL recording needs its accessible original or complete stopped fileset.")
    return InspectionError("recording_unreadable", "storage_failure", "The recording could not be read.")


def run_read(operation, path, scope, work, *, max_output_bytes,
             no_content, timeout=10.0, validate=None, content=False):
    ctx = None
    try:
        validate_int(max_output_bytes, 1024, MAX_OUTPUT_BYTES, "max_output_bytes")
        validate_bool(no_content, "no_content")
        timeout = validate_timeout(timeout)
        resolved = validate_path(path)
        for name, value in scope.items():
            validate_id(value, name)
        if validate is not None:
            validate()
        if content and no_content:
            raise InspectionError("content_disabled", "no_content", "Explicit content access is disabled by this output policy.")
        ctx = Context(operation, scope, max_output_bytes, no_content, timeout)
        ctx.path = resolved
        open_read(ctx, resolved)
        if ctx.run_id is not None and ctx.connection.execute("SELECT 1 FROM runs WHERE run_id=?", (ctx.run_id,)).fetchone() is None:
            raise InspectionError("unknown_run", "unknown_run", "The selected run is unavailable in this recording.")
        value = work(ctx)
        data, shrink = value if isinstance(value, tuple) else (value, None)
        result = finish_envelope(ctx, data, shrink)
    except InspectionError as error:
        result = error_envelope(operation, error.code, error.reason, error.message)
    except sqlite3.Error as error:
        translated = storage_error(ctx, error)
        result = error_envelope(operation, translated.code, translated.reason, translated.message)
    finally:
        if ctx is not None:
            ctx._cache.clear()
            ctx._member_through.clear()
        if ctx is not None and ctx.connection is not None:
            ctx.connection.set_authorizer(None)
            ctx.connection.set_progress_handler(None, 0)
            try:
                ctx.connection.rollback()
            finally:
                ctx.connection.close()
    # Host handlers run only after this call releases its read transaction.
    if result["ok"]:
        emit(_logger, logging.DEBUG, "inspection_completed operation=%s", operation)
    else:
        error = result["error"]
        level = logging.WARNING if error["code"] in {
            "incompatible_schema", "recording_unreadable", "recording_open_wal", "query_timeout"
        } else logging.DEBUG
        emit(_logger, level, "inspection_failed operation=%s code=%s reason=%s",
             operation, error["code"], error["reason"])
    return result
