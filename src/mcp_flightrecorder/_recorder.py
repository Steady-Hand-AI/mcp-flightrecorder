"""Bounded FIFO admission, recorder-issued scopes, and shutdown coordination."""
import asyncio
from collections import OrderedDict, deque
from contextlib import closing
from contextvars import ContextVar
from dataclasses import replace
from datetime import datetime, timezone
import hashlib
import hmac
import json
import logging
import os
from pathlib import Path
import sqlite3
import threading
import time
from types import MappingProxyType
import uuid

from . import _storage
from ._artifacts import Artifacts
from ._capture import Preparation, Prepared, encode, event_metadata
from ._config import RESERVATION_BYTES, constructor_config, timeout_seconds
from ._logging import emit
from ._types import (
    Capacity, CaptureRejected, CaptureResult, CaptureScope, CloseResult, Health,
    RecorderError, Ticket,
)
from ._values import InvalidInput, MISSING, freeze, json_text, validate_control


REASONS = frozenset(("invalid_input", "invalid_scope", "unavailable_boundary", "capacity",
                    "duplicate_completion", "prerequisite_lost", "writer_failed", "closed",
                    "body_omitted", "partial", "unresolved", "turn_update_lost",
                    "finalize_busy", "finalize_timeout", "other"))
_NO_SCOPE = object()
_logger = logging.getLogger(__name__)


class _IdentityReadScope:
    """One caller-owned lazy reader; each lookup is a fresh autocommit statement."""
    def __init__(self, recorder):
        self.recorder = recorder
        self.connection = None

    def __enter__(self):
        return self

    def lookup(self, category, identity):
        return self.recorder._lookup(category, identity, fallback=self._read)

    def _read(self, category, identity):
        self.recorder._check_process()
        if self.connection is None:
            self.connection = _storage.connect(
                self.recorder._path, readonly=True,
                timeout=self.recorder._limits["busy_timeout_ms"] / 1000)
            self.connection.row_factory = sqlite3.Row
        return self.recorder._lookup_connection(self.connection, category, identity)

    def __exit__(self, error_type, error, traceback):
        if not self.recorder._in_creator_process():
            return False
        connection, self.connection = self.connection, None
        if connection is not None:
            try:
                connection.close()
            except BaseException:
                # Retry resource release once, but never turn failed cleanup
                # into admission. Preserve the original cleanup exception.
                try:
                    connection.close()
                except BaseException:
                    pass
                raise
        return False


class _AmbientScope:
    __slots__ = ("scope", "previous", "foreign_closed")

    def __init__(self, scope, previous):
        self.scope = scope
        self.previous = previous
        self.foreign_closed = False


class _ScopeContext:
    """Context restoration never owns or suppresses the host's execution."""
    def __init__(self, recorder, kind, arguments):
        self.recorder, self.kind, self.arguments = recorder, kind, arguments
        self.receipt = self.scope = self.exit_receipt = self._token = None
        self._frame = None

    def __enter__(self):
        rec = self.recorder
        if not rec._in_creator_process():
            self.receipt = CaptureResult("rejected", "wrong_process")
            self.scope = rec._foreign_scope()
            return self
        previous = rec.capture_scope()
        if self.kind == "turn":
            self.receipt = rec._enter_turn(**self.arguments)
            accepted = self.receipt.status == "accepted"
            self.scope = rec._issue_scope(self.receipt.turn_id if accepted else None, None, disabled=not accepted)
        else:
            self.receipt = rec._capture(lambda preparation: preparation.dispatch(previous, **self.arguments),
                                        scope=previous)
            dispatch = self.receipt.event_id if self.receipt.status == "accepted" else None
            self.scope = rec._issue_scope(previous.turn_id, dispatch, disabled=previous._disabled)
        self._frame = _AmbientScope(self.scope, rec._current_scope_frame())
        self._token = rec._scope.set(self._frame)
        return self

    def __exit__(self, error_type, error, traceback):
        rec = self.recorder
        if not rec._in_creator_process():
            self.exit_receipt = CaptureResult("rejected", "wrong_process")
            return False
        try:
            if self.kind == "turn":
                if self.receipt.status == "accepted":
                    outcome = "returned" if error_type is None else "error"
                    if error_type is not None and issubclass(error_type, asyncio.CancelledError):
                        outcome = "cancelled"
                    self.exit_receipt = rec._exit_turn(self.scope, outcome)
                else:
                    self.exit_receipt = CaptureResult("rejected", "invalid_scope")
            else:
                self.exit_receipt = CaptureResult("accepted", turn_id=self.scope.turn_id)
        finally:
            try:
                rec._scope.reset(self._token)
            except ValueError:
                self._frame.foreign_closed = True
                if rec._scope.get() is self._frame:
                    rec._current_scope_frame()
        return False


class Recorder:
    """Freeze explicit observations and admit them to one owned SQLite writer.

    An accepted receipt means FIFO admission, never per-event durability. Use
    close/finalize results for storage readiness and run health for capture loss.
    """
    def __init__(self, path, *, capture, coverage, comparisons=True, environment=None, limits=None):
        self._creator_pid = os.getpid()
        self._coverage, self._environment, self._limits = constructor_config(
            capture, coverage, comparisons, environment, limits)
        try:
            self._path = Path(path).absolute()
        except (TypeError, ValueError) as error:
            raise ValueError("path must name a new local recording") from error
        if not self._path.parent.is_dir():
            raise ValueError("the recording parent directory must already exist")
        self._comparisons = comparisons
        self._key, self._run_id = os.urandom(32), "r_" + uuid.uuid4().hex[:24]
        self._started_at = self._utc_now()
        self._policy = {"capture": capture, "comparisons": comparisons, "limits": self._limits,
                        "projection_max_bytes": 1_048_576, "sqlite": {"journal_mode": "wal", "synchronous": "FULL"}}
        encode(self._environment)
        encode(self._coverage)
        encode(self._policy)
        self._condition = threading.Condition(threading.RLock())
        self._queue, self._pending, self._committed = deque(), {}, OrderedDict()
        self._tickets, self._ticket_owner = {}, object()
        self._ordinary_bytes = self._ordinary_slots = self._completion_bytes = self._completion_slots = 0
        self._state, self._stop_reason = "accepting", None
        self._dropped_events = self._writer_errors = 0
        self._reason_counts = {reason: 0 for reason in sorted(REASONS)}
        self._logged_capture_reasons = set()
        self._last_close_log = None
        self._capture_loss, self._global_loss, self._loss_turns = False, False, set()
        self._run_close_state, self._portable, self._final_reason = "open", False, None
        self._shutdown_deadline = None
        self._ready, self._init_error = threading.Event(), None
        self._init_cleanup = None
        self._retry_lock, self._retry_thread = threading.Lock(), None
        self._scope = ContextVar("mcp_flightrecorder_scope_" + self._run_id, default=None)
        self._artifacts = Artifacts(self._run_id, self._key, capture, comparisons)
        self._writer = _storage.Writer(self)
        self._writer_thread = threading.Thread(target=self._writer.run, name="mcp-flightrecorder-writer", daemon=True)
        self._writer_thread.start()
        self._ready.wait()
        if self._init_error is not None:
            self._writer_thread.join()
            if isinstance(self._init_error, FileExistsError):
                emit(_logger, logging.DEBUG, "recorder_initialization_refused run_id=%s reason=destination_exists",
                     self._run_id)
                raise self._init_error
            emit(_logger, logging.ERROR, "recorder_initialization_failed run_id=%s reason=storage_error",
                 self._run_id)
            if self._init_cleanup == "removed":
                detail = "failed initialization files were removed"
            elif self._init_cleanup == "retained":
                detail = "initialization files were retained; inspect them and choose a fresh path"
            else:
                detail = "no initialization files were removed; correct the storage error and choose a fresh path"
            raise RecorderError("Unable to initialize the recording; " + detail) from self._init_error
        emit(_logger, logging.INFO, "recorder_started run_id=%s capture=%s comparisons=%s",
             self._run_id, capture, comparisons)

    @staticmethod
    def _new_id(prefix):
        return prefix + "_" + uuid.uuid4().hex

    @staticmethod
    def _utc_now():
        return datetime.now(timezone.utc).isoformat(timespec="microseconds")

    @staticmethod
    def _mono_ms():
        return time.monotonic_ns() / 1_000_000

    def _initial_run(self):
        return {"run_id": self._run_id, "started_at": self._started_at,
                "environment_json": json_text(self._environment), "policy_json": json_text(self._policy),
                "coverage_json": json_text(self._coverage)}

    def _initialization_failed(self, error):
        self._init_error, self._state = error, "failed"

    def _in_creator_process(self):
        # Never acquire an inherited lock or invoke a logging handler to reject use.
        return os.getpid() == self._creator_pid

    def _check_process(self):
        if not self._in_creator_process():
            raise CaptureRejected("wrong_process")

    def _foreign_scope(self):
        return CaptureScope(self._run_id, None, None, True, b"")

    def _scope_seal(self, turn_id, dispatch_event_id, disabled):
        return hmac.digest(self._key, encode([self._run_id, turn_id, dispatch_event_id, disabled]), "sha256")

    def _issue_scope(self, turn_id=None, dispatch_event_id=None, *, disabled=False):
        return CaptureScope(self._run_id, turn_id, dispatch_event_id, disabled,
                            self._scope_seal(turn_id, dispatch_event_id, disabled))

    def _current_scope_frame(self):
        original = frame = self._scope.get()
        while frame is not None and frame.foreign_closed:
            frame = frame.previous
        if frame is not original:
            self._scope.set(frame)
        return frame

    def capture_scope(self):
        if not self._in_creator_process():
            return self._foreign_scope()
        frame = self._current_scope_frame()
        return self._issue_scope() if frame is None else frame.scope

    def _validate_scope(self, scope):
        if (type(scope) is not CaptureScope or scope.run_id != self._run_id or scope._disabled
                or type(scope._seal) is not bytes):
            raise CaptureRejected("invalid_scope")
        expected = self._scope_seal(scope.turn_id, scope.dispatch_event_id, scope._disabled)
        if not hmac.compare_digest(scope._seal, expected):
            raise CaptureRejected("invalid_scope")

    def _require_boundary(self, boundary):
        if self._coverage["producers"][boundary] is None:
            raise CaptureRejected("unavailable_boundary")

    def turn(self, prompt, *, conversation_id=None, conditions=None):
        return _ScopeContext(self, "turn", {"prompt": prompt, "conversation_id": conversation_id, "conditions": conditions})

    def dispatch(self, *, action, name, arguments, decision_event_ids=(), complete=True):
        return _ScopeContext(self, "dispatch", {"action": action, "name": name, "arguments": arguments,
                                                "decision_event_ids": decision_event_ids, "complete": complete})

    def observe(self, kind, *, scope, payload=MISSING, facts=None):
        return self._capture(lambda preparation: preparation.observe(kind, scope, payload, facts), scope=scope)

    def start_operation(self, *, scope, client_id, method, arguments, server_key=None,
                        name=None, decision_id=None, dispatch_event_id=None, link_basis="none", complete=True, retention=None):
        return self._capture(lambda preparation: preparation.start(
            scope, client_id=client_id, method=method, arguments=arguments, server_key=server_key,
            name=name, decision_id=decision_id, dispatch_event_id=dispatch_event_id,
            link_basis=link_basis, complete=complete, retention=retention), scope=scope)

    def finish_operation(self, ticket, *, status, result=MISSING, resolution="unknown",
                         input_rounds=None, error=None, text_paths=None, complete=True, retention=None):
        if not self._in_creator_process():
            return CaptureResult("rejected", "wrong_process")
        def prepare(preparation):
            self._validate_ticket(ticket)
            return preparation.finish(ticket, status=status, result=result, resolution=resolution,
                                             input_rounds=input_rounds, error=error, text_paths=text_paths,
                                             complete=complete, retention=retention)
        turn_id = None
        if type(ticket) is Ticket and ticket.owner is self._ticket_owner:
            turn_id = json.loads(ticket.encoded)["turn_id"]
        return self._capture(prepare, turn_id=turn_id)

    def _turn_preparation(self, prompt, conversation_id, conditions):
        self._require_boundary("host")
        validate_control(conversation_id, "conversation_id", nullable=True)
        prompt, conditions = freeze(prompt), freeze({} if conditions is None else conditions)
        if conditions is MISSING:
            raise InvalidInput("conditions must be declared JSON")
        condition_text = json.dumps(conditions, sort_keys=True, ensure_ascii=False,
                                    separators=(",", ":"), allow_nan=False)
        condition_key = hashlib.sha256(condition_text.encode("utf-8")).hexdigest()
        turn_id = self._new_id("t")
        artifact = self._artifacts.artifact(prompt)
        row = {"run_id": self._run_id, "turn_id": turn_id, "conversation_id": conversation_id,
               "started_at": self._utc_now(), "prompt_ref": artifact["artifact_id"],
               "conditions_json": json_text(conditions), "condition_key": condition_key,
               "coverage_json": json_text(self._coverage)}
        return Prepared(self, "turn.start", {"run_id": self._run_id, "turn": row, "artifacts": [artifact]},
                        CaptureResult("accepted", turn_id=turn_id))

    def _enter_turn(self, prompt, conversation_id, conditions):
        return self._capture(lambda preparation: self._turn_preparation(prompt, conversation_id, conditions), is_event=False)

    def _exit_turn(self, scope, outcome):
        def prepare(preparation):
            end = {"run_id": self._run_id, "turn_id": scope.turn_id,
                   "ended_at": self._utc_now(), "host_outcome": outcome}
            return Prepared(self, "turn.end", {"run_id": self._run_id, "turn_end": end},
                            CaptureResult("accepted", turn_id=scope.turn_id))
        return self._capture(prepare, scope=scope, is_event=False, turn_update=True)

    def _stopped_reason(self):
        if self._state == "accepting":
            return None
        return "writer_failed" if self._state == "failed" else (self._stop_reason or "closed")

    def _capture(self, prepare, *, scope=_NO_SCOPE, is_event=True, turn_id=None, turn_update=False):
        if not self._in_creator_process():
            return CaptureResult("rejected", "wrong_process")
        if type(scope) is CaptureScope and scope.run_id == self._run_id:
            turn_id = scope.turn_id
        try:
            with self._condition:
                stopped = self._stopped_reason()
            if stopped:
                result = self._reject(stopped, turn_id, is_event, turn_update)
            else:
                if scope is not _NO_SCOPE:
                    self._validate_scope(scope)
                with _IdentityReadScope(self) as identities:
                    preparation = Preparation(self, lookup=identities.lookup)
                    prepared = prepare(preparation)
                    bundle = prepared.bundle()
                result = self._admit(prepared, bundle, turn_update=turn_update)
        except CaptureRejected as error:
            result = self._reject(error.reason, turn_id, is_event, turn_update)
        except (ValueError, TypeError, OverflowError, RecursionError):
            result = self._reject("invalid_input", turn_id, is_event, turn_update)
        except Exception:
            if not self._in_creator_process():
                return CaptureResult("rejected", "wrong_process")
            with self._condition:
                self._state, self._stop_reason = "failed", "writer_failed"
                self._condition.notify_all()
            result = self._reject("writer_failed", turn_id, is_event, turn_update)
            emit(_logger, logging.ERROR, "capture_failed run_id=%s reason=internal_error", self._run_id)
        self._log_capture_result(result)
        return result

    def _log_capture_result(self, result):
        """Claim each fixed loss category once; run handlers outside the lock."""
        if not self._in_creator_process():
            return
        reason = result.reason or ("capacity" if "capacity" in result.losses else None)
        # Fatal transitions have their own error record; policy omissions are normal.
        if reason is None or reason in ("writer_failed", "prerequisite_lost"):
            return
        reason = reason if reason in REASONS else "other"
        with self._condition:
            if reason in self._logged_capture_reasons:
                return
            self._logged_capture_reasons.add(reason)
            dropped = self._dropped_events
        level = logging.DEBUG if reason == "closed" else logging.WARNING
        emit(_logger, level, "capture_loss run_id=%s reason=%s status=%s dropped_events=%d",
             self._run_id, reason, result.status, dropped)

    def _validate_ticket(self, ticket):
        self._check_process()
        with self._condition:
            if type(ticket) is not Ticket or ticket.owner is not self._ticket_owner:
                raise InvalidInput("an operation requires its recorder-issued ticket")
            if ticket.phase in ("queued", "done"):
                raise CaptureRejected("duplicate_completion")
            if ticket.phase != "active" or not ticket.reserved:
                raise CaptureRejected("prerequisite_lost")

    def _validate_reservation(self, ticket, preparation):
        """Bound the worst supported terminal control envelope before start."""
        worst = preparation.finish(
            ticket, status="transport_error", result="x", resolution="client_cache",
            input_rounds=2**63-1, error={"origin": "\x00" * 64, "type": "\x00" * 64, "code": -(2**63)},
            text_paths=[""], complete=True, retention=None)
        metadata_bound = max(worst.bundle(omission).metadata_bytes for omission in range(3)) + 64
        if metadata_bound + len(ticket.encoded) > RESERVATION_BYTES:
            raise InvalidInput("operation metadata cannot fit its terminal reservation")

    def _has_capacity(self, bundle):
        ordinary = self._ordinary_bytes + bundle.ordinary_charge <= self._limits["ordinary_bytes"]
        if not bundle.terminal:
            ordinary = ordinary and self._ordinary_slots < self._limits["ordinary_items"]
        if bundle.ticket is not None and not bundle.terminal:
            return ordinary and self._completion_slots < self._limits["completion_items"] and (
                self._completion_bytes + RESERVATION_BYTES <= self._limits["completion_bytes"])
        return ordinary

    def _admit(self, prepared, bundle, *, turn_update=False):
        self._check_process()
        for omission in range(3):
            rejection_turn = None if bundle.kind == "turn.start" else bundle.turn_id
            with self._condition:
                reason = self._stopped_reason()
                if reason:
                    return self._reject(reason, rejection_turn, bundle.is_event, turn_update)
                if bundle.terminal:
                    self._validate_ticket(bundle.ticket)
                    if bundle.metadata_bytes + len(bundle.ticket.encoded) > RESERVATION_BYTES:
                        return self._reject("invalid_input", bundle.turn_id, True)
                if self._has_capacity(bundle):
                    return self._publish(bundle)
            if omission < 2:
                bundle = prepared.bundle(omission + 1)
        return self._reject("capacity", rejection_turn, bundle.is_event, turn_update)

    def _publish(self, bundle):
        """The admission lock owns charge, reservation, FIFO and identities."""
        bundle = replace(bundle, admitted_at=time.monotonic())
        self._ordinary_bytes += bundle.ordinary_charge
        self._ordinary_slots += int(not bundle.terminal)
        ticket = bundle.ticket
        if ticket is not None and not bundle.terminal:
            ticket.phase, ticket.reserved = "active", True
            self._tickets[ticket.operation_id] = ticket
            self._completion_slots += 1
            self._completion_bytes += RESERVATION_BYTES
        elif ticket is not None:
            ticket.phase = "queued"
        for identity in bundle.identities:
            self._pending[(identity.category, identity.key)] = identity
        self._queue.append(bundle)
        self._note_reasons((*bundle.losses, *(('partial',) if bundle.partial else ())))
        if bundle.partial:
            self._mark_loss(bundle.turn_id)
        self._condition.notify_all()
        return bundle.receipt

    def _note_reasons(self, reasons):
        """Caller holds the admission lock; each bounded category counts once."""
        for reason in {reason if reason in REASONS else "other" for reason in reasons}:
            self._reason_counts[reason] += 1

    def _mark_loss(self, turn_id):
        """A bounded set marks local losses; overflow conservatively scopes to run."""
        self._capture_loss = True
        if turn_id is None:
            self._global_loss = True
        elif not self._global_loss:
            self._loss_turns.add(turn_id)
            if len(self._loss_turns) > self._limits["committed_cache_items"]:
                self._global_loss = True
                self._loss_turns.clear()

    def _reject(self, reason, turn_id=None, is_event=True, turn_update=False):
        if not self._in_creator_process():
            return CaptureResult("rejected", "wrong_process")
        with self._condition:
            self._dropped_events += int(is_event)
            self._note_reasons((reason, *(('turn_update_lost',) if turn_update else ())))
            self._mark_loss(turn_id)
            return CaptureResult("closed" if reason == "closed" else "rejected", reason, turn_id=turn_id)

    def _cache_identity(self, identity):
        """Caller holds the admission lock; cache eviction cannot erase evidence."""
        key = (identity.category, identity.key)
        self._committed[key] = identity.encoded
        self._committed.move_to_end(key)
        while len(self._committed) > self._limits["committed_cache_items"]:
            self._committed.popitem(last=False)

    def _lookup(self, category, identity, *, fallback=None):
        self._check_process()
        key = (category, identity)
        with self._condition:
            pending = self._pending.get(key)
            if pending is not None:
                return json.loads(pending.encoded)
            committed = self._committed.get(key)
            if committed is not None:
                self._committed.move_to_end(key)
                return json.loads(committed)
        return (self._read_identity if fallback is None else fallback)(category, identity)

    def _read_identity(self, category, identity):
        """Fallback creates/uses/closes its read connection on this caller thread."""
        self._check_process()
        with closing(_storage.connect(self._path, readonly=True,
                                      timeout=self._limits["busy_timeout_ms"] / 1000)) as reader:
            reader.row_factory = sqlite3.Row
            return self._lookup_connection(reader, category, identity)

    def _lookup_connection(self, reader, category, identity):
        """Read one current statement; only durable positive rows enter memory."""
        self._check_process()
        where = {"event": "event_id=?", "decision": "decision_id=? AND kind='model.tool_call'",
                 "model": "model_call_id=? AND kind='model.request'"}[category]
        with closing(reader.execute("SELECT * FROM observed_events WHERE run_id=? AND " + where,
                                    (self._run_id, identity))) as cursor:
            row = cursor.fetchone()
        value = None if row is None else event_metadata(dict(row))
        if value is not None:
            from ._types import Identity
            with self._condition:
                self._cache_identity(Identity(category, identity, encode(value)))
        return value

    def _persist_bundle(self, connection, bundle):
        _storage.persist_bundle(connection, bundle, self._writer.sequence)

    def _commit_batch(self, connection):
        connection.execute("COMMIT")

    def _release_ticket(self, ticket, phase):
        """Caller holds the admission lock; a reservation has one release owner."""
        if ticket is not None and ticket.reserved:
            ticket.reserved, ticket.phase = False, phase
            self._tickets.pop(ticket.operation_id, None)
            self._completion_slots -= 1
            self._completion_bytes -= RESERVATION_BYTES

    def _release_bundle(self, bundle, *, discarded=False):
        self._ordinary_bytes -= bundle.ordinary_charge
        self._ordinary_slots -= int(not bundle.terminal)
        if bundle.terminal or discarded:
            self._release_ticket(bundle.ticket, "discarded" if discarded else "done")
        if min(self._ordinary_bytes, self._ordinary_slots, self._completion_bytes, self._completion_slots) < 0:
            raise RuntimeError("capture capacity released more than once")

    def _commit_bundles(self, bundles):
        with self._condition:
            for bundle in bundles:
                for identity in bundle.identities:
                    self._pending.pop((identity.category, identity.key), None)
                    self._cache_identity(identity)
                self._release_bundle(bundle)
            self._condition.notify_all()

    def _discard_bundle(self, bundle, reason):
        with self._condition:
            for identity in bundle.identities:
                self._pending.pop((identity.category, identity.key), None)
            self._release_bundle(bundle, discarded=True)
            self._dropped_events += int(bundle.is_event)
            self._note_reasons((reason, *(('turn_update_lost',) if bundle.kind == "turn.end" else ())))
            self._mark_loss(None if bundle.kind == "turn.start" else bundle.turn_id)

    def _prerequisite_failure(self, suffix):
        with self._condition:
            self._state, self._stop_reason = "stopped", "prerequisite_lost"
            discarded = [*suffix, *self._queue]
            self._queue.clear()
            for bundle in discarded:
                self._discard_bundle(bundle, "prerequisite_lost")
            self._condition.notify_all()
        emit(_logger, logging.ERROR, "capture_stopped run_id=%s reason=prerequisite_lost discarded_bundles=%d",
             self._run_id, len(discarded))

    def _writer_failure(self, current):
        with self._condition:
            self._state, self._stop_reason = "failed", "writer_failed"
            self._writer_errors += 1
            self._capture_loss = True
            discarded = [*current, *self._queue]
            self._queue.clear()
            for bundle in discarded:
                self._discard_bundle(bundle, "writer_failed")
            for ticket in list(self._tickets.values()):
                self._release_ticket(ticket, "discarded")
            self._condition.notify_all()

    def health(self):
        if not self._in_creator_process():
            return self._health_snapshot("wrong_process")
        with self._condition:
            return self._health_snapshot(self._state)

    def _health_snapshot(self, state):
        reasons = dict(self._reason_counts)
        if state == "wrong_process":
            # Other fields are inherited observations, not live parent health.
            reasons["wrong_process"] = 1
        return Health(state, self._dropped_events, self._writer_errors,
                      MappingProxyType(reasons), len(self._tickets),
                      Capacity(self._ordinary_bytes, self._limits["ordinary_bytes"], self._ordinary_slots,
                               self._limits["ordinary_items"]),
                      Capacity(self._completion_bytes, self._limits["completion_bytes"], self._completion_slots,
                               self._limits["completion_items"]))

    def _closure_snapshot(self):
        with self._condition:
            return {"dropped_events": self._dropped_events, "writer_errors": self._writer_errors,
                    "reason_counts": dict(self._reason_counts), "capture_loss": self._capture_loss,
                    "global_loss": self._global_loss, "loss_turns": tuple(self._loss_turns)}

    def _policy_with_health(self, snapshot):
        return {**self._policy, "health": {"dropped_events": snapshot["dropped_events"],
                "writer_errors": snapshot["writer_errors"], "reason_counts": snapshot["reason_counts"]}}

    def _closure_committed(self, close_state):
        with self._condition:
            self._run_close_state = close_state
            for ticket in list(self._tickets.values()):
                self._release_ticket(ticket, "discarded")

    def _finalization_finished(self, reason):
        with self._condition:
            self._final_reason, self._portable = reason, reason is None
            if reason == "storage_error":
                self._writer_errors += 1
                self._note_reasons(("writer_failed",))
            elif reason is not None:
                self._note_reasons(("finalize_timeout" if reason == "timeout" else "finalize_busy",))
            self._condition.notify_all()
        if reason is None:
            emit(_logger, logging.DEBUG, "storage_finalized run_id=%s", self._run_id)
        else:
            level = logging.ERROR if reason == "storage_error" else logging.WARNING
            emit(_logger, level, "storage_finalization_pending run_id=%s reason=%s", self._run_id, reason)

    def _start_shutdown(self, deadline):
        with self._condition:
            if self._state == "accepting":
                self._state, self._stop_reason = "stopped", "closed"
            if self._shutdown_deadline is None:
                self._shutdown_deadline = deadline
            self._condition.notify_all()

    def _close_result(self, joined, reason=None):
        if not self._in_creator_process():
            return CloseResult(False, None, False, "wrong_process")
        with self._condition:
            retry_alive = self._retry_thread is not None and self._retry_thread.is_alive()
            current_reason = "writer_running" if retry_alive else self._final_reason
            return CloseResult(joined, self._run_close_state, joined and not retry_alive and self._portable,
                               reason if reason is not None else current_reason)

    def _log_close_result(self, result):
        """Log changed public outcomes, after all shutdown locks are released."""
        if not self._in_creator_process():
            return CloseResult(False, None, False, "wrong_process")
        with self._condition:
            if result == self._last_close_log:
                return result
            self._last_close_log = result
        level = logging.INFO if result.portable and result.run_close_state == "clean" else logging.WARNING
        if result.reason == "storage_error":
            level = logging.ERROR
        emit(_logger, level, "recorder_close run_id=%s writer_stopped=%s portable=%s state=%s reason=%s",
             self._run_id, result.writer_stopped, result.portable, result.run_close_state, result.reason)
        return result

    @staticmethod
    def _wait_budget(deadline):
        return max(0.0, min(threading.TIMEOUT_MAX, deadline - time.monotonic()))

    def _join_writer(self, deadline):
        """Join within an existing caller budget without changing diagnostics."""
        if not self._in_creator_process():
            return False
        self._writer_thread.join(self._wait_budget(deadline))
        return not self._writer_thread.is_alive()

    def _close_wait_result(self, joined):
        """Account for one close wait after its owner join or caller timeout."""
        if not self._in_creator_process():
            return CloseResult(False, None, False, "wrong_process")
        if not joined:
            with self._condition:
                self._note_reasons(("finalize_timeout",))
            return self._close_result(False, "timeout")
        return self._close_result(True)

    def close(self, timeout=5.0):
        if not self._in_creator_process():
            return CloseResult(False, None, False, "wrong_process")
        timeout = timeout_seconds(timeout)
        deadline = time.monotonic() + timeout
        self._start_shutdown(deadline)
        return self._log_close_result(self._close_wait_result(self._join_writer(deadline)))

    async def aclose(self, timeout=5.0):
        if not self._in_creator_process():
            return CloseResult(False, None, False, "wrong_process")
        timeout = timeout_seconds(timeout)
        deadline = time.monotonic() + timeout
        self._start_shutdown(deadline)
        try:
            joined = await asyncio.wait_for(asyncio.to_thread(self._join_writer, deadline),
                                            timeout=self._wait_budget(deadline))
        except asyncio.TimeoutError:
            # The caller's budget expired even if its join completes later.
            joined = False
        return self._log_close_result(self._close_wait_result(joined))

    def finalize(self, timeout=5.0):
        if not self._in_creator_process():
            return CloseResult(False, None, False, "wrong_process")
        timeout = timeout_seconds(timeout)
        deadline = time.monotonic() + timeout
        return self._log_close_result(self._finalize_wait(deadline))

    def _finalize_wait(self, deadline):
        """Share the retry owner without invoking application handlers under its lock."""
        if not self._in_creator_process():
            return CloseResult(False, None, False, "wrong_process")
        if self._writer_thread.is_alive():
            return self._close_result(False, "writer_running")
        self._writer_thread.join()
        if not self._retry_lock.acquire(timeout=self._wait_budget(deadline)):
            return self._close_result(True, "timeout")
        try:
            if self._portable and (self._retry_thread is None or not self._retry_thread.is_alive()):
                return self._close_result(True)
            if self._retry_thread is None or not self._retry_thread.is_alive():
                self._retry_thread = threading.Thread(target=_storage.retry_finalization, args=(self, deadline),
                                                      name="mcp-flightrecorder-finalize", daemon=True)
                self._retry_thread.start()
            thread = self._retry_thread
        finally:
            self._retry_lock.release()
        thread.join(self._wait_budget(deadline))
        if thread.is_alive():
            with self._condition:
                self._note_reasons(("finalize_timeout",))
            return self._close_result(True, "timeout")
        return self._close_result(True)
