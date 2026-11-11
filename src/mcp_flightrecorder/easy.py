"""Explicit host integration without application-owned capture bookkeeping.

Only ordinary JSON and the named native SDK adapters are supported. Importing
this module starts no writer and imports no optional SDK.
"""
from contextvars import ContextVar
from dataclasses import dataclass
import inspect
import json
import os
import weakref

from . import MISSING, Recorder
from ._recording_path import recording_path
from .integrations._common import (EvidenceBase, copy_json, policy_retention,
                                   validate_label, validate_retention_policy)

__all__ = ("AgentRecorder", "IntegrationError")
_ACTIVE = ContextVar("mfr_easy_attempt", default=None)
_TURN = ContextVar("mfr_easy_turn", default=None)


class IntegrationError(ValueError):
    """Actionable facade misuse, raised only by explicit strict mode."""
    def __init__(self, reason, hint):
        self.reason, self.hint = reason, hint
        super().__init__(reason + ": " + hint)


def _accepted(receipt):
    return receipt is not None and receipt.status == "accepted"


def _text_paths(content):
    if type(content) is str:
        return [""]
    if type(content) is list and all(type(block) is dict and block.get("type") == "text"
                                   and type(block.get("text")) is str for block in content):
        return ["/" + str(i) + "/text" for i in range(len(content))]
    return None


def _cleanup(context, variable, token, owner, exc):
    """Always release local context, without replacing an existing host error."""
    try:
        if context is not None:
            context.__exit__(*exc)
    except Exception:
        owner._note("cleanup_failed", "Keep context entry and exit in the same task; inspect recorder health.",
                    misuse=exc[0] is None)
    finally:
        if token is not None:
            try:
                variable.reset(token)
            except (ValueError, RuntimeError):
                owner._note("context_cleanup_failed", "Enter and exit each context once in the same task.",
                            misuse=exc[0] is None)


class AgentRecorder:
    """An inert, process-local recorder configuration; call start in the worker.

    ``None`` disables recording. ``all_model_calls_recorded`` is a host promise,
    never inferred. Ledger limits apply per caller-owned conversation.
    """
    def __init__(self, path=None, *, capture="metadata", comparisons=True,
                 all_model_calls_recorded=False, strict=False, binding_limit=1024,
                 prepared_byte_limit=1048576, key_byte_limit=256, limits=None,
                 environment=None, retention_policy=None):
        for name, value in (("strict", strict),
                            ("all_model_calls_recorded", all_model_calls_recorded)):
            if type(value) is not bool:
                raise ValueError(name + " must be boolean")
        for name, value in (("binding_limit", binding_limit),
                            ("prepared_byte_limit", prepared_byte_limit),
                            ("key_byte_limit", key_byte_limit)):
            if type(value) is not int or value < 1:
                raise ValueError(name + " must be a positive integer")
        self.path, self.strict = path, strict
        self._destination = path
        self._retention_policy = validate_retention_policy(retention_policy)
        self.binding_limit, self.prepared_byte_limit = binding_limit, prepared_byte_limit
        self.key_byte_limit = key_byte_limit
        self._options = dict(capture=capture, comparisons=comparisons, limits=limits,
                             environment=environment,
                             coverage={"producers": {"host": "easy.host", "model": "easy.model",
                                                     "mcp": "easy.mcp"},
                                       "model_complete": all_model_calls_recorded})
        self._recorder = None
        self._started = False
        self._generation = 0
        self._pid = os.getpid()
        self._conversations = weakref.WeakSet()
        self._notes = {}

    @property
    def diagnostics(self):
        """Bounded local integration notes; these are not persisted run claims."""
        return tuple(dict(reason=reason, **entry) for reason, entry in self._notes.items())

    def _note(self, reason, hint, *, misuse=False):
        # Reasons are internal constants; neither prompts nor native errors enter this map.
        item = self._notes.setdefault(reason, {"count": 0, "hint": hint})
        item["count"] = min(item["count"] + 1, 2 ** 63 - 1)
        if misuse and self.strict:
            raise IntegrationError(reason, hint)

    def _live(self):
        return self._started and os.getpid() == self._pid

    def start(self, path=MISSING):
        """Start explicitly; directory destinations get a fresh file each time.

        ``path`` preserves explicit file values; directory mode exposes the
        successfully initialized generation's absolute file.
        An explicit replacement destination (including disabled ``None``) is kept
        for later starts; closing never replaces it with a generated filename.
        """
        if self._started:
            self._note("already_started", "Close the recorder before starting another run.", misuse=True)
            return self
        if path is not MISSING:
            self._destination = path
        self._pid = os.getpid()
        self._generation += 1
        destination = None if self._destination is None else recording_path(self._destination)
        recorder = None if destination is None else Recorder(destination, **self._options)
        self._recorder, self.path = recorder, destination
        self._started = True
        return self

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc, traceback):
        try:
            self.close()
        except Exception:
            if exc_type is None:
                raise
            self._note("cleanup_failed", "The original host error is preserved; inspect recorder health separately.")
        return False

    def conversation(self, name, *, is_tool_message=None, key=None):
        """Own a bounded history ledger. Overloaded roles require a host predicate."""
        conv = Conversation(self, name, is_tool_message, key)
        self._conversations.add(conv)
        return conv

    def mcp(self, native, *, name="tools", kind="client"):
        """Decorate only list_tools/call_tool; the host owns the native lifecycle."""
        if kind not in ("client", "session"):
            raise ValueError("kind must be client or session")
        validate_label(name, "name")
        return _MCP(self, native, name, kind)

    def _observe(self, kind, scope, payload=MISSING, **facts):
        if not self._live() or self._recorder is None or scope is None:
            return None
        if self._retention_policy is not None and payload is not MISSING:
            evidence = EvidenceBase()
            facts["retention"] = policy_retention(evidence, self._retention_policy,
                                                   kind, payload, {})
            if evidence.losses:
                self._note("retention_unavailable", "Correct the retention callback; failed policy capture omits content.")
        receipt = self._recorder.observe(kind, scope=scope, payload=payload, facts=facts)
        if not _accepted(receipt):
            # A rejected turn intentionally installs a disabled scope. Its
            # downstream invalid_scope receipts are capture loss, not host misuse.
            misuse = receipt.reason in ("invalid_input", "unavailable_boundary") or (
                receipt.reason == "invalid_scope" and not scope._disabled)
            self._note("capture_unavailable", "Inspect recorder health and integration diagnostics; "
                       "a rejected observation cannot identify a selected result.", misuse=misuse)
        return receipt

    def _invalidate(self):
        if os.getpid() != self._pid:
            return
        for conv in tuple(self._conversations):
            conv.close()
        self._conversations.clear()
        self._started = False
        self._generation += 1

    def close(self, *args, **kwargs):
        """Invalidate history, then close only the recorder (never native clients)."""
        self._invalidate()
        return None if self._recorder is None else self._recorder.close(*args, **kwargs)

    async def aclose(self, *args, **kwargs):
        self._invalidate()
        return None if self._recorder is None else await self._recorder.aclose(*args, **kwargs)

    def health(self):
        return None if self._recorder is None else self._recorder.health()

    def finalize(self, timeout=5.0):
        """Retry existing core finalization after its writer has stopped."""
        return None if self._recorder is None else self._recorder.finalize(timeout=timeout)


@dataclass
class _Binding:
    message: object
    decision: object
    result: object
    prepared: object = None
    content: object = MISSING
    byte_count: int = 0
    reason: str | None = None
    local: bool = False


class Conversation:
    """Caller-owned occurrence ledger; keys are local to this conversation/run."""
    def __init__(self, owner, name, predicate, key):
        if type(name) is not str or not name or len(name.encode("utf-8")) > 256:
            raise ValueError("conversation name must be a nonempty string of at most 256 UTF-8 bytes")
        for label, callback in (("is_tool_message", predicate), ("key", key)):
            if callback is not None and (not callable(callback) or inspect.iscoroutinefunction(callback)
                                         or inspect.isasyncgenfunction(callback)):
                raise ValueError(label + " must be a synchronous callable")
        self.owner, self.name, self._predicate, self._key = owner, name, predicate, key
        self._generation = owner._generation
        self._bindings, self._prepared_bytes = {}, 0
        self._closed, self._turn = False, None

    @property
    def binding_count(self):
        return len(self._bindings)

    @property
    def prepared_bytes(self):
        return self._prepared_bytes

    def _valid(self):
        valid = (not self._closed and self.owner._live()
                 and self._generation == self.owner._generation)
        if not valid:
            self.owner._note("conversation_unavailable", "Start in the worker, then create a new "
                             "conversation for this run; replayed answers need new explicit bindings.")
        return valid

    def close(self):
        self._bindings.clear()
        self._prepared_bytes = 0
        self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    def turn(self, prompt):
        return _Turn(self, prompt)

    def model_call(self, payload, *, message_keys=None, create=None, name="model", record_usage=False):
        """Observe manual JSON, or the named native OpenAI create method.

        With ``create=client.chat.completions.create``, await ``call.create()``;
        the existing SDK adapter owns response projection and consumed streaming.
        """
        return _Model(self, payload, message_keys, create, name, record_usage)

    def host_call(self, name, arguments):
        """An explicitly host-initiated operation, without a model decision."""
        return _Choice(self, name, arguments, None, self._current_scope())

    def _current_scope(self):
        turn = _TURN.get()
        if self._valid() and turn is self._turn and turn is not None and not turn.closed:
            return turn.scope
        self.owner._note("turn_required", "Use this conversation's turn context around native work.", misuse=True)
        return None

    def _address(self, message, key):
        if key is MISSING and self._key is not None:
            try:
                key = self._key(message)
            except Exception:
                self.owner._note("binding_key_invalid", "The key callback must return a bounded stable string.", misuse=True)
                return None
            if key is None:
                self.owner._note("binding_key_missing", "The configured key callback could not resolve this message; supply its stable key explicitly.")
                return None
        if key is MISSING or key is None:
            return ("identity", id(message))
        try:
            valid = type(key) is str and bool(key) and len(key.encode("utf-8")) <= self.owner.key_byte_limit
        except UnicodeError:
            valid = False
        if not valid:
            self.owner._note("binding_key_invalid", "Supply a nonempty stable string within key_byte_limit.", misuse=True)
            return None
        return ("key", key)

    def _bind(self, attempt, message, prepared, key):
        if not self._valid():
            return message
        if self.owner._recorder is None:
            return message
        address = self._address(message, key)
        if address is None:
            return message
        reason = None
        decision, result = attempt.choice.receipt, attempt._single
        if attempt._count != 1:
            reason = "attempt_ambiguous"
            self.owner._note(reason, "Use one choice.run() per actual invocation and bind the selected attempt.", misuse=True)
        elif not _accepted(decision) or not _accepted(result):
            reason = "capture_unavailable"
            self.owner._note(reason, "The selected attempt lacks admitted evidence; do not substitute an earlier result.")
        entry = _Binding(message if address[0] == "identity" else None, decision, result,
                         reason=reason, local=attempt.local)
        previous = self._bindings.get(address)
        if previous is not None:
            if previous.reason == "binding_conflict" or (previous.decision, previous.result) != (decision, result):
                self._prepared_bytes -= previous.byte_count
                self._bindings[address] = _Binding(previous.message, None, None, reason="binding_conflict")
                self.owner._note("binding_conflict", "Use a distinct stable key or message object for each selected occurrence.", misuse=True)
            return message
        if len(self._bindings) >= self.owner.binding_limit:
            self.owner._note("binding_limit", "Close completed conversations or increase binding_limit; new answers remain unknown.", misuse=True)
            return message
        if prepared:
            try:
                content = copy_json(message["content"]) if type(message) is dict else MISSING
                paths = _text_paths(content)
                if paths is None:
                    raise ValueError("unsupported content")
                size = len(json.dumps(content, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
            except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
                self.owner._note("prepared_content_unavailable", "Use string content or ordinary text blocks for prepared=True.", misuse=True)
            else:
                if size + self._prepared_bytes > self.owner.prepared_byte_limit:
                    self.owner._note("prepared_limit", "Increase prepared_byte_limit or bind without a prepared snapshot.", misuse=True)
                elif _accepted(result):
                    receipt = self.owner._observe("host.tool_result", attempt.scope, content,
                        sources=[{"event_id": result.event_id, "slot": "payload", "pointer": ""}], text_paths=paths)
                    if _accepted(receipt):
                        entry.prepared, entry.content, entry.byte_count = receipt, content, size
                        self._prepared_bytes += size
        self._bindings[address] = entry
        return message

    def _annotations(self, payload, keys):
        messages = payload.get("messages") if type(payload) is dict else None
        if type(messages) is not list:
            self.owner._note("messages_unavailable", "Pass an ordinary JSON payload with an ordered messages list.", misuse=True)
            return None, []
        if keys is not None and (type(keys) not in (list, tuple) or len(keys) != len(messages)):
            self.owner._note("binding_keys_invalid", "message_keys must have one entry per submitted message.", misuse=True)
            keys = [False] * len(messages)
        valid = self._valid()
        annotations, insertions = [], []
        for index, message in enumerate(messages):
            address = self._address(message, MISSING if keys is None else keys[index])
            entry = self._bindings.get(address) if valid and address is not None else None
            reason = None
            tool_message = type(message) is dict and message.get("role") == "tool"
            if self._predicate is not None:
                try:
                    identified = bool(self._predicate(message))
                    tool_message = tool_message or identified
                except Exception:
                    reason = "tool_message_predicate_failed"
                    self.owner._note(reason, "The host predicate must identify tool answers without raising.", misuse=True)
            annotation = {"bindings": [], "unresolved": [], "complete": True}
            if entry is not None:
                reason = reason or entry.reason
                if reason is None:
                    annotation["bindings"] = [{"decision_event_id": entry.decision.event_id,
                        "result_event_id": None if entry.local else entry.result.event_id, "pointer": "/content",
                        "text_paths": _text_paths(message.get("content")) if type(message) is dict else None}]
                if entry.prepared is not None:
                    insertions.append({"source_event_id": entry.prepared.event_id,
                                       "target_pointer": "/messages/" + str(index) + "/content",
                                       "content": entry.content})
            elif tool_message or (address is not None and address[0] == "key") or address is None:
                reason = reason or "binding_key_missing"
            if reason:
                pointer = "/content" if type(message) is dict and "content" in message else ""
                annotation.update(bindings=None, unresolved=[{"pointer": pointer, "reason": reason}])
            annotations.append(annotation)
        return annotations, insertions


class _Turn:
    def __init__(self, conv, prompt):
        self.conv, self.prompt = conv, prompt
        self.scope = self.context = self.token = None
        self.closed = False

    def __enter__(self):
        conv = self.conv
        if conv._turn is not None:
            conv.owner._note("overlapping_turns", "Use a separate conversation for concurrent turns.", misuse=True)
            self.token = _TURN.set(self)
            return self
        self.token = _TURN.set(self)
        if not conv._valid():
            return self
        conv._turn = self
        rec = conv.owner._recorder
        if rec is not None:
            self.context = rec.turn(self.prompt, conversation_id=conv.name)
            self.context.__enter__()
            self.scope = rec.capture_scope()
        return self

    def __exit__(self, *exc):
        self.closed = True
        try:
            _cleanup(self.context, _TURN, self.token, self.conv.owner, exc)
        finally:
            if self.conv._turn is self:
                self.conv._turn = None
        return False


class _Model:
    def __init__(self, conv, payload, keys, create, name, record_usage):
        validate_label(name, "name")
        if type(record_usage) is not bool:
            raise ValueError("record_usage must be boolean")
        if create is not None and not callable(create):
            raise ValueError("create must be the native callable")
        self.conv, self.payload, self.keys = conv, payload, keys
        self.native, self.name, self.record_usage = create, name, record_usage
        self.scope = self.request = self.evidence = None
        self._done = self._entered = self._created = False
        self._closed = False
        self._turn = None
        self.annotations, self.insertions = None, []

    def __enter__(self):
        self._entered = True
        self._turn = _TURN.get()
        self.scope = self.conv._current_scope()
        if self.native is None:
            self.annotations, self.insertions = self.conv._annotations(self.payload, self.keys)
            facts = {"message_path": "/messages", "messages": self.annotations,
                     "insertions": self.insertions}
            if type(self.payload) is dict and "tools" in self.payload:
                facts.update(field_paths=["/tools"], catalog_path="/tools")
            self.request = self.conv.owner._observe("model.request", self.scope, self.payload, **facts)
        return self

    def __exit__(self, exc_type, exc, traceback):
        self._closed = True
        if self.native is None and not self._done:
            try:
                self._terminal(MISSING, "partial" if exc_type is None else
                               "cancelled" if exc_type.__name__ == "CancelledError" else "error")
            except Exception:
                if exc_type is None:
                    raise
        return False

    def _terminal(self, payload, state):
        self._done = True
        self.conv.owner._observe("model.response", self.scope, payload, state=state,
                                 model_call_id=self.request.model_call_id if _accepted(self.request) else None)

    def done(self, response):
        """Record an exposed manual JSON response, returning it unchanged."""
        if self._done or not self._entered or self.native is not None:
            self.conv.owner._note("model_completion_invalid", "Call done once inside a manual model_call context.", misuse=True)
            return response
        self._terminal(response, "complete")
        return response

    def create(self):
        """Return the lazy native OpenAI call, preserving SDK response/stream ownership."""
        if self.native is None:
            raise IntegrationError("model_create_invalid", "Set create=the native method and call create once inside its context.")
        if self._created or not self._entered or self._closed:
            self.conv.owner._note("model_create_invalid", "Call create once inside its model_call context.", misuse=True)
            self.evidence = None
            return self.native(**self.payload)
        self._created = True
        # Native **kwargs bind their top-level values at factory invocation.
        # Borrow nested objects so mutations remain visible exactly as to the SDK.
        payload = {**self.payload}
        if self.conv.owner._recorder is None or not self.conv.owner._live():
            return self.native(**payload)
        rec, generation = self.conv.owner._recorder, self.conv.owner._generation
        async def execute():
            if not self.conv._valid() or generation != self.conv.owner._generation:
                return await self.native(**payload)
            from .integrations.openai_chat import ModelEvidence, wrap_model
            self.evidence = ModelEvidence()
            annotations, insertions = self.conv._annotations(payload, self.keys)
            scope = self.scope if self._turn is not None and not self._turn.closed else None
            create = wrap_model(_Scoped(rec, scope), self.native, instance_id=self.name,
                                record_usage=self.record_usage,
                                retention_policy=self.conv.owner._retention_policy)
            return await create(evidence=self.evidence, annotations=annotations,
                                insertions=insertions, **payload)
        return execute()

    def choice(self, name, arguments, *, raw=None, choice_index=None, tool_index=None):
        """Declare the host-selected executable decision; never parse model text."""
        receipt = None
        if self.native is not None:
            valid_indices = all(type(index) is int and index >= 0
                                for index in (choice_index, tool_index))
            if not valid_indices:
                self.conv.owner._note("choice_unavailable", "Supply nonnegative integer choice_index and tool_index from the native response.", misuse=True)
            matches = [] if self.evidence is None or not valid_indices else [item for item in self.evidence.decisions
                if item.choice_index == choice_index and item.tool_index == tool_index]
            if len(matches) == 1:
                receipt = matches[0].receipt
            elif valid_indices and self.evidence is not None and _accepted(self.evidence.response) and (
                    not self.evidence.losses and set(self.evidence.response.losses) <= {"body_omitted"}):
                # Only a complete observed selection can disprove this position.
                # Disabled, partial or lost capture is not an execution gate.
                self.conv.owner._note("choice_unavailable", "Supply the actual choice_index and tool_index exposed by the native response.", misuse=True)
            elif valid_indices and self.conv.owner._recorder is not None:
                self.conv.owner._note("capture_unavailable", "Native choice capture is unavailable or incomplete; preserve host selection and inspect capture health.")
        else:
            payload = {"arguments": arguments}
            if raw is not None:
                payload["raw"] = raw
            receipt = self.conv.owner._observe("model.tool_call", self.scope, payload, name=name,
                       model_call_id=self.request.model_call_id if _accepted(self.request) else None)
        return _Choice(self.conv, name, arguments, receipt, self.scope, turn=self._turn)


class _Choice:
    def __init__(self, conv, name, arguments, receipt, scope, *, turn=MISSING):
        self.conv, self.name, self.arguments, self.receipt, self.scope = conv, name, arguments, receipt, scope
        self.turn = _TURN.get() if turn is MISSING else turn
        self._count, self._only = 0, None

    def run(self, *, local=False):
        return _Attempt(self, local)

    def _invoked(self, attempt):
        self._count = min(2, self._count + 1)
        self._only = attempt if self._count == 1 else None

    def answer(self, message, *, prepared=False, key=MISSING):
        """Shorthand for exactly one actual invocation, never the latest success."""
        if self._count == 1 and self._only is not None:
            return self._only.answer(message, prepared=prepared, key=key)
        unknown = _Attempt(self, False)
        return self.conv._bind(unknown, message, prepared, key)


class _Attempt:
    def __init__(self, choice, local):
        self.choice, self.local, self.scope = choice, local, choice.scope
        self._count, self._single = 0, None
        self._token = self._context = None
        self._active = False

    def __enter__(self):
        self._active = True
        self._token = _ACTIVE.set(self)
        conv, rec = self.choice.conv, self.choice.conv.owner._recorder
        current = _TURN.get() is self.choice.turn and self.choice.turn is not None and not self.choice.turn.closed
        if not current:
            self.scope = None
            conv.owner._note("attempt_context_unavailable", "Execute the choice in its originating live turn; late work remains unattributed.")
        if current and conv._valid() and rec is not None and self.scope is not None:
            self._context = rec.dispatch(action="local" if self.local else "execute", name=self.choice.name,
                arguments=self.choice.arguments,
                decision_event_ids=[self.choice.receipt.event_id] if _accepted(self.choice.receipt) else [])
            self._context.__enter__()
            self.scope = rec.capture_scope()
        return self

    def __exit__(self, *exc):
        self._active = False
        _cleanup(self._context, _ACTIVE, self._token, self.choice.conv.owner, exc)
        return False

    def _invoked(self):
        self._count = min(2, self._count + 1)
        self._single = None
        self.choice._invoked(self)

    def _finished(self, receipt):
        if self._count == 1:
            self._single = receipt

    def call(self, function, *args, **kwargs):
        """Invoke one explicitly local synchronous function without changing its value/error."""
        if not self.local or not self._active:
            self.choice.conv.owner._note("local_attempt_required", "Use choice.run(local=True) around call/acall.", misuse=True)
            self._invoked()
            return function(*args, **kwargs)
        self._invoked()
        result = function(*args, **kwargs)
        self._local_result(result)
        return result

    async def acall(self, function, *args, **kwargs):
        if not self.local or not self._active:
            self.choice.conv.owner._note("local_attempt_required", "Use choice.run(local=True) around call/acall.", misuse=True)
            self._invoked()
            return await function(*args, **kwargs)
        self._invoked()
        result = await function(*args, **kwargs)
        self._local_result(result)
        return result

    def _local_result(self, result):
        # A host observation is not an MCP terminal and cannot claim an MCP answer comparison.
        receipt = self.choice.conv.owner._observe("host.tool_result", self.scope, result, sources=[])
        self._finished(receipt)

    def answer(self, message, *, prepared=False, key=MISSING):
        """Bind this attempt only; return the exact caller message without mutation."""
        return self.choice.conv._bind(self, message, prepared, key)


class _Scoped:
    """A fixed recorder scope for existing lazy SDK adapters."""
    def __init__(self, recorder, scope):
        self._recorder, self._scope = recorder, scope

    def capture_scope(self):
        return self._scope

    def __getattr__(self, name):
        return getattr(self._recorder, name)


class _MCP:
    def __init__(self, owner, native, name, kind):
        self.owner, self.native, self.name, self.kind = owner, native, name, kind

    def _call(self, method, args, kwargs):
        owner, attempt = self.owner, _ACTIVE.get()
        rec, generation = owner._recorder, owner._generation
        scope = rec.capture_scope() if rec is not None and owner._live() else None
        selected = attempt is not None and attempt.choice.conv.owner is owner and attempt._active
        async def execute():
            active = (selected and attempt._active and attempt.scope is not None
                      and attempt.choice.conv._valid() and generation == owner._generation)
            if method == "call_tool" and selected:
                attempt._invoked()
            if rec is None or not owner._live() or generation != owner._generation:
                return await getattr(self.native, method)(*args, **kwargs)
            from .integrations._mcp_capture import OperationEvidence
            if self.kind == "session":
                from .integrations.mcp_session import wrap_session as wrap
            else:
                from .integrations.mcp_client import wrap_mcp as wrap
            evidence = OperationEvidence()
            observed_scope = attempt.scope if active else scope if method == "list_tools" else None
            observer = wrap(_Scoped(rec, observed_scope),
                            self.native, client_id=self.name, retention_policy=owner._retention_policy)
            options = dict(kwargs, evidence=evidence)
            if method == "call_tool":
                options["decision"] = attempt.choice.receipt if active and _accepted(attempt.choice.receipt) else None
            try:
                return await getattr(observer, method)(*args, **options)
            finally:
                if method == "call_tool" and selected:
                    attempt._finished(evidence.terminal if active else None)
                if evidence.losses or not _accepted(evidence.terminal):
                    owner._note("capture_unavailable", "Native work completed or raised independently; inspect health for capture loss.")
        return execute()

    def call_tool(self, *args, **kwargs):
        return self._call("call_tool", args, kwargs)

    def list_tools(self, *args, **kwargs):
        return self._call("list_tools", args, kwargs)
