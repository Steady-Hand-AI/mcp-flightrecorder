"""Observe submitted Ollama-provider AnyLLM calls and consumed normalized output.

This boundary does not observe wire identities or emulate the SDK's request
conversion. The host owns transport, iteration, retries and provider cleanup.
"""
import asyncio
from collections.abc import AsyncIterator
from functools import wraps
import inspect

try:
    # Require the named optional SDK at this integration boundary only.
    import any_llm as _any_llm
    from ._model_capture import ModelCall, ModelDecision, ModelEvidence
except ImportError as error:
    raise ImportError("AnyLLM/Ollama observation requires mcp-flightrecorder[anyllm-ollama]") from error

from ._common import capture_scope, claim_evidence, copy_json, validate_label, validate_retention_policy
from ._model_stream import StreamCapture, cleanup_stream, read_chunk


__all__ = ["ModelEvidence", "ModelDecision", "wrap_model"]

_OLLAMA_OPTIONS = frozenset((
    "num_keep", "seed", "num_predict", "top_k", "top_p", "min_p", "typical_p",
    "repeat_last_n", "temperature", "repeat_penalty", "presence_penalty",
    "frequency_penalty", "stop", "num_ctx", "num_batch",
))
_NON_BODY = frozenset(("timeout", "session_label"))


def _supplied_body(bound):
    """Freeze supplied named arguments and only the specified hidden options."""
    body = {}
    for name, value in bound.arguments.items():
        parameter = bound.signature.parameters[name]
        if parameter.kind is inspect.Parameter.VAR_KEYWORD:
            if any(key not in _OLLAMA_OPTIONS for key in value):
                raise ValueError("Unknown Ollama provider options cannot be captured")
            body.update(value)
        elif name not in _NON_BODY:
            body[name] = value
    body = copy_json(body)
    if type(body.get("messages")) is not list:
        raise ValueError("messages must expose an ordered ordinary JSON array")
    return body


class _ObservedIterator:
    """The native AnyLLM iterator protocol, without OpenAI-only conveniences."""
    def __init__(self, native, call):
        self._native = native
        self._capture = StreamCapture(
            call, representation="anyllm_ollama_stream/1",
            tool_terminal_reasons=frozenset(("stop", "tool_calls")),
        )

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await read_chunk(self._native, self._capture)

    async def aclose(self):
        return await cleanup_stream(self._capture, self._native.aclose)


def _is_closeable_iterator(native):
    """Decline unsupported returns without invoking or consuming them."""
    try:
        return isinstance(native, AsyncIterator) and callable(getattr(native, "aclose", None))
    except Exception:
        return False


def wrap_model(recorder, native_acompletion, *, instance_id, retention_policy=None):
    """Decorate the bound provider call while preserving its supplied arguments."""
    validate_label(instance_id, "instance_id")
    validate_retention_policy(retention_policy)
    signature = inspect.signature(native_acompletion)

    @wraps(native_acompletion)
    def create(*native_args, evidence=None, annotations=None, insertions=(), **native_kwargs):
        bound = signature.bind(*native_args, **native_kwargs)
        evidence = claim_evidence(evidence, ModelEvidence)
        scope = capture_scope(evidence, recorder)

        async def execute():
            call = ModelCall(recorder, scope, instance_id, evidence,
                             retention_policy=retention_policy)
            call.request(bound, annotations, insertions, body_builder=_supplied_body)
            try:
                native = await native_acompletion(*native_args, **native_kwargs)
            except asyncio.CancelledError:
                call.terminal(state="cancelled")
                raise
            except BaseException:
                call.terminal(state="error")
                raise
            if _is_closeable_iterator(native):
                return _ObservedIterator(native, call)
            call.normal(native)
            return native

        return execute()

    return create
