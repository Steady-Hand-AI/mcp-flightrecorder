"""Optional observation of the supported asynchronous Chat Completions method.

The host retains its native client, retry policy, results, and stream lifecycle.
Only submitted ordinary JSON and actually consumed responses are observed.
"""
import asyncio
from functools import wraps
import inspect

try:
    from openai import AsyncStream
except ImportError as error:
    raise ImportError("OpenAI observation requires mcp-flightrecorder[openai-chat]") from error

from ._common import capture_scope, claim_evidence, validate_label, validate_retention_policy
from ._model_capture import ModelCall, ModelDecision, ModelEvidence
from ._model_stream import ObservedStream


__all__ = ["ModelEvidence", "ModelDecision", "wrap_model"]


def wrap_model(recorder, native_create, *, instance_id, retention_policy=None):
    """Decorate one bound native method; invocation itself schedules no work."""
    validate_label(instance_id, "instance_id")
    validate_retention_policy(retention_policy)
    signature = inspect.signature(native_create)

    @wraps(native_create)
    def create(*, evidence=None, annotations=None, insertions=(), **sdk_kwargs):
        signature.bind(**sdk_kwargs)
        evidence = claim_evidence(evidence, ModelEvidence)
        scope = capture_scope(evidence, recorder)

        async def execute():
            call = ModelCall(recorder, scope, instance_id, evidence,
                             retention_policy=retention_policy)
            call.request(sdk_kwargs, annotations, insertions)
            try:
                native = await native_create(**sdk_kwargs)
            except asyncio.CancelledError:
                call.terminal(state="cancelled")
                raise
            except BaseException:
                call.terminal(state="error")
                raise
            if isinstance(native, AsyncStream):
                return ObservedStream(native, call)
            call.normal(native)
            return native

        return execute()

    return create
