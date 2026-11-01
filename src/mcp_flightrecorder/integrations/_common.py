"""Small SDK-independent capture guards shared by the optional observers."""
import asyncio
import inspect
import logging
import math

from .._artifacts import _retained_request, _retention
from .._logging import emit
from .._values import MISSING, freeze, json_text


_logger = logging.getLogger(__name__)


LOSS_CODES = frozenset(("request_unavailable", "projection_failed", "stream_capture_limit",
                        "decision_limit", "partial_tool_call", "invalid_decision", "capture_rejected",
                        "retention_policy_failed"))


def validate_label(value, name, optional=False):
    """Validate one bounded configuration label before any native work."""
    if value is None and optional:
        return value
    if type(value) is not str or not value:
        raise ValueError(name + " must be a nonempty UTF-8 string")
    try:
        encoded = value.encode("utf-8")
    except UnicodeError as error:
        raise ValueError(name + " must be a valid UTF-8 string") from error
    if len(encoded) > 256:
        raise ValueError(name + " must encode to at most 256 UTF-8 bytes")
    return value


class EvidenceBase:
    """One invocation's bounded diagnostics and exclusive receipt ownership."""
    __slots__ = ("_claimed", "losses")

    def __init__(self):
        self._claimed = False
        self.losses = ()

    def add_loss(self, reason):
        if reason not in LOSS_CODES:
            raise ValueError("Unknown integration loss code")
        if reason not in self.losses:
            self.losses += (reason,)
            emit(_logger, logging.WARNING, "integration_capture_loss reason=%s", reason)


def claim_evidence(value, expected_type):
    """Claim a side channel at factory invocation, before creating native work."""
    if value is None:
        value = expected_type()
    if type(value) is not expected_type:
        raise TypeError("evidence must be " + expected_type.__name__ + " or None")
    if value._claimed:
        raise ValueError("evidence belongs to an earlier invocation and cannot be reused")
    value._claimed = True
    return value


def _json_string(value):
    value.encode("utf-8")
    return value


def _json_object(value):
    if any(type(key) is not str for key in value):
        raise ValueError("JSON object keys must be ordinary strings")
    return {_json_string(key): copy_json(item) for key, item in value.items()}


def copy_json(value):
    """Copy ordinary JSON without consuming iterables or calling serializers."""
    kind = type(value)
    if value is None or kind in (bool, int):
        return value
    if kind is str:
        return _json_string(value)
    if kind is float:
        if not math.isfinite(value):
            raise ValueError("JSON numbers must be finite")
        return value
    if kind is list:
        return [copy_json(item) for item in value]
    if kind is dict:
        return _json_object(value)
    raise ValueError("Capture requires ordinary JSON values")


def capture_scope(evidence, recorder):
    """Capture invocation attribution without letting observer faults run a host."""
    try:
        return recorder.capture_scope()
    except Exception:
        evidence.add_loss("capture_rejected")
        return None


def capture_call(evidence, callback, *args, **kwargs):
    """Preserve a core receipt, or contain a capture-only failure as a loss."""
    try:
        receipt = callback(*args, **kwargs)
    except Exception:
        evidence.add_loss("capture_rejected")
        return None
    if getattr(receipt, "status", None) != "accepted":
        evidence.add_loss("capture_rejected")
    return receipt


def validate_retention_policy(policy):
    """Accept an optional synchronous callback before any native invocation."""
    if policy is not None and (not callable(policy) or inspect.iscoroutinefunction(policy)
                               or inspect.isasyncgenfunction(policy)):
        raise TypeError("retention_policy must be a synchronous callable or None")
    return policy


def policy_retention(evidence, policy, kind, payload, context, *, request_paths=None):
    """Validate detached policy output before one capture; omit on hook failure."""
    if policy is None or payload is MISSING:
        return None
    try:
        value = policy(kind, copy_json(payload), copy_json(context))
        if inspect.iscoroutine(value):
            value.close()
            raise ValueError("retention_policy must return synchronously")
        if value is None:
            raise ValueError("retention_policy must explicitly retain or omit")
        value = freeze(value)
        normalized = _retention("content", value)
        json_text(value).encode("utf-8")
        if request_paths is not None:
            _retained_request(payload, normalized, *request_paths)
        return value
    except (Exception, asyncio.CancelledError):
        evidence.add_loss("retention_policy_failed")
        return {"policy_id": "adapter_policy_error",
                "omitted_reason": "retention_policy_failed"}
