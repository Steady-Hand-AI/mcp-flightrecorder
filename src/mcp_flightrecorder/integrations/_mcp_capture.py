"""One SDK-independent operation workflow for the explicit MCP facades."""

from .. import MISSING
from ._common import (
    EvidenceBase, capture_call, copy_json, policy_retention, validate_label,
)


class OperationEvidence(EvidenceBase):
    """One invocation's admission receipts; no native inputs or results are kept."""
    __slots__ = ("start", "terminal")

    def __init__(self):
        super().__init__()
        self.start = None
        self.terminal = None


def _freeze_arguments(arguments, evidence):
    """Decline unavailable JSON capture without consuming or coercing input."""
    try:
        return copy_json(arguments), True
    except Exception:
        evidence.add_loss("projection_failed")
        return MISSING, False


def _decision_identity(decision, evidence):
    """Validate public receipt structure, leaving ownership resolution to core."""
    if decision is None:
        return None, True
    try:
        if decision.status != "accepted":
            raise ValueError("The decision was not accepted")
        return validate_label(decision.decision_id, "decision_id"), True
    except Exception:
        evidence.add_loss("invalid_decision")
        return None, False


def _attribution(scope, decision_id):
    """Use only the captured dispatch, with explicit decision propagation next."""
    dispatch_id = None if scope is None else scope.dispatch_event_id
    if dispatch_id is not None:
        return dispatch_id, "host_hook"
    return None, "propagated" if decision_id is not None else "none"


def _result_projection(body, *, tool_result):
    """Freeze an already translated JSON body and locate its actual text."""
    body = copy_json(body)
    paths = None
    if tool_result:
        paths = []
        for index, block in enumerate(body["content"]):
            if block.get("type") == "text":
                if type(block.get("text")) is not str:
                    raise ValueError("A text block did not expose its text")
                paths.append("/content/" + str(index) + "/text")
    return body, paths


def _error_projection(error, code, evidence):
    """Keep bounded control labels separate from optionally retained messages."""
    name = type(error).__name__.encode("utf-8", errors="replace")[:64].decode("utf-8", errors="ignore")
    name = name or "Exception"
    metadata = {"origin": "client", "type": name, "code": code}
    try:
        if code is not None and (type(code) is not int or not -(2 ** 63) <= code < 2 ** 63):
            metadata["code"] = None
            raise ValueError("The exposed code cannot fit core control metadata")
        return copy_json({"error": {"type": name, "message": str(error)}}), metadata, True
    except Exception:
        evidence.add_loss("projection_failed")
        return MISSING, metadata, False


class _OperationObserver:
    """Method decoration only: no context manager, cache, retry, or client close."""
    __slots__ = ("_recorder", "_native_list", "_native_call", "_client_id", "_server_key",
                 "_translate_result", "_translate_error", "_retention_policy")

    def __init__(self, recorder, native_client, client_id, server_key,
                 translate_result, translate_error, retention_policy=None):
        self._recorder = recorder
        self._native_list = native_client.list_tools
        self._native_call = native_client.call_tool
        self._client_id = client_id
        self._server_key = server_key
        self._translate_result = translate_result
        self._translate_error = translate_error
        self._retention_policy = retention_policy

    def _prepare_arguments(self, method, arguments, evidence):
        """The Client boundary supplies ordinary JSON; session listing is typed."""
        return _freeze_arguments(arguments, evidence)

    def _retention(self, evidence, kind, payload, method, name):
        if self._retention_policy is None or payload is MISSING:
            return None
        context = {"client_id": self._client_id, "server_key": self._server_key,
                   "method": method, "name": name}
        return policy_retention(evidence, self._retention_policy, kind, payload, context)

    def _start(self, evidence, scope, method, name, arguments, decision):
        frozen, complete = self._prepare_arguments(method, arguments, evidence)
        decision_id, valid_decision = _decision_identity(decision, evidence)
        dispatch_id, basis = _attribution(scope, decision_id) if valid_decision else (None, "none")
        evidence.start = capture_call(
            evidence, self._recorder.start_operation, scope=scope,
            client_id=self._client_id, server_key=self._server_key,
            method=method, name=name, arguments=frozen,
            decision_id=decision_id, dispatch_event_id=dispatch_id,
            link_basis=basis, complete=complete and valid_decision,
            retention=self._retention(evidence, "mcp.request",
                MISSING if frozen is MISSING else {"arguments": frozen}, method, name),
        )

    def _finish(self, evidence, *, status, result, method, name, text_paths=None, error=None, complete=True):
        start = evidence.start
        if start is None or start.status != "accepted" or start.ticket is None:
            return
        evidence.terminal = capture_call(
            evidence, self._recorder.finish_operation, start.ticket,
            status=status, result=result, resolution="unknown", input_rounds=None,
            error=error, text_paths=text_paths, complete=complete,
            retention=self._retention(evidence, "mcp.response", result, method, name),
        )

    def _returned(self, evidence, result, *, method, name, allow_input_required=False):
        status, body, paths, complete = self._translate_result(
            result, evidence, allow_input_required=allow_input_required
        )
        self._finish(evidence, status=status, result=body, method=method, name=name,
                     text_paths=paths, complete=complete)

    def _raised(self, evidence, error, *, method, name):
        status, body, metadata, complete = self._translate_error(error, evidence)
        self._finish(evidence, status=status, result=body, method=method, name=name,
                     error=metadata, complete=complete)

    async def _execute(self, native, args, kwargs, scope, evidence, *, method, name,
                       arguments, decision, allow_input_required=False):
        # Capture faults are separated from the native try/except so an observer
        # cannot turn an SDK success into a host failure or run the SDK twice.
        try:
            self._start(evidence, scope, method, name, arguments, decision)
        except Exception:
            evidence.add_loss("capture_rejected")
        try:
            result = await native(*args, **kwargs)
        except BaseException as error:
            try:
                self._raised(evidence, error, method=method, name=name)
            except Exception:
                evidence.add_loss("capture_rejected")
            raise
        try:
            self._returned(evidence, result, method=method, name=name,
                           allow_input_required=allow_input_required)
        except Exception:
            evidence.add_loss("capture_rejected")
        return result

