"""Observe the MCP2 ClientSession boundary; the host owns its transport."""
try:
    from mcp_types import PaginatedRequestParams
    from ._mcp_capture import OperationEvidence, _OperationObserver, _freeze_arguments
    from ._mcp_native import _translate_error, _translate_result
except ImportError as error:
    raise ImportError(
        "The ClientSession observer requires the 'mcp-session' extra: "
        "install mcp-flightrecorder[mcp-session]."
    ) from error

from .. import MISSING
from ._common import capture_scope, claim_evidence, validate_label, validate_retention_policy

__all__ = ("OperationEvidence", "wrap_session")


class _SessionObserver(_OperationObserver):
    """Two explicit session methods over the shared logical-operation workflow."""
    __slots__ = ()

    def list_tools(self, *, params=None, evidence=None):
        """Capture scope now; freeze the exposed typed params only on execution."""
        evidence = claim_evidence(evidence, OperationEvidence)
        scope = capture_scope(evidence, self._recorder)
        return self._execute(self._native_list, (), {"params": params}, scope, evidence,
                             method="tools/list", name=None, arguments=params, decision=None)

    def call_tool(self, name, arguments=None, read_timeout_seconds=None,
                  progress_callback=None, *, input_responses=None,
                  request_state=None, meta=None, allow_input_required=False,
                  allow_claimed=False, evidence=None, decision=None):
        """Forward the native invocation without taking over input rounds or cleanup."""
        evidence = claim_evidence(evidence, OperationEvidence)
        scope = capture_scope(evidence, self._recorder)
        options = {"input_responses": input_responses, "request_state": request_state,
                   "meta": meta, "allow_input_required": allow_input_required,
                   "allow_claimed": allow_claimed}
        return self._execute(self._native_call,
                             (name, arguments, read_timeout_seconds, progress_callback),
                             options, scope, evidence, method="tools/call", name=name,
                             arguments=arguments, decision=decision,
                             allow_input_required=allow_input_required)

    def _prepare_arguments(self, method, arguments, evidence):
        if method != "tools/list":
            return _freeze_arguments(arguments, evidence)
        try:
            if arguments is not None:
                if not isinstance(arguments, PaginatedRequestParams):
                    raise ValueError("Discovery capture requires trusted pagination params")
                arguments = arguments.model_dump(mode="json", by_alias=True, exclude_unset=True)
        except Exception:
            evidence.add_loss("projection_failed")
            return MISSING, False
        return _freeze_arguments({"params": arguments}, evidence)

def wrap_session(recorder, native_session, *, client_id, server_key=None, retention_policy=None):
    """Observe a host-owned ClientSession's list_tools and call_tool methods."""
    retention_policy = validate_retention_policy(retention_policy)
    client_id = validate_label(client_id, "client_id")
    server_key = validate_label(server_key, "server_key", optional=True)
    return _SessionObserver(recorder, native_session, client_id, server_key,
                            _translate_result, _translate_error, retention_policy)
