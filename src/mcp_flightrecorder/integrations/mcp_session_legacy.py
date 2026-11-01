"""Observe the explicit MCP 1.23.1 ClientSession methods without owning them."""
import asyncio

try:
    import anyio
    import httpx
    from mcp.shared.exceptions import McpError
    from mcp.types import CallToolResult, ListToolsResult, PaginatedRequestParams
except ImportError as error:
    raise ImportError(
        "The legacy ClientSession observer requires the 'mcp-session-legacy' extra: "
        "install mcp-flightrecorder[mcp-session-legacy]."
    ) from error

from .. import MISSING
from ._common import capture_scope, claim_evidence, validate_label, validate_retention_policy
from ._mcp_capture import (
    OperationEvidence, _OperationObserver, _error_projection,
    _freeze_arguments, _result_projection,
)

__all__ = ("OperationEvidence", "wrap_session")


def _translate_result(result, evidence, *, allow_input_required=False):
    """Project only SDK1's trusted result types with its actual isError field."""
    status = "tool_error" if isinstance(result, CallToolResult) and result.isError else "ok"
    try:
        if not isinstance(result, (CallToolResult, ListToolsResult)):
            raise ValueError("The native result is not a supported legacy MCP model")
        body = result.model_dump(mode="json", by_alias=True, exclude_unset=True)
        if isinstance(result, CallToolResult) and type(body.get("content")) is not list:
            raise ValueError("A legacy tool result did not expose a content array")
        body, paths = _result_projection(body, tool_result=isinstance(result, CallToolResult))
        return status, body, paths, True
    except Exception:
        evidence.add_loss("projection_failed")
        return status, MISSING, None, False


def _translate_error(error, evidence):
    """Use SDK1's implemented 408 timeout and preserve bounded native labels."""
    code = error.error.code if isinstance(error, McpError) else None
    if isinstance(error, asyncio.CancelledError):
        status = "cancelled"
    elif isinstance(error, TimeoutError):
        status = "timeout"
    elif isinstance(error, McpError):
        status = "timeout" if code == 408 else "protocol_error"
    elif isinstance(error, (httpx.TransportError, anyio.EndOfStream,
                            anyio.BrokenResourceError, anyio.ClosedResourceError)):
        status = "transport_error"
    else:
        status = "local_error"
    body, metadata, complete = _error_projection(error, code, evidence)
    return status, body, metadata, complete


class _LegacySessionObserver(_OperationObserver):
    """The two SDK1 signatures over the single shared operation lifecycle."""
    __slots__ = ()

    def list_tools(self, cursor=None, *, params=None, evidence=None):
        """Keep native cursor/params precedence and warning timing untouched."""
        evidence = claim_evidence(evidence, OperationEvidence)
        scope = capture_scope(evidence, self._recorder)
        return self._execute(self._native_list, (cursor,), {"params": params}, scope, evidence,
                             method="tools/list", name=None,
                             arguments={"cursor": cursor, "params": params}, decision=None)

    def call_tool(self, name, arguments=None, read_timeout_seconds=None,
                  progress_callback=None, *, meta=None, evidence=None, decision=None):
        """Forward native timedelta, callback and metadata without serialization."""
        evidence = claim_evidence(evidence, OperationEvidence)
        scope = capture_scope(evidence, self._recorder)
        return self._execute(self._native_call,
                             (name, arguments, read_timeout_seconds, progress_callback),
                             {"meta": meta}, scope, evidence, method="tools/call", name=name,
                             arguments=arguments, decision=decision)

    def _prepare_arguments(self, method, arguments, evidence):
        if method != "tools/list":
            return _freeze_arguments(arguments, evidence)
        try:
            params = arguments["params"]
            if params is not None:
                if not isinstance(params, PaginatedRequestParams):
                    raise ValueError("Discovery capture requires trusted legacy pagination params")
                params = params.model_dump(mode="json", by_alias=True, exclude_unset=True)
        except Exception:
            evidence.add_loss("projection_failed")
            return MISSING, False
        return _freeze_arguments({"cursor": arguments["cursor"], "params": params}, evidence)


def wrap_session(recorder, native_session, *, client_id, server_key=None, retention_policy=None):
    """Observe an SDK1 session's methods; the caller still owns its lifecycle."""
    retention_policy = validate_retention_policy(retention_policy)
    client_id = validate_label(client_id, "client_id")
    server_key = validate_label(server_key, "server_key", optional=True)
    return _LegacySessionObserver(recorder, native_session, client_id, server_key,
                                  _translate_result, _translate_error, retention_policy)
