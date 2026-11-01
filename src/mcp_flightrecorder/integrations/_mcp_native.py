"""Fixed MCP2 result/error translations for its two public method facades."""
import asyncio

import anyio
import httpx2
from mcp import MCPError
from mcp_types import CallToolResult, InputRequiredResult, ListToolsResult, REQUEST_TIMEOUT

from .. import MISSING
from ._common import copy_json
from ._mcp_capture import _error_projection, _result_projection


def _translate_result(result, evidence, *, allow_input_required=False):
    """Trust MCP2 models and keep its explicitly allowed intermediate partial."""
    status = "tool_error" if isinstance(result, CallToolResult) and result.is_error else "ok"
    try:
        if isinstance(result, InputRequiredResult) and allow_input_required:
            body = copy_json(result.model_dump(mode="json", by_alias=True, exclude_unset=True))
            return status, body, None, False
        if not isinstance(result, (CallToolResult, ListToolsResult)):
            raise ValueError("The native result is not a supported MCP model")
        body, paths = _result_projection(
            result.model_dump(mode="json", by_alias=True, exclude_unset=True),
            tool_result=isinstance(result, CallToolResult),
        )
        return status, body, paths, True
    except Exception:
        evidence.add_loss("projection_failed")
        return status, MISSING, None, False


def _translate_error(error, evidence):
    """Classify only the actual MCP2 exception and its exposed native code."""
    code = error.code if isinstance(error, MCPError) else None
    if isinstance(error, asyncio.CancelledError):
        status = "cancelled"
    elif isinstance(error, TimeoutError):
        status = "timeout"
    elif isinstance(error, MCPError):
        status = "timeout" if code == REQUEST_TIMEOUT else "protocol_error"
    elif isinstance(error, (httpx2.TransportError, anyio.EndOfStream,
                            anyio.BrokenResourceError, anyio.ClosedResourceError)):
        status = "transport_error"
    else:
        status = "local_error"
    body, metadata, complete = _error_projection(error, code, evidence)
    return status, body, metadata, complete
