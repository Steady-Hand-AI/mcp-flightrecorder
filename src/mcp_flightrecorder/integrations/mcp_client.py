"""Observe the two supported MCP 2.3.0 methods without owning the client."""
try:
    from ._mcp_capture import OperationEvidence, _OperationObserver
    from ._mcp_native import _translate_error, _translate_result
except ImportError as error:
    raise ImportError(
        "The MCP observer requires the 'mcp' extra: install mcp-flightrecorder[mcp]."
    ) from error

from ._common import capture_scope, claim_evidence, validate_label, validate_retention_policy

__all__ = ("OperationEvidence", "wrap_mcp")


class _MCPObserver(_OperationObserver):
    """The existing Client method signatures and discovery argument shape."""
    __slots__ = ()

    def list_tools(self, *, evidence=None, cursor=None, meta=None, cache_mode="use"):
        """Snapshot invocation scope; observe the original options when awaited."""
        evidence = claim_evidence(evidence, OperationEvidence)
        scope = capture_scope(evidence, self._recorder)
        options = {"cursor": cursor, "meta": meta, "cache_mode": cache_mode}
        return self._execute(self._native_list, (), options, scope, evidence,
                             method="tools/list", name=None, arguments=options, decision=None)

    def call_tool(self, name, arguments=None, read_timeout_seconds=None,
                  progress_callback=None, *, evidence=None, decision=None,
                  input_responses=None, request_state=None, meta=None):
        """Create lazy work without a native coroutine, event, or reservation."""
        evidence = claim_evidence(evidence, OperationEvidence)
        scope = capture_scope(evidence, self._recorder)
        options = {"input_responses": input_responses, "request_state": request_state, "meta": meta}
        return self._execute(self._native_call,
                             (name, arguments, read_timeout_seconds, progress_callback), options,
                             scope, evidence, method="tools/call", name=name,
                             arguments=arguments, decision=decision)


def wrap_mcp(recorder, native_client, *, client_id, server_key=None, retention_policy=None):
    """Observe the host-owned MCP client's list_tools and call_tool methods."""
    retention_policy = validate_retention_policy(retention_policy)
    client_id = validate_label(client_id, "client_id")
    server_key = validate_label(server_key, "server_key", optional=True)
    return _MCPObserver(recorder, native_client, client_id, server_key,
                        _translate_result, _translate_error, retention_policy)
