"""An mcp-agent 0.2.6 init hook for each host-owned MCP session.

Register with `registry.register_init_hook(server_name, hook)`; compose any
existing host hook first. This callback observes MCP calls, not model traffic.
"""
from mcp_flightrecorder.integrations.mcp_session_legacy import wrap_session


def make_session_init_hook(recorder, *, client_id, server_key=None):
    """Return the native synchronous ``(session, auth) -> bool`` callback."""
    def attach(session, auth):
        if session is None:
            return False
        observer = wrap_session(recorder, session, client_id=client_id, server_key=server_key)
        session.list_tools = observer.list_tools
        session.call_tool = observer.call_tool
        return True

    return attach
