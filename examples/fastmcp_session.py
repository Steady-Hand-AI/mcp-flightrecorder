"""Wrap an active FastMCP client.session; attach again after reconnect.

Call only inside the host-owned `async with client` scope. Cached high-level
results may bypass the session and are not observed.
"""
from mcp_flightrecorder.integrations.mcp_session import wrap_session


def attach_session(recorder, native_session, *, client_id, server_key=None) -> None:
    """Observe the two native methods once; leave session ownership with the host."""
    observer = wrap_session(recorder, native_session, client_id=client_id,
                            server_key=server_key)
    # Construction retains both original bound methods before either assignment.
    native_session.list_tools = observer.list_tools
    native_session.call_tool = observer.call_tool
