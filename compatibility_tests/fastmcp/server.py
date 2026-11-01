"""Test-owned tools served by the installed, unmodified FastMCP framework."""
from pathlib import Path
import json
import sys

from fastmcp import FastMCP
from fastmcp.server.middleware import Middleware
from fastmcp.tools import ToolResult
from mcp_types import TextContent


LEDGER = Path(sys.argv[1])


def record(value):
    with LEDGER.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(value, ensure_ascii=False) + "\n")


class BoundaryLedger(Middleware):
    async def on_list_tools(self, context, call_next):
        record({"kind": "server_request", "method": "tools/list"})
        return await call_next(context)

    async def on_call_tool(self, context, call_next):
        params = context.message.model_dump(mode="json", by_alias=True, exclude_unset=True)
        record({"kind": "server_request", "method": "tools/call", "params": params})
        return await call_next(context)


server = FastMCP("Reviewer stdio tools", middleware=[BoundaryLedger()],
                 cache_ttl=60, cache_scope="public", mask_error_details=False)


@server.tool
def echo(text: str) -> ToolResult:
    """Return exactly one text block and explicit result metadata."""
    record({"kind": "tool", "name": "echo", "arguments": {"text": text}})
    return ToolResult(content=[TextContent(type="text", text=text)],
                      meta={"source": "reviewer-server"})


@server.tool
def structured(a: int, b: int) -> dict[str, int]:
    """Return a typed structured object for client hydration."""
    record({"kind": "tool", "name": "structured", "arguments": {"a": a, "b": b}})
    return {"sum": a + b, "difference": a - b}


@server.tool
def empty() -> ToolResult:
    """Return a successful empty content array."""
    record({"kind": "tool", "name": "empty", "arguments": {}})
    return ToolResult(content=[])


@server.tool
def fail(message: str) -> ToolResult:
    """Produce a native FastMCP tool error."""
    record({"kind": "tool", "name": "fail", "arguments": {"message": message}})
    raise ValueError(message)


if __name__ == "__main__":
    server.run(transport="stdio", show_banner=False)
