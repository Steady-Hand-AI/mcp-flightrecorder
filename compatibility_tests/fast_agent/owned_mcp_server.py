"""Test-owned real stdio server, using the installed MCP 2.3 API."""
import asyncio
import json
import sys
from pathlib import Path

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp_types import CallToolResult, ListToolsResult, TextContent, Tool


LEDGER = Path(sys.argv[1])


def record(value):
    with LEDGER.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(value, ensure_ascii=False) + "\n")


async def listed(context, params):
    record({"method": "tools/list", "cursor": params.cursor if params else None})
    return ListToolsResult(tools=[
        Tool(name="echo", description="Return an owned fixture string.", inputSchema={
            "type": "object", "properties": {"value": {"type": "string"}},
            "required": ["value"], "additionalProperties": False,
        }),
        Tool(name="fail", description="Return an owned fixture tool error.", inputSchema={
            "type": "object", "properties": {"value": {"type": "string"}},
            "required": ["value"], "additionalProperties": False,
        }),
    ])


async def called(context, params):
    record({"method": "tools/call", "name": params.name, "arguments": params.arguments})
    return CallToolResult(content=[TextContent(type="text", text=(
        "fixture-error:" if params.name == "fail" else "fixture-echo:"
    ) + params.arguments["value"])], isError=params.name == "fail")


async def main():
    server = Server("review-owned", version="1", on_list_tools=listed, on_call_tool=called)
    async with stdio_server() as (reader, writer):
        await server.run(reader, writer, server.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
