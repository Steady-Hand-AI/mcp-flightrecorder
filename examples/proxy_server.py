"""A deterministic MCP stdio server for the recording-proxy walkthrough.

Run this source-shipped example with the optional modern MCP SDK installed.
Stdout belongs exclusively to the MCP transport; no credentials are required.
"""
from __future__ import annotations

import asyncio

from mcp.server import Server
from mcp.server.stdio import stdio_server
import mcp.types as types


async def list_tools(context, params):
    return types.ListToolsResult(tools=[types.Tool(
        name="answer",
        description="Return the deterministic example answer.",
        input_schema={"type": "object", "properties": {}, "additionalProperties": False},
    )])


async def call_tool(context, params):
    if params.name != "answer" or params.arguments != {}:
        raise ValueError("The example accepts answer with empty arguments")
    return types.CallToolResult(
        content=[types.TextContent(type="text", text="42")], is_error=False,
    )


async def main():
    server = Server("proxy-example", on_list_tools=list_tools, on_call_tool=call_tool)
    async with stdio_server() as streams:
        await server.run(*streams, server.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
