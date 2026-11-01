"""Actual SDK1 stdio server with independent argument/result/overlap ledgers."""
import argparse
import asyncio
import json
from pathlib import Path

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import CallToolResult, TextContent, Tool


def append(path, kind, payload):
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps({"kind": kind, "payload": payload}, ensure_ascii=False) + "\n")


async def run(options):
    server = Server("mcp-agent-integration-peer")
    both_entered = asyncio.Event()
    arrivals = 0

    @server.list_tools()
    async def list_tools():
        tools = [Tool(name="echo", description="Return the exact label and integer.",
                      inputSchema={"type": "object", "properties": {"label": {"type": "string"},
                                   "value": {"type": "integer"}}, "required": ["label", "value"],
                                   "additionalProperties": False}),
                 Tool(name="refuse", description="Return an observed refusal.",
                      inputSchema={"type": "object", "properties": {"reason": {"type": "string"}},
                                   "required": ["reason"], "additionalProperties": False})]
        append(options.ledger, "tools/list", [tool.model_dump(mode="json", by_alias=True, exclude_unset=True) for tool in tools])
        return tools

    @server.call_tool()
    async def call_tool(name, arguments):
        nonlocal arrivals
        arrivals += 1
        occurrence = arrivals
        append(options.ledger, "tools/call", {"name": name, "arguments": arguments})
        append(options.ledger, "entered", {"occurrence": occurrence})
        if options.parallel:
            if arrivals == 2:
                both_entered.set()
            await asyncio.wait_for(both_entered.wait(), 5)
        result = CallToolResult(
            content=[TextContent(type="text", text="refused: " + arguments["reason"] if name == "refuse"
                                 else json.dumps(arguments, sort_keys=True))], isError=name == "refuse")
        append(options.ledger, "result", result.model_dump(mode="json", by_alias=True, exclude_unset=True))
        return result

    async with stdio_server() as streams:
        await server.run(*streams, server.create_initialization_options())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("ledger", type=Path)
    parser.add_argument("--parallel", action="store_true")
    options = parser.parse_args()

    asyncio.run(run(options))
