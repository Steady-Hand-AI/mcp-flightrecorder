"""Owned stdio peer for ClientSession controls; no recorder imports or network."""
import argparse
import asyncio
import json
from pathlib import Path
import sys

from mcp import MCPError
from mcp.server import Server
from mcp.server.stdio import stdio_server
import mcp_types as types


SCHEMA = {"type": "object", "properties": {"text": {"type": "string"},
                                            "case": {"type": "string"}}}


def append(path, value):
    with path.open("a", encoding="utf-8") as output:
        output.write(json.dumps(value, ensure_ascii=False) + "\n")


def make_server(path, paged):
    async def listing(context, params):
        append(path, {"method": "tools/list", "params": None if params is None else
                      params.model_dump(mode="json", by_alias=True, exclude_unset=True)})
        second = params is not None and params.cursor == "page-two"
        names = ["last"] if second else ["echo", "second"]
        return types.ListToolsResult(
            tools=[types.Tool(name=name, description="Ordered declaration " + name,
                             input_schema=SCHEMA) for name in names],
            next_cursor="page-two" if paged and not second else None,
            ttl_ms=1000 if second else 60000, cache_scope="private",
            meta={"page": 2 if second else 1, "retained": "catalog café"})

    async def calling(context, params):
        append(path, {"method": "tools/call", "params":
                      params.model_dump(mode="json", by_alias=True, exclude_unset=True)})
        arguments = params.arguments or {}
        mode = arguments.get("case", "return")
        await context.session.report_progress(1, 2, "entered ✓")
        if mode in ("timeout", "cancelled"):
            await asyncio.Event().wait()
        if mode == "protocol-error":
            raise MCPError(-32602, "native protocol explanation café")
        if mode == "intermediate":
            return types.InputRequiredResult(request_state="resume-state", meta={"retained": "intermediate ✓"})
        content = [] if mode == "empty" else [types.TextContent(
            type="text", text=arguments.get("text", "default ✓"))]
        return types.CallToolResult(content=content, is_error=mode == "tool-error",
                                    structured_content={"mode": mode}, meta={"retained": "result Ω"})

    return Server("Session qualification peer", on_list_tools=listing, on_call_tool=calling)


def malformed_wire_peer(path):
    """Labelled raw-wire double: native ClientSession must reject missing content."""
    for line in sys.stdin:
        message = json.loads(line)
        method = message["method"]
        if "id" not in message:
            continue
        if method == "initialize":
            result = {"protocolVersion": message["params"]["protocolVersion"],
                      "capabilities": {"tools": {}},
                      "serverInfo": {"name": "Malformed wire peer", "version": "1"}}
        elif method == "tools/list":
            append(path, {"method": method, "params": message.get("params")})
            result = {"tools": []}
        elif method == "tools/call":
            append(path, {"method": method, "params": message.get("params"),
                          "returned_wire": {"isError": False}})
            result = {"isError": False}
        else:
            result = {}
        sys.stdout.write(json.dumps({"jsonrpc": "2.0", "id": message["id"], "result": result}) + "\n")
        sys.stdout.flush()


async def main(options):
    server = make_server(options.log, options.paged)
    async with stdio_server() as (read, write):
        await server.run(read, write, server.create_initialization_options())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("log", type=Path)
    parser.add_argument("--paged", action="store_true")
    parser.add_argument("--malformed-wire", action="store_true")
    options = parser.parse_args()
    if options.malformed_wire:
        malformed_wire_peer(options.log)
    else:
        asyncio.run(main(options))
