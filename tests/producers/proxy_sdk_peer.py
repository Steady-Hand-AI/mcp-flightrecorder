"""Genuine SDK stdio peer with independent request and native-result evidence."""
import argparse
import asyncio
import json
import os
from pathlib import Path
from typing import Literal


SCHEMA = {"type": "object", "properties": {"text": {"type": "string"}},
          "additionalProperties": False}
STATE = "opaque continuation café 🌱 /?=not-a-proxy-token"


def append(path, value):
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(value, ensure_ascii=False) + "\n")


async def modern(options):
    import mcp.types as types
    from mcp.server import Server
    from mcp.server.stdio import stdio_server
    from mcp.shared.exceptions import MCPError

    class DeferredResult(types.Result):
        result_type: Literal["peer_deferred"] = "peer_deferred"
        status: Literal["working"] = "working"
        handle: str
        content: list[types.TextContent]
        tools: list[types.Tool]

    async def listing(context, params):
        names = ("echo", "refuse", "protocol", "continue", "wait")
        if options.claimed_extension:
            names += ("defer",)
        return types.ListToolsResult(tools=[types.Tool(name=name, input_schema=SCHEMA) for name in names])

    async def calling(context, params):
        append(options.ledger, {"kind": "invocation", "id": context.request_id,
                                "name": params.name, "arguments": params.arguments,
                                "request_state": params.request_state,
                                "input_responses": None if params.input_responses is None else {
                                    key: value.model_dump(mode="json", by_alias=True, exclude_unset=True)
                                    for key, value in params.input_responses.items()}})
        if params.name == "protocol":
            raise MCPError(-32001, "peer protocol refusal", {"native": "café 🌱"})
        if params.name == "defer" and options.claimed_extension:
            return DeferredResult(handle="opaque handle café 🌱",
                content=[types.TextContent(type="text", text="Extension progress is not a core tool answer.")],
                tools=[types.Tool(name="opaque-extension-tool", input_schema=SCHEMA)])
        if params.name == "continue" and params.request_state is None:
            return types.InputRequiredResult(request_state=STATE)
        if params.name == "continue":
            assert params.request_state == STATE
        if params.name == "wait":
            await context.session.report_progress(1, 2, "entered")
            try:
                await asyncio.Event().wait()
            finally:
                append(options.ledger, {"kind": "wait-exited", "id": context.request_id})
        return types.CallToolResult(content=[types.TextContent(type="text", text=json.dumps(
            {"name": params.name, "arguments": params.arguments}, ensure_ascii=False, sort_keys=True))],
            is_error=params.name == "refuse")

    async def audit(context, call_next):
        append(options.ledger, {"kind": "request", "id": context.request_id,
                                "method": context.method, "params": context.params,
                                "protocol_version": context.protocol_version})
        try:
            result = await call_next(context)
        except MCPError as error:
            append(options.ledger, {"kind": "protocol-error", "id": context.request_id,
                                    "error": error.error.model_dump(mode="json", by_alias=True, exclude_unset=True)})
            raise
        except asyncio.CancelledError:
            append(options.ledger, {"kind": "cancelled-handler", "id": context.request_id})
            raise
        if options.mrtr_extension and isinstance(result, dict) and result.get("resultType") == "input_required":
            # Public middleware adds an unknown field to the genuine native
            # result. The client must still classify it by resultType.
            result = {**result, "content": []}
        if context.request_id is not None:
            append(options.ledger, {"kind": "result", "id": context.request_id,
                                    "result": result.model_dump(mode="json", by_alias=True, exclude_unset=True)
                                    if hasattr(result, "model_dump") else result})
        return result

    server = Server("proxy-sdk-peer", version="1", on_list_tools=listing, on_call_tool=calling)
    server.middleware.append(audit)
    async with stdio_server() as streams:
        await server.run(*streams, server.create_initialization_options())


async def legacy(options):
    import anyio
    from mcp.server import Server
    from mcp.server.stdio import stdio_server
    from mcp.types import CallToolResult, TextContent, Tool

    class ObservedStream:
        """Observe native SDK messages without replacing or modifying any object."""
        def __init__(self, stream, direction):
            self.stream, self.direction = stream, direction

        async def __aenter__(self):
            await self.stream.__aenter__()
            return self

        async def __aexit__(self, *args):
            return await self.stream.__aexit__(*args)

        def record(self, message):
            append(options.ledger, {"kind": self.direction, "message":
                message.message.model_dump(mode="json", by_alias=True, exclude_unset=True)})

        async def send(self, message):
            self.record(message)
            await self.stream.send(message)

        def __aiter__(self):
            return self

        async def __anext__(self):
            try:
                message = await self.stream.receive()
            except anyio.EndOfStream:
                raise StopAsyncIteration from None
            self.record(message)
            return message

    server = Server("proxy-legacy-sdk-peer", version="1")

    @server.list_tools()
    async def listing():
        # Native counters start at zero in each direction. While client request
        # 1 remains live, the second genuine reverse ping also uses integer 1.
        context = server.request_context
        append(options.ledger, {"kind": "listing-entered", "id": context.request_id})
        await context.session.send_ping()
        await context.session.send_ping()
        return [Tool(name=name, inputSchema=SCHEMA) for name in ("echo", "refuse")]

    @server.call_tool()
    async def calling(name, arguments):
        append(options.ledger, {"kind": "invocation", "id": server.request_context.request_id,
                                "name": name, "arguments": arguments})
        return CallToolResult(content=[TextContent(type="text", text=json.dumps(
            {"name": name, "arguments": arguments}, ensure_ascii=False, sort_keys=True))],
            isError=name == "refuse")

    async with stdio_server() as (read, write):
        await server.run(ObservedStream(read, "client-to-server"),
                         ObservedStream(write, "server-to-client"), server.create_initialization_options())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--pid", type=Path, required=True)
    parser.add_argument("--legacy", action="store_true")
    parser.add_argument("--mrtr-extension", action="store_true")
    parser.add_argument("--claimed-extension", action="store_true")
    options = parser.parse_args()
    options.pid.write_text(str(os.getpid()))
    asyncio.run(legacy(options) if options.legacy else modern(options))
