"""Owned real MCP stdio server for native host qualification."""
import argparse
import asyncio
import json
from pathlib import Path

from mcp import types
from mcp.server.lowlevel import Server
from mcp.server.stdio import stdio_server


def append_event(path, value):
    with Path(path).open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(value, ensure_ascii=False) + "\n")


def tool_result(label, name, arguments):
    if name == "positive" and arguments["x"] <= 0:
        return types.CallToolResult(content=[types.TextContent(type="text", text="positive input required")], isError=True)
    if name == "empty":
        return types.CallToolResult(content=[types.TextContent(type="text", text="")])
    value = {"server": label, "x": arguments["x"], "unit": "µ"}
    if name == "structured":
        return types.CallToolResult(content=[], structuredContent=value)
    return types.CallToolResult(content=[types.TextContent(type="text", text=json.dumps(value, ensure_ascii=False))])


def make_server(label, tool_name, log_path):
    async def list_tools(context, params):
        append_event(log_path, {"method": "tools/list", "server": label})
        tool = types.Tool(name=tool_name, description="Return the observed integer with its real server identity.",
                          inputSchema={"type": "object", "properties": {"x": {"type": "integer"}},
                                       "required": ["x"], "additionalProperties": False})
        return types.ListToolsResult(tools=[tool])

    async def call_tool(context, params):
        append_event(log_path, {"method": "tools/call", "server": label,
                                "name": params.name, "arguments": params.arguments})
        return tool_result(label, params.name, params.arguments)

    async def list_prompts(context, params):
        append_event(log_path, {"method": "prompts/list", "server": label})
        return types.ListPromptsResult(prompts=[])

    async def list_resources(context, params):
        append_event(log_path, {"method": "resources/list", "server": label})
        return types.ListResourcesResult(resources=[types.Resource(uri="review://context", name="Review context", mimeType="text/plain")])

    async def read_resource(context, params):
        append_event(log_path, {"method": "resources/read", "server": label, "uri": str(params.uri)})
        return types.ReadResourceResult(contents=[types.TextResourceContents(uri=params.uri, mimeType="text/plain",
                                        text="Resource witness: amount=7; unit=µ; origin=" + label)])

    async def list_templates(context, params):
        append_event(log_path, {"method": "resources/templates/list", "server": label})
        return types.ListResourceTemplatesResult(resourceTemplates=[])

    return Server("review-" + label, version="1", on_list_tools=list_tools, on_call_tool=call_tool,
                  on_list_prompts=list_prompts, on_list_resources=list_resources,
                  on_list_resource_templates=list_templates, on_read_resource=read_resource)


async def serve(options):
    server = make_server(options.label, options.tool, options.log)
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", required=True)
    parser.add_argument("--tool", required=True)
    parser.add_argument("--log", required=True)
    asyncio.run(serve(parser.parse_args()))


if __name__ == "__main__":
    main()
