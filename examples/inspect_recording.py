"""Inspect a portable recording through an installed MCP stdio subprocess."""
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import sys

from mcp import Client
from mcp.client.stdio import StdioServerParameters


async def inspect_recording(path: Path, run_id: str | None, allow_content: bool):
    arguments = ["mcp", str(path.resolve())]
    if allow_content:
        arguments.append("--allow-content")
    server = StdioServerParameters(
        command=str(Path(sys.executable).with_name("fr")), args=arguments,
    )
    async with Client(server) as client:
        tools = await client.list_tools()
        resources = await client.list_resources()
        schema = await client.read_resource("mfr://schema/inspection")
        report = {
            "tools": [tool.name for tool in tools.tools],
            "resources": [str(resource.uri) for resource in resources.resources],
            "schema": json.loads(schema.contents[0].text),
        }

        async def read(name, **values):
            result = await client.call_tool(name, {
                **values, "timeout": 10.0, "max_output_bytes": 65536,
            })
            if result.is_error:
                raise RuntimeError(result.content[0].text)
            envelope = json.loads(result.content[0].text)
            if not envelope["ok"]:
                raise RuntimeError(envelope["error"])
            return envelope

        report["runs"] = await read("list_runs")
        page = report["runs"]["data"]
        if run_id is None:
            if len(page["items"]) != 1 or page["has_more"]:
                raise ValueError("Choose an explicit --run from list_runs; this file is not a singleton")
            run_id = page["items"][0]["run_id"]
        report["problems"] = await read("summarize_problems", run_id=run_id)
        report["turns"] = await read(
            "find_turns", run_id=run_id, filters={"text_mismatch": True}, last=True,
        )
        if not report["turns"]["data"]["items"]:
            raise ValueError("This walkthrough needs a recorded raw text mismatch; use tool_roundtrip.py")
        turn_id = report["turns"]["data"]["items"][0]["turn"]["turn_id"]
        report["overview"] = await read("explain_turn", run_id=run_id, turn_id=turn_id)
        calls = report["overview"]["data"]["calls"]
        if len(calls) != 2:
            raise ValueError("This walkthrough expects the two-call tool_roundtrip.py recording")
        selected = calls[1]
        report["detail"] = await read(
            "explain_turn", run_id=run_id, turn_id=turn_id,
            model_call_id=selected["model_call_id"],
        )
        report["ancestry"] = await read(
            "trace_back", run_id=run_id, request_event_id=selected["request_event_ids"][0],
        )
        if allow_content:
            artifacts = report["detail"]["data"]["detail"]["artifacts"]
            artifact = next(item for item in artifacts
                            if item["omitted_reason"] is None and item["stored_bytes"] > 0)
            report["payload"] = await read(
                "read_payload", run_id=run_id, artifact_id=artifact["artifact_id"],
                offset=0, max_bytes=128,
            )
        return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("recording", type=Path)
    parser.add_argument("--run", help="Explicit run ID for a multi-run recording")
    parser.add_argument("--allow-content", action="store_true",
                        help="Enable the server content tool and request one retained payload window")
    args = parser.parse_args()
    result = asyncio.run(inspect_recording(args.recording, args.run, args.allow_content))
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
