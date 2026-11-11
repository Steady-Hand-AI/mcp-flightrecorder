"""A complete facade-based OpenAI Chat Completions → MCP → submitted answer example.

Run this matching source example with an installed package and its integration
dependencies. The local peer setup lives in openai_mcp_peers.py and reuses the
existing tool_roundtrip.py peer. No API key or remote service is required.
"""
import argparse
import asyncio
from copy import deepcopy
import json
from pathlib import Path
import runpy

import mcp_types

from mcp_flightrecorder.easy import AgentRecorder
from mcp_flightrecorder.testing import EvidenceReadError, assert_evidence


# Load source-owned peers under -I; the library and SDKs remain installed imports.
_peers = runpy.run_path(str(Path(__file__).with_name("openai_mcp_peers.py")))
ExamplePeers, local_clients = _peers["ExamplePeers"], _peers["local_clients"]
MODEL, QUESTION, CASES = _peers["MODEL"], _peers["QUESTION"], _peers["CASES"]
MIN_ACTIVITY = {"turns": 1, "model_requests": 2, "model_responses": 2,
                "decisions": 1, "tool_calls": 1, "answer_occurrences": 1}


class UnsupportedToolResult(ValueError):
    """The example's text-only formatter cannot represent the native result."""


def answer_openai_tool(attempt, tool_call_id, result, *, transform=None):
    """Format only a complete, nonempty text-only result from this native attempt.

    An intended transform receives list[str] and must return str. Its exceptions
    propagate unchanged. Tool-error text follows the same formatting rules.
    """
    if (not isinstance(result, mcp_types.CallToolResult) or result.result_type != "complete"
            or result.structured_content is not None or not result.content
            or any(not isinstance(block, mcp_types.TextContent) for block in result.content)):
        raise UnsupportedToolResult("This example requires a complete nonempty text-only MCP result without structured content")
    texts = [block.text for block in result.content]
    if transform is not None:
        content = transform(texts)
        if not isinstance(content, str):
            raise TypeError("The answer transform must return str")
    else:
        content = texts[0] if len(texts) == 1 else [{"type": "text", "text": text} for text in texts]
    return attempt.answer({"role": "tool", "tool_call_id": tool_call_id, "content": content},
                          prepared=transform is not None)


def prepare_answer(texts):
    """This application's intended display transformation."""
    return "Tool result: " + " | ".join(texts)


async def run_host(recorder, provider, native_mcp, *, transform=None, join_undeclared=False):
    """Use a caller-owned model/recorder and scope the supplied inert MCP client."""
    conversation = recorder.conversation("openai-mcp-example")
    try:
        with conversation.turn(QUESTION):
            async with native_mcp:
                tools = recorder.mcp(native_mcp)
                catalog = await tools.list_tools()
                functions = [{"type": "function", "function": {
                    "name": tool.name, "description": tool.description,
                    "parameters": deepcopy(tool.input_schema)}} for tool in catalog.tools]
                messages = [{"role": "user", "content": QUESTION}]
                payload = {"model": MODEL, "messages": messages, "tools": functions, "temperature": 0}
                with conversation.model_call(payload, create=provider.chat.completions.create) as call:
                    first = await call.create()

                # The host selects the native response, independently of capture admission.
                selected_choice = first.choices[0]
                selected = selected_choice.message.tool_calls[0]
                arguments = json.loads(selected.function.arguments)
                choice = call.choice(selected.function.name, arguments,
                                     choice_index=selected_choice.index, tool_index=0)
                with choice.run() as attempt:
                    result = await tools.call_tool(selected.function.name, arguments)
            # Finish the MCP task group before invoking application formatting callbacks.
            answer = answer_openai_tool(attempt, selected.id, result, transform=transform)
            if join_undeclared:
                # Deliberate bug: flattening loses the original text-block boundaries.
                answer["content"] = "".join(block["text"] for block in answer["content"])
            messages.extend([selected_choice.message.to_dict(mode="json", use_api_names=True,
                                                              exclude_unset=True), answer])
            with conversation.model_call(payload, create=provider.chat.completions.create) as followup:
                second = await followup.create()
            return second.choices[0].message.content
    finally:
        conversation.close()


async def run_example(output, *, case="single", transform=None, peers=None):
    """Own and close a fresh recording plus deterministic native SDK clients."""
    if case not in CASES:
        raise ValueError("Unknown example case")
    peers = ExamplePeers(case) if peers is None else peers
    if case == "prepared" and transform is None:
        transform = prepare_answer
    recorder = AgentRecorder(output, capture="content", all_model_calls_recorded=True).start()
    try:
        async with local_clients(peers) as (provider, native):
            answer = await run_host(recorder, provider, native, transform=transform,
                                    join_undeclared=case == "joined")
    finally:
        closed = await recorder.aclose()
    return {"answer": answer, "backend": peers.observations(),
            "recording": None if recorder.path is None else str(Path(recorder.path).resolve()),
            "close": None if closed is None else {
                "writer_stopped": closed.writer_stopped, "portable": closed.portable,
                "run_close_state": closed.run_close_state, "reason": closed.reason}}


def assert_application(observed, case):
    """Independent requirements checked against actual SDK transport requests."""
    assert observed["tool_calls"] == [{"name": "answer", "arguments": {}}], "Unexpected native tool call"
    requests = [json.loads(body) for body in observed["provider_request_bodies"]]
    if case == "unsupported":
        assert len(requests) == 1, "Unsupported content must not be submitted to the model"
        return
    expected = {
        "single": "42",
        "multiple": [{"type": "text", "text": "4"}, {"type": "text", "text": "2"}],
        "tool-error": "Tool unavailable",
        "joined": [{"type": "text", "text": "4"}, {"type": "text", "text": "2"}],
        "prepared": "Tool result: 4 | 2",
    }[case]
    assert len(requests) == 2, "Expected two model requests"
    assert requests[1]["messages"][2] == {
        "role": "tool", "tool_call_id": "call_answer", "content": expected}, "Submitted answer differs from the application requirement"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="New recording file; its parent must exist")
    parser.add_argument("--case", choices=CASES, default="single")
    args = parser.parse_args()
    peers = ExamplePeers(args.case)
    refused = False
    try:
        summary = asyncio.run(run_example(args.output, case=args.case, peers=peers))
        if not (summary["close"]["writer_stopped"] and summary["close"]["portable"]):
            raise RuntimeError("Recording did not close portably")
    except UnsupportedToolResult as error:
        if args.case != "unsupported":
            raise
        refused = True
        print("formatting: REFUSED — " + str(error))
    print("recording: " + str(args.output.resolve()))
    failed, unavailable = refused, False
    try:
        assert_evidence(args.output, min_activity=MIN_ACTIVITY)
        print("evidence: PASS")
    except AssertionError as error:
        failed = True
        print("evidence: FAILED\n" + str(error))
    except EvidenceReadError as error:
        unavailable = True
        print("evidence: UNAVAILABLE — " + error.reason)
    # This assertion still runs after an evidence failure.
    try:
        assert_application(peers.observations(), args.case)
        print("application: PASS")
    except AssertionError as error:
        failed = True
        print("application: FAILED — " + str(error))
    return 1 if unavailable else 2 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
