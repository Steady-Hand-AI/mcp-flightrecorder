"""Run a local OpenAI SDK → MCP tool → prepared answer roundtrip."""
from __future__ import annotations

import argparse
import asyncio
from contextlib import nullcontext
from copy import deepcopy
import json
from pathlib import Path

import httpx2
from mcp import Client
from mcp.server import Server
import mcp_types
from openai import AsyncOpenAI

from mcp_flightrecorder import Recorder
from mcp_flightrecorder.integrations.mcp_client import OperationEvidence, wrap_mcp
from mcp_flightrecorder.integrations.openai_chat import ModelEvidence, wrap_model


COVERAGE = {
    "producers": {"model": "roundtrip.model", "host": "roundtrip.host", "mcp": "roundtrip.mcp"},
    "model_complete": True,
}
MODEL = "local-example-model"
TOOLS = [{"type": "function", "function": {
    "name": "answer", "description": "Return the example's answer.",
    "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
}}]
QUESTION = "Use the answer tool, then report its result."


class LocalServices:
    """Owned SDK peers with observations taken independently of the recording."""

    def __init__(self):
        self.provider_requests = []
        self.provider_responses = []
        self.list_calls = 0
        self.tool_calls = []
        self.tool_results = []

    async def provider(self, request):
        if request.url.host != "roundtrip.invalid" or len(self.provider_requests) >= 2:
            raise RuntimeError("Unexpected example model request")
        self.provider_requests.append(json.loads(request.content))
        if len(self.provider_requests) == 1:
            message = {"role": "assistant", "content": None, "tool_calls": [{
                "id": "call_answer", "type": "function",
                "function": {"name": "answer", "arguments": "{}"},
            }]}
            finish_reason = "tool_calls"
        else:
            message = {"role": "assistant", "content": "The answer is 42."}
            finish_reason = "stop"
        response = {"id": f"completion_{len(self.provider_requests)}", "object": "chat.completion",
                    "created": 0, "model": MODEL,
                    "choices": [{"index": 0, "message": message, "finish_reason": finish_reason}]}
        self.provider_responses.append(deepcopy(response))
        return httpx2.Response(200, json=response)

    async def list_tools(self, context, params):
        self.list_calls += 1
        tool = TOOLS[0]["function"]
        return mcp_types.ListToolsResult(tools=[mcp_types.Tool(
            name=tool["name"], description=tool["description"], input_schema=deepcopy(tool["parameters"]))])

    async def call_tool(self, context, params):
        self.tool_calls.append({"name": params.name, "arguments": deepcopy(params.arguments)})
        if params.name != "answer" or params.arguments != {}:
            raise ValueError("The example tool accepts answer with empty arguments")
        result = mcp_types.CallToolResult(content=[mcp_types.TextContent(type="text", text="42")],
                                          is_error=False)
        self.tool_results.append(result.model_dump(mode="json", by_alias=True, exclude_unset=True))
        return result

    def observations(self):
        return deepcopy({"provider_requests": self.provider_requests, "provider_responses": self.provider_responses,
                         "list_calls": self.list_calls, "tool_calls": self.tool_calls, "tool_results": self.tool_results})


def admitted(receipt):
    return receipt is not None and receipt.status == "accepted"


def receipt_summary(receipt):
    if receipt is None:
        return {"status": "unavailable"}
    return {name: getattr(receipt, name) for name in
            ("status", "reason", "event_id", "turn_id", "model_call_id", "decision_id", "operation_id")}


def answer_annotations(decision, result):
    """Only this invocation's admitted decision and selected result can bind."""
    annotations = [{"bindings": [], "unresolved": [], "complete": True} for _ in range(3)]
    if admitted(decision) and admitted(result):
        annotations[2]["bindings"] = [{"decision_event_id": decision.event_id,
                                        "result_event_id": result.event_id,
                                        "pointer": "/content", "text_paths": [""]}]
    else:
        annotations[2] = {"bindings": None, "complete": False,
                          "unresolved": [{"pointer": "/content", "reason": "capture_unavailable"}]}
    return annotations


async def run_host(recorder: Recorder | None = None, *, services: LocalServices | None = None):
    """Run the native host flow. The caller owns the event loop and optional recorder."""
    services = LocalServices() if services is None else services
    server = Server("roundtrip-server", on_list_tools=services.list_tools, on_call_tool=services.call_tool)
    http = httpx2.AsyncClient(transport=httpx2.MockTransport(services.provider), trust_env=False)
    models = [ModelEvidence(), ModelEvidence()] if recorder is not None else []
    operation = OperationEvidence() if recorder is not None else None
    discovery_evidence = OperationEvidence() if recorder is not None else None
    decision = preparation = None
    turn_context = recorder.turn(QUESTION) if recorder is not None else nullcontext()
    async with AsyncOpenAI(api_key="local-example-placeholder", base_url="https://roundtrip.invalid/v1",
                           http_client=http, max_retries=0) as provider, Client(server) as native_client:
        create, client = provider.chat.completions.create, native_client
        if recorder is not None:
            create = wrap_model(recorder, create, instance_id="roundtrip-openai")
            client = wrap_mcp(recorder, native_client, client_id="roundtrip-client", server_key="roundtrip-server")
        with turn_context as turn:
            # Discovery also seeds the native client's tool-result validation cache.
            options = {"evidence": discovery_evidence} if recorder is not None else {}
            discovery = await client.list_tools(**options)
            tools = [{"type": "function", "function": {"name": tool.name, "description": tool.description,
                     "parameters": deepcopy(tool.input_schema)}} for tool in discovery.tools]
            messages = [{"role": "user", "content": QUESTION}]
            options = {"evidence": models[0]} if recorder is not None else {}
            first = await create(model=MODEL, messages=messages, tools=tools, temperature=0, **options)

            # The host selects from the native response, independently of capture admission.
            selected = first.choices[0].message.tool_calls[0]
            arguments = json.loads(selected.function.arguments)
            if recorder is not None:
                entry = next((item for item in models[0].decisions
                              if (item.choice_index, item.tool_index) == (0, 0)), None)
                decision = None if entry is None else entry.receipt
            options = {"evidence": operation} if recorder is not None else {}
            if admitted(decision):
                options["decision"] = decision
            result = await client.call_tool(selected.function.name, arguments, **options)
            raw_text = result.content[0].text
            prepared = f"Tool result: {raw_text}"
            assistant = first.choices[0].message.to_dict(mode="json", use_api_names=True, exclude_unset=True)
            messages.extend([assistant, {"role": "tool", "tool_call_id": selected.id, "content": prepared}])

            options = {}
            if recorder is not None:
                terminal = operation.terminal
                insertions = []
                if admitted(terminal):
                    preparation = recorder.observe("host.tool_result", scope=recorder.capture_scope(),
                        payload=prepared, facts={"text_paths": [""], "sources": [{
                            "event_id": terminal.event_id, "slot": "payload", "pointer": "/content/0/text",
                        }]})
                    if admitted(preparation):
                        insertions = [{"source_event_id": preparation.event_id,
                                       "target_pointer": "/messages/2/content", "content": prepared}]
                options = {"evidence": models[1], "annotations": answer_annotations(decision, terminal),
                           "insertions": insertions}
            second = await create(model=MODEL, messages=messages, tools=tools, temperature=0, **options)

    capture = {"status": "disabled", "run_id": None, "turn_id": None, "model_call_ids": []}
    if recorder is not None:
        receipts = [turn.receipt, turn.exit_receipt, decision, operation.start, operation.terminal, preparation,
                    discovery_evidence.start, discovery_evidence.terminal]
        receipts.extend(receipt for item in models for receipt in (item.request, item.response))
        available = all(admitted(receipt) for receipt in receipts) and not any(
            item.losses for item in [*models, operation, discovery_evidence])
        capture = {"status": "accepted" if available else "unavailable", "run_id": recorder.capture_scope().run_id,
                   "turn_id": turn.receipt.turn_id,
                   "model_call_ids": [item.request.model_call_id if admitted(item.request) else None for item in models],
                   "turn": receipt_summary(turn.receipt), "turn_exit": receipt_summary(turn.exit_receipt),
                   "models": [{"request": receipt_summary(item.request), "response": receipt_summary(item.response),
                               "losses": list(item.losses)} for item in models],
                   "decision": receipt_summary(decision), "preparation": receipt_summary(preparation),
                   "discovery": {"start": receipt_summary(discovery_evidence.start),
                                 "terminal": receipt_summary(discovery_evidence.terminal),
                                 "losses": list(discovery_evidence.losses)},
                   "operation": {"start": receipt_summary(operation.start), "terminal": receipt_summary(operation.terminal),
                                 "losses": list(operation.losses)}}
    return {"host": {"discovery": discovery.model_dump(mode="json", by_alias=True, exclude_unset=True),
                     "selected_tool": {"name": selected.function.name, "arguments": arguments},
                     "raw_tool_text": raw_text, "prepared_text": prepared, "answer": second.choices[0].message.content},
            "backend": services.observations(), "capture": capture}


async def record_roundtrip(output):
    """Construct and close the recorder within the established async worker."""
    recorder = Recorder(output, capture="content", comparisons=True, coverage=COVERAGE)
    try:
        summary = await run_host(recorder)
    finally:
        closed = await recorder.aclose(timeout=5)
    summary["close"] = {name: getattr(closed, name) for name in
                        ("writer_stopped", "portable", "run_close_state", "reason")}
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--output", type=Path, help="New recording path; an existing path is refused")
    mode.add_argument("--no-capture", action="store_true", help="Run the same local host without a recorder")
    args = parser.parse_args()
    summary = asyncio.run(run_host() if args.no_capture else record_roundtrip(args.output))
    print(json.dumps(summary, indent=2))
    if not args.no_capture and not (summary["close"]["writer_stopped"] and summary["close"]["portable"]):
        raise SystemExit("Recording did not close portably; inspect the close summary")


if __name__ == "__main__":
    main()
