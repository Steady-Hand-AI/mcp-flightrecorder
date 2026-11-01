"""Create the deterministic SDK integration recording used by the tests."""
from __future__ import annotations

import argparse
import asyncio
from contextlib import nullcontext
from copy import deepcopy
from importlib.metadata import version
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
    "producers": {"model": "fixture.model", "host": "fixture.host", "mcp": "fixture.mcp"},
    "model_complete": True,
}
BOUNDARIES = {
    "model": "submitted SDK body arguments and consumed SDK results/chunks",
    "mcp": "logical Client.list_tools and Client.call_tool arguments/results",
    "provider_backend": "scripted_sdk_transport",
    "mcp_backend": "in_process_sdk_server",
    "unobserved": ["wire requests", "hidden SDK retries/rounds", "notifications", "cache source"],
    "outside_coverage": ["live provider", "HTTP/stdio MCP transports", "other SDKs/platforms"],
}
INPUT_FILES = {
    "messages": "messages.json", "order": "request-order.json",
    "stream": "provider-stream.json", "normal": "provider-normal.json",
    "tool": "mcp-tool.json",
}


def read_inputs(directory):
    """Read the supplied service scripts and host conversation, without a database."""
    return {name: json.loads((Path(directory) / filename).read_text())
            for name, filename in INPUT_FILES.items()}


def sdk_versions():
    return {name: version(name) for name in ("openai", "mcp", "mcp-types", "httpx2")}


def environment():
    return {"fixture": "phase2_integration", "provider_backend": "scripted_sdk_transport",
            "mcp_backend": "in_process_sdk_server", "sdk_versions": sdk_versions(),
            "capture_boundaries": deepcopy(BOUNDARIES)}


def accepted(receipt):
    """The canonical fixture needs admitted identities before supplying explicit links."""
    if receipt is None or receipt.status != "accepted":
        reason = None if receipt is None else receipt.reason
        raise RuntimeError(f"canonical fixture prerequisite was not admitted: {reason}")
    return receipt


def native_model(value):
    return value.to_dict(mode="json", use_api_names=True, exclude_unset=True)


def native_mcp(value):
    return value.model_dump(mode="json", by_alias=True, exclude_unset=True)


class ScriptedServices:
    """The host-owned test peers expose their real requests and deterministic gates."""

    def __init__(self, inputs):
        self.inputs = inputs
        self.provider_requests = []
        self.list_calls = 0
        self.tool_calls = []
        self.order = []
        self.entered = {name: asyncio.Event() for name in ("alpha", "beta")}
        self.release = {name: asyncio.Event() for name in ("alpha", "beta")}

    async def provider(self, request):
        if request.url.host != "fixture.invalid":
            raise RuntimeError("the canonical provider transport accepts fixture.invalid only")
        index = len(self.provider_requests)
        self.provider_requests.append(json.loads(request.content))
        if index == 0:
            data = "".join("data: " + json.dumps(chunk) + "\n\n" for chunk in self.inputs["stream"])
            return httpx2.Response(200, headers={"content-type": "text/event-stream"},
                                   content=(data + "data: [DONE]\n\n").encode("utf-8"))
        if index > 3:
            raise RuntimeError("the canonical host made an unexpected fifth provider request")
        return httpx2.Response(200, json=self.inputs["normal"][f"W{index}"])

    async def list_tools(self, context, params):
        self.list_calls += 1
        tool = self.inputs["tool"]
        declaration = mcp_types.Tool(name=tool["name"], description=tool["description"],
                                     input_schema=deepcopy(tool["inputSchema"]))
        return mcp_types.ListToolsResult(tools=[declaration])

    async def call_tool(self, context, params):
        index = len(self.tool_calls)
        if index > 1:
            raise RuntimeError("the canonical host made an unexpected third tool call")
        label = ("alpha", "beta")[index]
        self.tool_calls.append({"name": params.name, "arguments": deepcopy(params.arguments)})
        self.order.append(f"enter:{label}")
        self.entered[label].set()
        await asyncio.wait_for(self.release[label].wait(), timeout=10)
        result = mcp_types.CallToolResult(
            content=[mcp_types.TextContent(type="text", text=params.arguments["text"])],
            is_error=False,
        )
        self.order.append(f"return:{label}")
        return result

    def release_result(self, label):
        self.order.append(f"release:{label}")
        self.release[label].set()

    def observations(self):
        return {"provider_requests": deepcopy(self.provider_requests),
                "mcp_list_calls": self.list_calls, "mcp_tool_calls": deepcopy(self.tool_calls),
                "operation_control_order": list(self.order)}


def provider_catalog(discovery):
    """Build the submitted provider catalog from the actual discovery result."""
    return [{"type": "function", "function": {
        "name": tool.name, "description": tool.description,
        "parameters": deepcopy(tool.input_schema),
    }} for tool in discovery.tools]


def append_fragments(calls, choice):
    """Host-side assembly uses consumed SDK positions, independently of recorder receipts."""
    for fragment in choice.delta.tool_calls or ():
        key = (choice.index, fragment.index)
        call = calls.setdefault(key, {"id": "", "type": "function",
                                      "function": {"name": "", "arguments": ""}})
        if fragment.id is not None:
            call["id"] += fragment.id
        if fragment.function is not None:
            for field in ("name", "arguments"):
                value = getattr(fragment.function, field)
                if value is not None:
                    call["function"][field] += value


async def consume_decisions(stream):
    calls, chunks = {}, []
    try:
        async for chunk in stream:
            chunks.append(native_model(chunk))
            for choice in chunk.choices:
                append_fragments(calls, choice)
    finally:
        await stream.aclose()
    assistant = {"role": "assistant", "content": None,
                 "tool_calls": [call for key, call in calls.items() if key[0] == 0]}
    return assistant, chunks


def remember_model(symbols, evidence, request_name, response_name, call_name):
    if evidence is None:
        return
    request, response = accepted(evidence.request), accepted(evidence.response)
    symbols.update({request_name: request.event_id, response_name: response.event_id,
                    call_name: request.model_call_id})


def remember_operation(symbols, evidence, start_name, end_name, operation_name):
    if evidence is None:
        return
    start, terminal = accepted(evidence.start), accepted(evidence.terminal)
    symbols.update({start_name: start.event_id, end_name: terminal.event_id,
                    operation_name: start.operation_id})


def message_annotations(order, decisions, results):
    annotations = []
    for name in order:
        bindings = []
        if name in ("B1", "B2"):
            index = int(name[-1]) - 1
            bindings.append({"decision_event_id": accepted(decisions[index]).event_id,
                             "result_event_id": accepted(results[index].terminal).event_id,
                             "pointer": "/content", "text_paths": [""]})
        annotations.append({"bindings": bindings, "unresolved": [], "complete": True})
    return annotations


async def submit_model(create, inputs, messages, tools, symbol, *, recorder=None,
                       decisions=(), results=(), insertion=()):
    order = inputs["order"][symbol]
    kwargs = {"model": "fixture-model", "temperature": 0, "n": 1,
              "messages": [deepcopy(messages[name]) for name in order],
              "tools": deepcopy(tools), "stream": symbol == "R0"}
    if symbol == "R0":
        kwargs["stream_options"] = {"include_usage": True}
    if recorder is None:
        return await create(**kwargs), None
    evidence = ModelEvidence()
    result = await create(evidence=evidence,
                          annotations=message_annotations(order, decisions, results),
                          insertions=insertion, **kwargs)
    return result, evidence


async def execute_tools(recorder, client, services, assistant, decisions, symbols):
    """Gate both real server calls, then release beta before alpha."""
    calls = assistant["tool_calls"]
    arguments = [json.loads(call["function"]["arguments"]) for call in calls]
    arguments[0] = {"text": arguments[0]["text"].replace("-selected", "-executed")}
    evidence = [OperationEvidence(), OperationEvidence()] if recorder is not None else [None, None]
    tasks = []
    try:
        for index, label in enumerate(("alpha", "beta")):
            context = (recorder.dispatch(action="execute", name=calls[index]["function"]["name"],
                                        arguments=arguments[index],
                                        decision_event_ids=[accepted(decisions[index]).event_id])
                       if recorder is not None else nullcontext())
            with context as dispatch:
                options = {} if recorder is None else {"evidence": evidence[index], "decision": decisions[index]}
                if recorder is not None:
                    symbols[f"H{index + 1}"] = accepted(dispatch.receipt).event_id
                tasks.append(asyncio.create_task(client.call_tool(
                    calls[index]["function"]["name"], arguments[index], **options)))
                await asyncio.wait_for(services.entered[label].wait(), timeout=10)
        overlap = {"entered": [label for label in ("alpha", "beta") if services.entered[label].is_set()],
                   "released": [label for label in ("alpha", "beta") if services.release[label].is_set()]}
        services.release_result("beta")
        beta = await asyncio.wait_for(tasks[1], timeout=10)
        services.release_result("alpha")
        alpha = await asyncio.wait_for(tasks[0], timeout=10)
        for index in range(2):
            remember_operation(symbols, evidence[index], f"S{index + 1}", f"E{index + 1}", f"O{index + 1}")
        return (alpha, beta), evidence, overlap
    finally:
        for gate in services.release.values():
            gate.set()
        if tasks:
            await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=10)


def supplied_results(messages, assistant, results):
    alpha, beta = (result.content[0].text for result in results)
    messages["A"] = assistant
    messages["B1"] = {"role": "tool", "tool_call_id": assistant["tool_calls"][0]["id"], "content": alpha}
    messages["B2"] = {"role": "tool", "tool_call_id": assistant["tool_calls"][1]["id"], "content": beta + "-submitted"}
    prepared = f"Results: {alpha}, {beta}."
    messages["S"] = {"role": "system", "content": prepared}
    return prepared


def prepare_summary(recorder, symbols, prepared):
    if recorder is None:
        return ()
    receipt = accepted(recorder.observe("host.tool_result", scope=recorder.capture_scope(), payload=prepared,
                                      facts={"sources": [{"event_id": symbols[name], "slot": "payload",
                                                          "pointer": "/content/0/text"} for name in ("E1", "E2")],
                                             "text_paths": [""]}))
    symbols["P"] = receipt.event_id
    return [{"source_event_id": receipt.event_id, "target_pointer": "/messages/5/content", "content": prepared}]


async def first_turn(recorder, create, client, services, inputs, messages, host, symbols):
    context = (recorder.turn(messages["U"]["content"], conversation_id="fixture-conversation",
                             conditions={"case": "phase2_integration", "variant": "parallel_rewrite"})
               if recorder is not None else nullcontext())
    with context as turn:
        if recorder is not None:
            symbols["T1"] = accepted(turn.receipt).turn_id
        discovery_evidence = OperationEvidence() if recorder is not None else None
        discovery = await client.list_tools(**({"evidence": discovery_evidence} if recorder is not None else {}))
        remember_operation(symbols, discovery_evidence, "L0", "L1", "O0")
        tools = provider_catalog(discovery)
        host.update({"discovery": native_mcp(discovery), "provider_catalog": tools})
        stream, evidence = await submit_model(create, inputs, messages, tools, "R0", recorder=recorder)
        assistant, chunks = await consume_decisions(stream)
        host.update({"assembled_assistant": assistant, "stream_chunks": chunks})
        remember_model(symbols, evidence, "R0", "Z0", "M0")
        decisions = ()
        if recorder is not None:
            entries = evidence.decisions
            if [(entry.choice_index, entry.tool_index) for entry in entries] != [(0, 0), (0, 1)]:
                raise RuntimeError("canonical stream did not expose its two ordered decision receipts")
            decisions = tuple(accepted(entry.receipt) for entry in entries)
            for index, decision in enumerate(decisions, 1):
                symbols.update({f"D{index}": decision.event_id, f"delta{index}": decision.decision_id})
        results, operation_evidence, overlap = await execute_tools(
            recorder, client, services, assistant, decisions, symbols)
        host.update({"tool_results": [native_mcp(result) for result in results], "overlap_before_release": overlap})
        prepared = supplied_results(messages, assistant, results)
        host["prepared_summary"] = prepared
        host["summary_source_order"] = ["alpha", "beta"]
        insertion = prepare_summary(recorder, symbols, prepared)
        response, evidence = await submit_model(create, inputs, messages, tools, "W1", recorder=recorder,
                                                decisions=decisions, results=operation_evidence, insertion=insertion)
        remember_model(symbols, evidence, "W1", "F1", "M1")
        host["responses"] = {"W1": native_model(response)}
        messages["F1m"] = native_model(response.choices[0].message)
    if recorder is not None:
        accepted(turn.exit_receipt)
    return tools, decisions, operation_evidence


async def retained_turn(recorder, create, inputs, messages, tools, decisions, results, host, symbols):
    context = (recorder.turn(messages["U2"]["content"], conversation_id="fixture-conversation",
                             conditions={"case": "phase2_integration", "variant": "history_no_tool"})
               if recorder is not None else nullcontext())
    with context as turn:
        if recorder is not None:
            symbols["T2"] = accepted(turn.receipt).turn_id
        for index in (2, 3):
            response, evidence = await submit_model(create, inputs, messages, tools, f"W{index}", recorder=recorder,
                                                    decisions=decisions, results=results)
            remember_model(symbols, evidence, f"W{index}", f"F{index}", f"M{index}")
            host["responses"][f"W{index}"] = native_model(response)
            messages[f"F{index}m"] = native_model(response.choices[0].message)
    if recorder is not None:
        accepted(turn.exit_receipt)


async def run_host(inputs: Path, recorder: Recorder | None = None):
    """Run identical native host work; the caller owns the event loop and recorder."""
    data = read_inputs(inputs)
    services = ScriptedServices(data)
    server = Server("fixture-server", on_list_tools=services.list_tools, on_call_tool=services.call_tool)
    http = httpx2.AsyncClient(transport=httpx2.MockTransport(services.provider), trust_env=False)
    summary = {"symbols": {}, "sdk_versions": sdk_versions(), "boundaries": deepcopy(BOUNDARIES), "host": {}}
    symbols = summary["symbols"]
    if recorder is not None:
        symbols["R"] = recorder.capture_scope().run_id
    async with AsyncOpenAI(api_key="fixture-only-not-a-secret", base_url="https://fixture.invalid/v1",
                           http_client=http) as provider, Client(server) as native_client:
        create = provider.chat.completions.create
        client = native_client
        if recorder is not None:
            create = wrap_model(recorder, create, instance_id="fixture-openai")
            client = wrap_mcp(recorder, native_client, client_id="fixture-client", server_key="fixture-server")
        messages = deepcopy(data["messages"])
        tools, decisions, results = await first_turn(
            recorder, create, client, services, data, messages, summary["host"], symbols)
        await retained_turn(recorder, create, data, messages, tools, decisions, results, summary["host"], symbols)
    summary["backend"] = services.observations()
    return summary


async def record_fixture(output, inputs):
    recorder = Recorder(output, capture="content", comparisons=True, coverage=COVERAGE, environment=environment())
    try:
        summary = await run_host(inputs, recorder)
    finally:
        closed = await recorder.aclose(timeout=5)
    summary["close"] = {key: getattr(closed, key) for key in
                        ("writer_stopped", "run_close_state", "portable", "reason")}
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--inputs", type=Path, required=True)
    args = parser.parse_args()
    summary = asyncio.run(asyncio.wait_for(record_fixture(args.output, args.inputs), timeout=40))
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
