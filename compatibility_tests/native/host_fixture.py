"""Installed ollmcp with packaged observers at its actual two SDK seams.

The test host retains its native parser, sessions, transports and cleanup. The
only additional host observations are original turn input, execute/refuse,
positively witnessed cache getters and the explicit resource-read operation.
There is no parser correspondence or result-to-decision reconstruction.
"""

import asyncio
from contextlib import nullcontext
from dataclasses import asdict
from functools import wraps
import inspect
import io
import json
from pathlib import Path
import sys
from unittest.mock import patch

from rich.console import Console
from mcp_client_for_ollama.client import MCPClient
from mcp_client_for_ollama.resources.parser import extract_resource_refs
from mcp_client_for_ollama.utils import server_logs
from mcp_flightrecorder import Recorder
from mcp_flightrecorder.integrations.anyllm_ollama import ModelEvidence, wrap_model
from mcp_flightrecorder.integrations.mcp_session import OperationEvidence, wrap_session

from .host_endpoint import OllamaEndpoint, copied
from .host_evidence import inspect_recording, receipt_dict


PROMPT = "Use the available tools with integer 7; preserve µ in the result."
SYSTEM = "You are the independent host integration fixture."
OPTIONS = {"num_ctx": 4096, "top_k": 37, "num_predict": 96}


def dumped(value):
    return value.model_dump(mode="json", by_alias=True, exclude_unset=True)


class Observation:
    """Test-owned native ledgers and explicitly admitted host receipts."""
    def __init__(self, recorder):
        self.recorder = recorder
        self.requests, self.protocol, self.receipts = [], [], []
        self.models, self.operations = [], []
        self.cache_reads = []

    def remember(self, receipt):
        self.receipts.append(receipt_dict(receipt))
        return receipt

    def cached_tools(self, values):
        body = [dumped(tool) for tool in values]
        self.cache_reads.append(body)
        if self.recorder is not None:
            self.remember(self.recorder.observe("cache.served", scope=self.recorder.capture_scope(), payload=body,
                facts={"owner": "host", "client_id": "ollmcp-tool-manager",
                       "cache_id": "available_tools", "cache_scope": "host_instance"}))

    def evidence(self):
        models = []
        operations = []
        for item in self.models:
            models.append({"request": receipt_dict(item.request), "response": receipt_dict(item.response),
                           "losses": item.losses, "decisions": [
                               {"choice_index": value.choice_index, "tool_index": value.tool_index,
                                "provider_ref": value.provider_ref, "receipt": receipt_dict(value.receipt)}
                               for value in item.decisions]})
        for item in self.operations:
            operations.append({"start": receipt_dict(item.start), "terminal": receipt_dict(item.terminal),
                               "losses": item.losses})
        return models, operations


def attach_session(session, server_name, state):
    """Decorate two bound methods; the actual ClientSession stays host-owned."""
    native_list, native_call = session.list_tools, session.call_tool
    call_signature = inspect.signature(native_call)

    @wraps(native_list)
    async def listed(*args, **kwargs):
        value = await native_list(*args, **kwargs)
        params = kwargs.get("params")
        state.protocol.append({"method": "tools/list", "server": server_name, "name": None,
                               "arguments": {"params": None if params is None else dumped(params)},
                               "result": dumped(value)})
        return value

    @wraps(native_call)
    async def called(*args, **kwargs):
        supplied = call_signature.bind(*args, **kwargs)
        supplied.apply_defaults()
        value = await native_call(*args, **kwargs)
        state.protocol.append({"method": "tools/call", "server": server_name,
                               "name": supplied.arguments["name"],
                               "arguments": copied(supplied.arguments["arguments"]), "result": dumped(value)})
        return value

    session.list_tools, session.call_tool = listed, called
    if state.recorder is not None:
        adapter = wrap_session(state.recorder, session, client_id="ollmcp-session-" + server_name,
                               server_key=server_name)

        @wraps(native_list)
        def observed_list(*args, **kwargs):
            evidence = OperationEvidence()
            state.operations.append(evidence)
            return adapter.list_tools(*args, evidence=evidence, **kwargs)

        @wraps(native_call)
        async def observed_call(*args, **kwargs):
            supplied = call_signature.bind(*args, **kwargs)
            supplied.apply_defaults()
            name, arguments = supplied.arguments["name"], supplied.arguments["arguments"]
            qualified = server_name + "." + name
            evidence = OperationEvidence()
            state.operations.append(evidence)
            with state.recorder.dispatch(action="execute", name=qualified, arguments=arguments) as dispatch:
                state.remember(dispatch.receipt)
                return await adapter.call_tool(*args, evidence=evidence, **kwargs)

        session.list_tools, session.call_tool = observed_list, observed_call
    attach_resource_read(session, server_name, state)


def attach_resource_read(session, server_name, state):
    """The existing explicit core hook witnesses resource reads, not insertions."""
    native = session.read_resource

    @wraps(native)
    async def read(uri, *args, **kwargs):
        start = None
        if state.recorder is not None:
            start = state.remember(state.recorder.start_operation(scope=state.recorder.capture_scope(),
                client_id="ollmcp-resource-" + server_name, server_key=server_name,
                method="resources/read", arguments={"uri": str(uri)}))
        try:
            result = await native(uri, *args, **kwargs)
        except BaseException as error:
            if start is not None and start.ticket is not None:
                state.remember(state.recorder.finish_operation(start.ticket,
                    status="cancelled" if isinstance(error, asyncio.CancelledError) else "local_error",
                    result={"error": {"type": type(error).__name__}}, complete=False))
            raise
        body = dumped(result)
        state.protocol.append({"method": "resources/read", "server": server_name, "name": None,
                               "arguments": {"uri": str(uri)}, "result": body})
        if start is not None and start.ticket is not None:
            state.remember(state.recorder.finish_operation(start.ticket, status="ok", result=body,
                                                          resolution="unknown"))
        return result

    session.read_resource = read


def attach_host_hooks(client, state):
    native_completion = client.llm.acompletion
    native_tools = client.tool_manager.get_enabled_tool_objects
    native_listing = client.server_connector._list_capabilities

    @wraps(native_completion)
    async def native_completion_ledger(*args, **kwargs):
        assert not args, "This installed host explicitly submits keyword arguments at its AnyLLM boundary."
        state.requests.append(copied(kwargs))
        return await native_completion(*args, **kwargs)

    if state.recorder is None:
        client.llm.acompletion = native_completion_ledger
    else:
        create = wrap_model(state.recorder, native_completion_ledger, instance_id="ollmcp-anyllm-ollama")

        @wraps(native_completion)
        def completion(*args, **kwargs):
            evidence = ModelEvidence()
            state.models.append(evidence)
            # No annotations/insertions: the stock host exposes no receipt seam.
            return create(*args, evidence=evidence, **kwargs)

        client.llm.acompletion = completion

    def cached_tools():
        values = native_tools()
        state.cached_tools(values)
        return values

    async def listing(session, server_name, *args, **kwargs):
        attach_session(session, server_name, state)
        return await native_listing(session, server_name, *args, **kwargs)

    client.tool_manager.get_enabled_tool_objects = cached_tools
    client.server_connector._list_capabilities = listing


def server_configs(directory, scenario):
    choices = [("review", {"tool_error": "positive", "structured": "structured", "empty_alias": "empty"}.get(scenario, "echo"))]
    if scenario == "collision":
        choices = [("alpha", "beta_echo"), ("alpha_beta", "echo")]
    fixture = str(Path(__file__).with_name("host_server.py").resolve())
    return {label: {"command": sys.executable, "args": ["-I", "-B", fixture, "--label", label,
              "--tool", tool, "--log", str((directory / (label + "-server.jsonl")).resolve())]}
            for label, tool in choices}


def make_client(endpoint, output, scenario):
    client = MCPClient(model="review-model", host=endpoint.url, provider="ollama", persist_api_key=False)
    console = Console(file=output, width=180, force_terminal=False)
    for instance in (client, client.server_connector, client.model_manager, client.model_config_manager,
                     client.tool_manager, client.prompt_manager, client.prompt_handler, client.resource_manager,
                     client.resource_handler, client.streaming_manager, client.tool_display_manager, client.hil_manager):
        instance.console = console
    client.model_config_manager.system_prompt = SYSTEM
    if scenario == "options":
        for key, value in OPTIONS.items():
            setattr(client.model_config_manager, key, value)
    client.answer_render_mode = "plain"
    client.monitor_paused_ack.set()
    client.hil_manager.set_session_auto_execute(True)
    return client


def configure_skip(client, state):
    async def decline(name, arguments):
        if state.recorder is not None:
            with state.recorder.dispatch(action="refuse", name=name, arguments=arguments) as dispatch:
                state.remember(dispatch.receipt)
        return False
    client.hil_manager.request_tool_confirmation = decline


def make_recorder(directory, scenario, capture):
    return Recorder(directory / "recording.sqlite", capture=capture,
        coverage={"producers": {"model": "anyllm-ollama-adapter", "host": "explicit-test-host-hooks",
                                 "mcp": "mcp-session-adapter"}, "model_complete": True},
        environment={"scenario": scenario, "fixture_kind": "installed_native_host",
                     "llm": "owned loopback Ollama NDJSON", "host": "ollmcp 0.35.1",
                     "dispatch_and_result_receipts": "unexposed",
                     "boundary": "submitted AnyLLM arguments and consumed normalized SDK output"})


async def execute_queries(client, state, scenario):
    outputs = []
    turn_prompt = PROMPT + " @review://context" if scenario == "resource" else PROMPT
    for index in range(2 if scenario == "repeated" else 1):
        context = (nullcontext() if state.recorder is None else
                   state.recorder.turn(turn_prompt, conditions={"scenario": scenario, "query_index": index}))
        with context as turn:
            if turn is not None:
                state.remember(turn.receipt)
            if scenario == "resource":
                query, refs = extract_resource_refs(turn_prompt, {"review://context"})
                client._process_query_with_monitoring = client.process_query
                await client._handle_inline_resources(query, refs)
                outputs.append(client.chat_history[-1]["response"])
            else:
                outputs.append(await client.process_query(PROMPT))
    return outputs


async def native_run(directory, scenario, capture=None):
    directory.mkdir(parents=True, exist_ok=False)
    output = io.StringIO()
    recorder = None if capture is None else make_recorder(directory, scenario, capture)
    state = Observation(recorder)
    closed = None
    with OllamaEndpoint(scenario) as endpoint:
        client = make_client(endpoint, output, scenario)
        attach_host_hooks(client, state)
        if scenario == "skip":
            configure_skip(client, state)
        try:
            with patch.object(server_logs, "SERVER_LOG_DIR", str(directory / "native-server-logs")):
                await client.connect_to_servers(server_configs=server_configs(directory, scenario))
                assert client.server_connector.sessions, "The owned real stdio server must connect before a native-host scenario can be qualified."
                outputs = await execute_queries(client, state, scenario)
        finally:
            try:
                await client.cleanup()
            finally:
                await client.llm.client._client.aclose()
                if recorder is not None:
                    closed = await recorder.aclose(timeout=5)
                (directory / "host-console.txt").write_text(output.getvalue(), encoding="utf-8")
    report = {"capture": capture, "outputs": outputs,
              "wire": endpoint.requests, "history": copied(client.chat_history),
              "requests": state.requests, "protocol": state.protocol,
              "cache_reads": state.cache_reads,
              "server_events": [json.loads(line) for path in sorted(directory.glob("*-server.jsonl"))
                                for line in path.read_text().splitlines()]}
    if recorder is not None:
        models, operations = state.evidence()
        report.update(model_evidence=models, operation_evidence=operations, receipts=state.receipts,
                      close=asdict(closed))
        report.update(inspect_recording(directory / "recording.sqlite", capture=capture))
    return report


async def run_pair(directory, scenario, capture="content"):
    plain = await native_run(directory / "plain", scenario)
    recorded = await native_run(directory / "recorded", scenario, capture)
    return plain, recorded
