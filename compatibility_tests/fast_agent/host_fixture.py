"""Owned HTTP/stdio controls for the actual installed fast-agent lifecycle.

The test counts client construction around each returned LLM instance. It does
not patch an MCP constructor, provider module, class, or model factory.
"""
from contextlib import contextmanager, nullcontext
import dataclasses
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import json
from pathlib import Path
import sqlite3
import sys
import threading

from fast_agent.agents.agent_types import AgentConfig
from fast_agent.agents.mcp_agent import McpAgent
from fast_agent.config import Settings
from fast_agent.context import cleanup_context, initialize_context
from fast_agent.llm.model_factory import ModelFactory
from fast_agent.llm.request_params import RequestParams
from mcp_flightrecorder import Recorder, inspect


ROOT = Path(__file__).resolve().parents[2]
BASE_MODEL = "gpt-4.1"
CLONE_MODEL = "gpt-4.1-mini"
_spec = importlib.util.spec_from_file_location("mfr_fast_agent_example", ROOT / "examples/fast_agent.py")
example = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(example)


def dump(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n")


def copied(value):
    return json.loads(json.dumps(value, ensure_ascii=False))


def history_without_runtime_timing(outcome):
    """Compare native content, retaining raw wall-clock/timing data in evidence."""
    result = copied(outcome)
    for message in result["history"]:
        message.pop("timestamp", None)
        channels = message.get("channels")
        if channels is not None:
            message["channels"] = {key: value for key, value in channels.items()
                                   if key not in {"fast-agent-timing", "fast-agent-tool-timing"}}
    return result


def chunk(model, delta, finish=None):
    return {"id": "owned-chat", "object": "chat.completion.chunk", "created": 1,
            "model": model, "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]}


def scripted_chunks(body, index):
    model = body["model"]
    if index % 2:
        return [chunk(model, {"role": "assistant", "content": "owned-complete"}), chunk(model, {}, "stop")]
    name = next(tool["function"]["name"] for tool in body["tools"] if tool["function"]["name"].endswith("echo"))
    return [chunk(model, {"role": "assistant", "tool_calls": [{"index": 0,
                "id": "owned-call-" + str(index), "type": "function",
                "function": {"name": name, "arguments": '{"value":'}}]}),
            chunk(model, {"tool_calls": [{"index": 0, "function": {"arguments": '"same-α"}'}}]}),
            chunk(model, {}, "tool_calls")]


@contextmanager
def model_endpoint(ledger):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            chunks = scripted_chunks(body, len(ledger))
            ledger.append({"path": self.path, "body": body, "chunks": copied(chunks)})
            payload = b"".join(("data: " + json.dumps(item, ensure_ascii=False) + "\n\n").encode() for item in chunks)
            payload += b"data: [DONE]\n\n"
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(payload)
            self.wfile.flush()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}/v1"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
        if thread.is_alive():
            raise RuntimeError("The owned model HTTP server did not stop within its cleanup bound.")


def counted_factory(recorder, model_spec, ledger, clients):
    native = (example.make_recorded_llm_factory(recorder, model_spec, instance_prefix="owned-model")
              if recorder is not None else ModelFactory.create_factory(model_spec))

    def factory(*args, **kwargs):
        llm = native(*args, **kwargs)
        original = llm._openai_client

        def construct():
            client = original()
            clients.append(client)
            ledger.append({"ordinal": len(clients), "llm_type": type(llm).__qualname__,
                           "client_type": type(client).__qualname__})
            return client

        llm._openai_client = construct
        return llm

    return factory


def settings_for(url, path):
    return Settings(_env_file=None, openai={"api_key": "owned-loopback-only", "base_url": url},
        session_history=False, otel={"enabled": False}, skills={"directories": []},
        logger={"type": "none", "show_chat": False, "show_tools": False, "progress_display": False},
        mcp={"servers": {"owned": {"command": sys.executable,
            "args": ["-I", str(Path(__file__).with_name("owned_mcp_server.py")), str(path / "server.jsonl")],
            "cwd": str(path), "ping_interval_seconds": 0, "read_timeout_seconds": 15}}})


async def native_turn(route, path, recorder, url, ledgers, clients, sends):
    context = await initialize_context(settings_for(url, path))
    owner_params = RequestParams(max_iterations=5, parallel_tool_calls=True)
    config = AgentConfig(name="owned-native", instruction="Use the supplied fixture tools.", servers=["owned"],
                         skills=[], shell=False, cwd=path, default_request_params=owner_params)
    agent = McpAgent(config, context=context)
    clone = None
    try:
        await agent.initialize()
        await agent.attach_llm(counted_factory(recorder, BASE_MODEL, ledgers["client_construction"], clients),
                               model=BASE_MODEL, request_params=owner_params)
        if route == "detached":
            clone = await agent.spawn_detached_instance(name="owned-clone")
        elif route in {"reattached", "unreattached"}:
            clone = await agent.spawn_isolated_instance(name="owned-model-clone", model=CLONE_MODEL)
            if route == "reattached":
                # The host retains and supplies its choices; no private clone state is read.
                await clone.attach_llm(counted_factory(recorder, CLONE_MODEL, ledgers["client_construction"], clients),
                                       model=CLONE_MODEL, request_params=owner_params)
        target = clone or agent
        outputs = []
        for index in range(sends):
            scope = (recorder.turn("Use owned fixture.", conditions={"route": route, "send": index})
                     if recorder is not None else nullcontext())
            with scope:
                outputs.append(await target.send("Use owned fixture."))
        return {"results": outputs,
                "history": [message.model_dump(mode="json", by_alias=True) for message in target.message_history]}
    finally:
        if clone is not None:
            await clone.shutdown()
        await agent.shutdown()
        await cleanup_context()


def inspect_database(path):
    with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as connection:
        connection.row_factory = sqlite3.Row
        result = {table: [dict(row) for row in connection.execute("SELECT * FROM " + table)]
                  for table in ("runs", "turns", "events", "operations", "links", "artifacts", "request_members")}
        result["events"].sort(key=lambda row: row["sequence"])
        result["integrity_check"] = connection.execute("PRAGMA integrity_check").fetchone()[0]
        result["foreign_key_check"] = [list(row) for row in connection.execute("PRAGMA foreign_key_check")]
    run_id = result["runs"][0]["run_id"]
    result["reader_check"] = inspect.check(path, run_id, expect=("model",))
    result["reader_turns"] = inspect.find_turns(path, run_id)
    result["reader_explain"] = [inspect.explain(path, run_id, row["turn_id"]) for row in result["turns"]]
    return result


def run_case(route, path, observed, run_async, *, sends=1):
    path.mkdir(parents=True)
    ledgers = {"http": [], "client_construction": []}
    clients = []
    recorder = None
    if observed:
        recorder = Recorder(path / "capture.sqlite", capture="content", coverage={
            "producers": {"model": "fast-agent.openai_chat", "host": "owned.turn-scope", "mcp": None},
            "model_complete": route != "unreattached"},
            environment={"recipe": "fast_agent", "route": route, "fixture": "owned HTTP and stdio"})
    try:
        with model_endpoint(ledgers["http"]) as url:
            outcome = run_async(native_turn(route, path, recorder, url, ledgers, clients, sends))
    finally:
        ledgers["clients_closed"] = [client.is_closed() for client in clients]
        ledgers["distinct_client_instances"] = len({id(client) for client in clients})
        if recorder is not None:
            dump(path / "close.json", dataclasses.asdict(recorder.close(timeout=5)))
            dump(path / "database.json", inspect_database(path / "capture.sqlite"))
    return outcome, ledgers


def reconstructed_requests(database, requests):
    """Reconstruct the specified public request-manifest fields independently."""
    artifacts = {row["artifact_id"]: json.loads(row["body_text"])
                 for row in database["artifacts"] if row["body_text"] is not None}
    reconstructed = []
    for event in requests:
        manifest = artifacts[event["payload_ref"]]
        body = copied(artifacts[manifest["template_ref"]])
        body["messages"] = [copied(artifacts[ref]) for ref in manifest["message_refs"]]
        for reference in manifest["field_refs"]:
            assert reference["pointer"] == "/tools", "This fixture's only separate catalogue projection is the top-level tools field."
            body["tools"] = copied(artifacts[reference["artifact_id"]])
        reconstructed.append(body)
    return reconstructed
