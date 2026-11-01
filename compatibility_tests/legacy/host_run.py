"""Run the actual tagged host via public base_url and session-init hook only."""
import argparse
import asyncio
from contextlib import nullcontext
from dataclasses import asdict
from functools import wraps
import importlib.util
import json
from pathlib import Path
import sys
import traceback

from mcp_agent.app import MCPApp
from mcp_agent.agents.agent import Agent
from mcp_agent.config import Settings
from mcp_agent.workflows.llm.augmented_llm import RequestParams
from mcp_agent.workflows.llm.augmented_llm_openai import OpenAIAugmentedLLM
from mcp_flightrecorder import Recorder


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


endpoint = load_module("owned_legacy_endpoint", HERE / "host_endpoint.py").endpoint
make_hook = load_module("owned_legacy_example", ROOT / "examples/mcp_agent_session.py").make_session_init_hook


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def settings_for(output, base_url, case):
    args = ["-I", "-B", str(HERE / "host_peer.py"), str(output / "server.jsonl")]
    if case == "parallel":
        args.append("--parallel")
    return Settings(_env_file=None, execution_engine="asyncio",
        mcp={"servers": {"owned": {"command": sys.executable, "args": args, "read_timeout_seconds": 10}}},
        openai={"api_key": "owned-loopback-fixture-only", "base_url": base_url, "default_model": "gpt-4o"},
        otel={"enabled": False}, usage_telemetry={"enabled": False},
        logger={"type": "none", "transports": ["none"], "level": "error", "progress_display": False})


def boundary_witness(native, method, ledger):
    """Test-owned call/outcome ledger, applied identically to both native routes."""
    @wraps(native)
    async def observed(*args, **kwargs):
        entry = {"method": method, "name": (args[0] if args else kwargs.get("name")) if method == "tools/call" else None,
                 "arguments": args[1] if method == "tools/call" and len(args) > 1 else
                 kwargs.get("arguments") if method == "tools/call" else None}
        ledger.append(entry)
        try:
            result = await native(*args, **kwargs)
        except BaseException as error:
            entry["error"] = {"type": type(error).__name__, "message": str(error)}
            raise
        entry["result"] = result.model_dump(mode="json", by_alias=True, exclude_unset=True)
        return result
    return observed


async def run(case, mode, output):
    output.mkdir(parents=True, exist_ok=False)
    http_ledger, boundary_ledger = [], []
    prompt = "Exercise native scenario " + case + ". Use the listed tools as needed."
    recorder = Recorder(output / "recording.sqlite", capture="content",
        coverage={"producers": {"model": None, "host": "owned-public-turn",
                                 "mcp": "mcp_session_legacy/1.23.1"}, "model_complete": False},
        environment={"host": "mcp-agent", "version": "0.2.6",
                     "model_observation": "unavailable; public base_url transport only",
                     "mcp_attachment": "public ServerRegistry.register_init_hook"}) if mode == "recorded" else None
    summary = {"case": case, "mode": mode, "prompt": prompt,
               "transport": "public OpenAI base_url to owned loopback JSON endpoint",
               "model_observer": False, "constructor_patch": False}

    def owned_hook(session, auth):
        session.list_tools = boundary_witness(session.list_tools, "tools/list", boundary_ledger)
        session.call_tool = boundary_witness(session.call_tool, "tools/call", boundary_ledger)
        return True if recorder is None else make_hook(recorder, client_id="tagged-session", server_key="owned")(session, auth)

    try:
        with endpoint(case, http_ledger) as base_url:
            app = MCPApp(name="integration-owned", settings=settings_for(output, base_url, case))
            with recorder.turn(prompt) if recorder is not None else nullcontext():
                async with app.run():
                    app.context.server_registry.register_init_hook("owned", owned_hook)
                    async with Agent(name="owned-agent", instruction="Use only the deterministic requested tools.",
                                     server_names=["owned"], context=app.context) as agent:
                        llm = await agent.attach_llm(OpenAIAugmentedLLM)
                        summary["output"] = await llm.generate_str(prompt,
                            request_params=RequestParams(model="gpt-4o", max_iterations=5,
                                                         use_history=False, parallel_tool_calls=True))
    finally:
        if recorder is not None:
            summary["close"] = asdict(await recorder.aclose())
            health = recorder.health()
            summary["health"] = {"writer_errors": health.writer_errors, "dropped_events": health.dropped_events,
                                 "open_operations": health.open_operations, "reason_counts": dict(health.reason_counts)}
        write_json(output / "summary.json", summary)
        write_json(output / "http-ledger.json", http_ledger)
        write_json(output / "boundary-ledger.json", boundary_ledger)
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--case", required=True)
    parser.add_argument("--mode", choices=("plain", "recorded"), required=True)
    parser.add_argument("--output", type=Path, required=True)
    options = parser.parse_args()

    try:
        summary = asyncio.run(asyncio.wait_for(run(options.case, options.mode, options.output), 50))
        print(json.dumps(summary))
    except BaseException:
        options.output.mkdir(parents=True, exist_ok=True)
        (options.output / "failure.txt").write_text(traceback.format_exc())
        raise
