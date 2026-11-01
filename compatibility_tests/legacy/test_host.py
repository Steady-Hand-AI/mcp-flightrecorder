"""B3/B4: actual tagged host loop and explicit public-hook lifetime controls."""
from collections import Counter
import gc
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import weakref

from mcp_agent.app import MCPApp
from mcp_agent.config import Settings
from mcp_agent.mcp.mcp_server_registry import ServerRegistry
from mcp_flightrecorder import Recorder, inspect
import pytest
import pytest_check as check

from .support import COVERAGE, finish, load_example, rows


HERE = Path(__file__).resolve().parent
make_hook = load_example("mcp_agent_session").make_session_init_hook


def load(path):
    return json.loads(path.read_text())


def launch(case, mode, directory):
    output = directory / mode
    command = [sys.executable, "-I", "-B", str(HERE / "host_run.py"),
               "--case", case, "--mode", mode, "--output", str(output)]
    result = subprocess.run(command, cwd=HERE, capture_output=True, text=True, timeout=65)
    (directory / (mode + ".stdout.log")).write_text(result.stdout)
    (directory / (mode + ".stderr.log")).write_text(result.stderr)
    assert result.returncode == 0, (
        "The actual tagged native host must complete before behavior/capture parity can be assessed; inspect retained stderr."
    )
    return load(output / "summary.json")


def server_ledger(directory):
    return [json.loads(line) for line in (directory / "server.jsonl").read_text().splitlines()]


def normalize(values):
    return Counter(json.dumps(value, sort_keys=True) for value in values)


@pytest.mark.parametrize("case", ["success", "parallel", "repeated", "tool_error", "malformed_json"])
def test_tagged_host_mcp_recipe_matches_plain_native_loop(evidence_path, case):
    plain = launch(case, "plain", evidence_path)
    recorded = launch(case, "recorded", evidence_path)
    check.equal((plain["output"], recorded["output"]), ("native-" + case + "-complete",) * 2,
                msg="Both actual host loops must reach the same scripted answer, not just initialize or list tools.")
    http_plain, http_recorded = [load(evidence_path / mode / "http-ledger.json") for mode in ("plain", "recorded")]
    check.equal(http_recorded, http_plain,
                msg="The public-base_url fixture must see unchanged prompts, catalogs, tool-result context, requests and responses.")
    check.equal(len(http_plain), 3 if case == "repeated" else 2,
                msg="The actual model loop consumes exactly the scripted submissions with no observer-added retry.")
    native_plain, native_recorded = [load(evidence_path / mode / "boundary-ledger.json") for mode in ("plain", "recorded")]
    check.equal(normalize(native_recorded), normalize(native_plain),
                msg="Every actual intercepted SDK boundary call/outcome must match plain native behavior, allowing parallel interleaving.")
    peer_plain, peer_recorded = [server_ledger(evidence_path / mode) for mode in ("plain", "recorded")]
    check.equal(normalize(peer_recorded), normalize(peer_plain),
                msg="Independent peer discovery, arguments, occurrence counts and results must remain unchanged.")
    expected_calls = 2 if case in ("parallel", "repeated") else 0 if case == "malformed_json" else 1
    check.equal(sum(row["kind"] == "tools/call" for row in peer_recorded), expected_calls,
                msg="Malformed JSON is rejected by the host before MCP; equal/repeated calls remain distinct executed occurrences.")
    if case == "parallel":
        entered = [i for i, row in enumerate(peer_recorded) if row["kind"] == "entered"]
        completed = [i for i, row in enumerate(peer_recorded) if row["kind"] == "result"]
        assert len(entered) == len(completed) == 2, "Overlap proof requires both actual server entries and both actual results."
        check.less(max(entered), min(completed),
                   msg="A server-side barrier witnesses overlapping equal calls before either result; two counts alone cannot prove parallelism.")
    db = evidence_path / "recorded/recording.sqlite"
    with sqlite3.connect(db.resolve().as_uri() + "?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        event_rows = [dict(row) for row in conn.execute("""SELECT e.*, a.body_text FROM events e
                      LEFT JOIN artifacts a ON a.artifact_id=e.payload_ref ORDER BY sequence""")]
    operations = rows(db, "SELECT * FROM operations ORDER BY rowid")
    starts = {event["event_id"]: event for event in event_rows if event["kind"] == "mcp.request"}
    terminals = {event["event_id"]: event for event in event_rows if event["kind"] == "mcp.response"}
    check.equal(Counter(operation["method"] for operation in operations),
                Counter(entry["method"] for entry in native_recorded),
                msg="Persisted operation multiplicity must equal observed native boundary invocations, including real internal discovery.")
    tool_operations = [operation for operation in operations if operation["method"] == "tools/call"]
    check.equal(normalize([json.loads(starts[op["start_event_id"]]["body_text"])["arguments"] for op in tool_operations]),
                normalize([row["payload"]["arguments"] for row in peer_recorded if row["kind"] == "tools/call"]),
                msg="Retained tool arguments match the independent server's received values and occurrence multiplicity.")
    check.equal(normalize([json.loads(terminals[op["end_event_id"]]["body_text"]) for op in tool_operations]),
                normalize([row["payload"] for row in peer_recorded if row["kind"] == "result"]),
                msg="Retained tool results equal independently emitted native bodies, including isError and text.")
    check.equal([operation["status"] for operation in tool_operations],
                ["tool_error" if case == "tool_error" else "ok"] * expected_calls,
                msg="The installed legacy facade preserves returned tool-error classification.")
    check.equal([event["kind"] for event in event_rows if event["kind"].startswith("model.")], [],
                msg="This maintained session recipe declares model observation unavailable and installs no model observer.")
    check.is_true(all(op["decision_id"] is None and op["dispatch_event_id"] is None and
                      op["model_call_id"] is None and op["resolution"] == "unknown" for op in operations),
                  msg="Native host success and server ledgers cannot create missing dispatch/result provenance.")
    check.equal(rows(db, "SELECT * FROM links"), [], msg="No causal links may be inferred from equal arguments, provider IDs or timing.")
    check.equal(rows(db, "SELECT * FROM answer_occurrences"), [],
                msg="Without a model-boundary observer or explicit result receipts there are no invented submitted-answer occurrences.")
    check.is_true(recorded["close"]["portable"], msg="Final recordings must be standalone portable SQLite files.")
    check.equal((recorded["health"]["writer_errors"], recorded["health"]["dropped_events"],
                 recorded["health"]["open_operations"]), (0, 0, 0),
                msg="Truthful partial coverage must not conceal dropped events, writer failures or unfinished native operations.")
    run_id = rows(db, "SELECT run_id FROM runs")[0]["run_id"]
    report = inspect.check(db, run_id, expect=["model", "mcp"])
    check.is_true(report["ok"], msg="The public reader must successfully assess the recorded file without claiming full-chain capture.")
    check.equal(report["data"]["status"], "fail",
                msg="A successful native host run cannot pass a complete-chain check when model coverage is unavailable.")
    check.equal(report["data"]["required_failures"], ["coverage.model"],
                msg="The public reader must name the explicitly unavailable model producer as the required coverage failure.")
    check.equal(report["data"]["required_unknowns"], ["capture_health"],
                msg="Incomplete turn coverage remains unknown despite clean capture and correct MCP lifecycle.")


class NullNative:
    """Labelled instance for synchronous hook behavior; no native work is run."""
    async def list_tools(self, cursor=None, *, params=None):
        return None

    async def call_tool(self, name, arguments=None, read_timeout_seconds=None, progress_callback=None, *, meta=None):
        return None


def hook_settings():
    return Settings(_env_file=None, mcp={"servers": {"owned": {"command": sys.executable, "args": []}}},
                    usage_telemetry={"enabled": False}, otel={"enabled": False},
                    logger={"type": "none", "transports": ["none"], "progress_display": False})


@pytest.mark.parametrize("previous", ["true", "false", "raise"])
def test_public_registry_owner_composition_preserves_previous_hook_outcome(evidence_path, previous):
    path = evidence_path / "recording.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    native = NullNative()
    originals = native.list_tools, native.call_tool
    calls, auth_values = [], []
    sentinel = ValueError("owner hook failure")
    recording_hook = make_hook(recorder, client_id="composed")
    registry = ServerRegistry(config=hook_settings())

    def owner_hook(session, auth):
        calls.append(session)
        auth_values.append(auth)
        if previous == "raise":
            raise sentinel
        return previous == "true"

    def composed(session, auth):
        if not owner_hook(session, auth):
            return False
        return recording_hook(session, auth)

    registry.register_init_hook("owned", composed)
    try:
        if previous == "raise":
            with pytest.raises(ValueError) as raised:
                registry.execute_init_hook("owned", native)
            check.is_(raised.value, sentinel, msg="Explicit owner composition propagates the exact previous hook exception without attachment.")
        else:
            check.equal(registry.execute_init_hook("owned", native), previous == "true",
                        msg="Public execute_init_hook exposes the composed boolean rather than inventing startup-abort semantics.")
        check.equal(calls, [native], msg="The previous owner hook is called once with the same native session.")
        check.equal(auth_values, [registry.registry["owned"].auth], msg="Composition forwards the exact native authentication argument.")
        check.equal(native.list_tools == originals[0], previous != "true",
                    msg="The recording methods attach only after the previous hook explicitly returns True.")
        check.equal(native.call_tool == originals[1], previous != "true",
                    msg="A False result or exception leaves both original methods untouched.")
        facts = finish(recorder, path)
        check.equal(facts["events"], [], msg="Hook registration/attachment alone cannot invent observed MCP work.")
    finally:
        recorder.close(timeout=5)


def test_hook_null_session_and_auth_nonretention(evidence_path):
    path = evidence_path / "recording.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)

    class Authentication:
        """Opaque auth sentinel; attachment must never inspect its contents."""
        def __getattr__(self, name):
            raise AssertionError("The recording hook must not read authentication.")

    auth = Authentication()
    reference = weakref.ref(auth)
    native = NullNative()
    hook = make_hook(recorder, client_id="no-auth")
    try:
        check.is_false(hook(None, auth), msg="A null native session returns False before attachment or capture.")
        check.is_true(hook(native, auth), msg="An initialized session attaches without reading its authentication object.")
        del auth
        gc.collect()
        check.is_none(reference(), msg="Attached observers and the reusable hook retain no authentication object.")
        facts = finish(recorder, path)
        check.equal(facts["events"], [], msg="Neither null nor real attachment emits events without native method execution.")
    finally:
        recorder.close(timeout=5)


@pytest.mark.parametrize("previous_accepts", [True, False], ids=["attach", "owner-refuses"])
def test_real_registry_reconnect_attaches_each_new_session_once(
        evidence_path, run_sdk_async, previous_accepts):
    path = evidence_path / "recording.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    sessions, owner_calls = [], []
    settings = hook_settings()
    settings.mcp.servers["owned"].args = [
        "-I", "-B", str(HERE / "session_peer.py"), str(evidence_path / "server.jsonl")]
    recording_hook = make_hook(recorder, client_id="reconnect")

    def composed(session, auth):
        owner_calls.append((session, auth))
        if not previous_accepts:
            return False
        return recording_hook(session, auth)

    async def scenario():
        app = MCPApp(name="owned-reconnect", settings=settings)
        async with app.run():
            registry = app.context.server_registry
            registry.register_init_hook("owned", composed)
            for index in range(2):
                async with registry.initialize_server("owned", context=app.context) as native:
                    sessions.append(native)
                    result = await native.list_tools()
                    check.equal([tool.name for tool in result.tools], ["echo", "refuse", "wait"],
                                msg="Native initialized/reconnected sessions remain usable, including when a hook returns False.")
            check.equal(len(owner_calls), 2, msg="Each actual new-session initialization invokes its owner hook once.")
            check.is_not(sessions[0], sessions[1], msg="Reconnect must yield a genuinely new native session requiring separate attachment.")

    try:
        run_sdk_async(scenario(), timeout=30)
        facts = finish(recorder, path)
        check.equal(len(facts["operations"]), 2 if previous_accepts else 0,
                    msg="Attach each new active session once; native startup ignoring False must not be misreported as aborted.")
        check.equal([operation["method"] for operation in facts["operations"]], ["tools/list"] * (2 if previous_accepts else 0),
                    msg="Reattachment adds neither duplicate invocation nor synthetic reconnect operation.")
    finally:
        recorder.close(timeout=5)
