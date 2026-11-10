"""Real SDK clients launch the installed inspector over subprocess stdio.

Database mutations below are explicit reader-input controls. Successful inputs
come from the existing genuine SDK producer or the public Recorder/snapshot API.
"""
from contextlib import asynccontextmanager, closing
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import threading
import time

from mcp import Client
from mcp.client.stdio import StdioServerParameters
from mcp.shared.exceptions import MCPError
import pytest
from pytest_check import check

from mcp_flightrecorder import Recorder, inspect
from mcp_flightrecorder._mcp_inspect import _Recording

from .recordings import integration_recording


URI = "mfr://schema/inspection"
TOOLS = {"summarize_problems", "list_runs", "find_turns", "explain_turn", "trace_back"}
FR = str(Path(sys.executable).with_name("fr"))
SUFFIXES = ("", "-wal", "-shm", "-journal")
SECRET = "retained-only secret 🌱 café "
COVERAGE = {"producers": {"model": "fixture.model", "host": "fixture.host", "mcp": None},
            "model_complete": True}


def inventory(path):
    return {suffix: hashlib.sha256(Path(str(path) + suffix).read_bytes()).hexdigest()
            for suffix in SUFFIXES if Path(str(path) + suffix).is_file()}


@pytest.fixture
def recording(tmp_path):
    original, symbols = integration_recording()
    path = tmp_path / "selected recording ? #.sqlite"
    shutil.copyfile(original, path)
    return path, symbols


@asynccontextmanager
async def client(path, tmp_path, *, allow_content=False):
    outside = tmp_path / "outside-checkout"
    outside.mkdir(exist_ok=True)
    args = ["mcp", str(path), *(["--allow-content"] if allow_content else [])]
    async with Client(StdioServerParameters(command=FR, args=args, cwd=outside),
                      read_timeout_seconds=10) as native:
        yield native


async def call(native, name, arguments=None, *, ok=True):
    result = await native.call_tool(name, arguments or {})
    assert len(result.content) == 1, "The transport returns one inspection JSON block, not duplicate result bodies."
    assert result.content[0].type == "text", "Inspection results use one compact JSON text block."
    check.is_none(result.structured_content, msg="Structured content must not duplicate the bounded inspection result.")
    text = result.content[0].text
    value = json.loads(text)
    check.equal(text, json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")),
                msg="The exact tool text uses the existing compact UTF-8 JSON encoding.")
    check.equal(result.is_error, not ok, msg="The SDK tool error flag agrees with inspection success/refusal.")
    check.equal(value["ok"], ok, msg="A protocol success must not disguise an inspection failure.")
    budget = (arguments or {}).get("max_output_bytes", 65536)
    if type(budget) is int and 1024 <= budget <= 1048576:
        check.less_equal(len(text.encode()), budget, msg="The encoded JSON text fits the requested budget, excluding MCP framing.")
    if not ok:
        check.less_equal(len(text.encode()), 1024, msg="Errors have a small independent encoded bound.")
        check.is_not_in("Traceback", text, msg="Tool failures do not expose Python tracebacks.")
    return value


def test_installed_stdio_discovery_and_reader_equivalent_navigation(recording, tmp_path, run_sdk_async):
    path, s = recording
    before = inventory(path)
    probe = subprocess.run([sys.executable, "-I", "-B", "-c",
                            "import mcp_flightrecorder; print(mcp_flightrecorder.__file__)"],
                           cwd=tmp_path, capture_output=True, text=True, timeout=10)
    assert probe.returncode == 0 and "site-packages" in probe.stdout, "The genuine fr child must use the installed distribution outside the checkout."

    async def scenario():
        async with client(path, tmp_path) as native:
            declarations = await native.list_tools()
            check.equal({tool.name for tool in declarations.tools}, TOOLS,
                        msg="The default server exposes exactly five bounded read-only tools.")
            check.equal(len(declarations.tools), 5, msg="Discovery has no duplicate tools.")
            for tool in declarations.tools:
                schema = tool.input_schema
                check.is_false(schema["additionalProperties"], msg="No undeclared path, SQL or command argument is accepted.")
                check.equal(schema["properties"]["timeout"]["maximum"], 30, msg="Every reader advertises the shared deadline ceiling.")
                check.equal(schema["properties"]["max_output_bytes"]["maximum"], 1048576,
                            msg="Every reader advertises the shared encoded JSON ceiling.")
            resources = await native.list_resources()
            check.equal([str(resource.uri) for resource in resources.resources], [URI],
                        msg="The only resource is the fixed installed inspection schema.")
            resource = await native.read_resource(URI)
            assert len(resource.contents) == 1
            check.equal(json.loads(resource.contents[0].text), inspect.schema(),
                        msg="The resource is the existing inspection contract, not a competing schema.")
            check.less_equal(len(resource.contents[0].text.encode()), 65536, msg="The static resource has a fixed encoded-response bound.")
            with pytest.raises(MCPError) as unavailable:
                await native.read_resource("file:///PRIVATE-RESOURCE-SENTINEL")
            check.is_not_in("PRIVATE-", str(unavailable.value), msg="Unknown resource errors never echo a client-supplied location.")
            cases = [
                ("summarize_problems", {"run_id": s["R"]}, inspect.diagnose(path, s["R"], no_content=True)),
                ("list_runs", {}, inspect.runs(path, no_content=True)),
                ("find_turns", {"run_id": s["R"], "filters": {"text_mismatch": True}},
                 inspect.find_turns(path, s["R"], filters={"text_mismatch": True}, no_content=True)),
                ("explain_turn", {"run_id": s["R"], "turn_id": s["T1"], "model_call_id": s["M1"]},
                 inspect.explain(path, s["R"], s["T1"], s["M1"], no_content=True)),
                ("trace_back", {"run_id": s["R"], "request_event_id": s["W1"]},
                 inspect.slice(path, s["R"], s["W1"], no_content=True)),
            ]
            for name, arguments, expected in cases:
                actual = await call(native, name, arguments)
                check.equal(actual, expected, msg=f"{name} preserves the existing reader's facts, unknowns and cutoffs.")
            turns = cases[2][2]["data"]["items"]
            check.equal([item["turn"]["turn_id"] for item in turns], [s["T1"], s["T2"]],
                        msg="The genuine fixture exposes both the initial rewrite and its later retained occurrence.")
    run_sdk_async(scenario())
    check.equal(inventory(path), before, msg="All discovery and navigation leave the portable source bytes and fileset unchanged.")


def test_pagination_cutoffs_and_session_recovery(recording, tmp_path, run_sdk_async):
    path, s = recording

    async def scenario():
        async with client(path, tmp_path) as native:
            first = await call(native, "find_turns", {"run_id": s["R"], "limit": 1})
            assert first["data"]["has_more"] and first["data"]["next_cursor"]
            second = await call(native, "find_turns", {"run_id": s["R"], "limit": 1,
                                                        "cursor": first["data"]["next_cursor"]})
            check.equal([first["data"]["items"][0]["turn"]["turn_id"], second["data"]["items"][0]["turn"]["turn_id"]],
                        [s["T1"], s["T2"]], msg="The reader cursor resumes the exact fixed recording without repeats.")
            check.is_false(second["data"]["has_more"], msg="The second page exhausts the fixture.")
            bounded = await call(native, "trace_back", {"run_id": s["R"], "request_event_id": s["W1"], "limit": 1, "max_depth": 0})
            check.is_true(bool(bounded["cutoffs"]), msg="A small ancestry traversal preserves its explicit incomplete cutoff.")
            timed = await call(native, "list_runs", {"timeout": 1e-100}, ok=False)
            check.equal(timed["error"]["code"], "query_timeout", msg="A real reader deadline becomes a normal tool error.")
            recovered = await call(native, "list_runs")
            check.equal(recovered["data"]["items"][0]["run_id"], s["R"], msg="The same session remains usable after a timed-out read.")
    run_sdk_async(scenario())


@pytest.mark.parametrize("name,arguments", [
    ("list_runs", {"path": "PRIVATE-PATH-SENTINEL"}),
    ("list_runs", {"limit": True}),
    ("list_runs", {"limit": 201}),
    ("list_runs", {"timeout": 31}),
    ("list_runs", {"max_output_bytes": 1023}),
    ("list_runs", {"max_output_bytes": 1048577}),
    ("list_runs", {"cursor": "PRIVATE-CURSOR-SENTINEL"}),
    ("find_turns", {}),
    ("find_turns", {"run_id": "PRIVATE-UNKNOWN-RUN"}),
    ("find_turns", {"run_id": "R", "filters": {"sql": "PRIVATE-SQL-SENTINEL"}}),
    ("explain_turn", {"run_id": "R", "turn_id": "PRIVATE-UNKNOWN-TURN"}),
    ("explain_turn", {"run_id": "R", "turn_id": "T1", "model_call_id": "PRIVATE-UNKNOWN-MODEL"}),
    ("trace_back", {"run_id": "R", "request_event_id": "PRIVATE-UNKNOWN-EVENT"}),
])
def test_invalid_requests_are_safe_and_recoverable(recording, tmp_path, run_sdk_async, name, arguments):
    path, s = recording
    arguments = {key: s[value] if type(value) is str and value in {"R", "T1"} else value
                 for key, value in arguments.items()}

    async def scenario():
        async with client(path, tmp_path) as native:
            value = await call(native, name, arguments, ok=False)
            check.is_not_in("PRIVATE-", json.dumps(value), msg="Rejected client values are not echoed in bounded error text.")
            await call(native, "list_runs")
    run_sdk_async(scenario())


def test_ambiguous_run_requires_selection(recording, tmp_path, run_sdk_async):
    path, s = recording
    with closing(sqlite3.connect(path)) as connection:
        connection.row_factory = sqlite3.Row
        row = dict(connection.execute("SELECT * FROM runs LIMIT 1").fetchone())
        row["run_id"] = "second-recorded-run"
        connection.execute("INSERT INTO runs (" + ",".join(row) + ") VALUES (" + ",".join("?" for _ in row) + ")", list(row.values()))
        connection.commit()

    async def scenario():
        async with client(path, tmp_path) as native:
            ambiguous = await call(native, "summarize_problems", ok=False)
            check.equal(ambiguous["error"]["reason"], "ambiguous_run", msg="Multiple recorded runs require an explicit selection.")
            await call(native, "summarize_problems", {"run_id": s["R"]})
            page = await call(native, "list_runs", {"limit": 1})
            assert page["data"]["has_more"]
            last = await call(native, "list_runs", {"limit": 1, "cursor": page["data"]["next_cursor"]})
            check.is_false(last["data"]["has_more"], msg="Run pagination also uses the reader's exact continuation.")
    run_sdk_async(scenario())


@pytest.fixture
def content_recording(tmp_path):
    path = tmp_path / "content.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    try:
        with recorder.turn("ordinary turn label") as turn:
            receipt = recorder.observe("model.request", scope=recorder.capture_scope(),
                                       payload={"messages": [{"role": "user", "content": SECRET * 200}]},
                                       facts={"message_path": "/messages", "messages": [
                                           {"bindings": [], "unresolved": [], "complete": True}]})
            assert receipt.status == "accepted"
            terminal = recorder.observe("model.response", scope=recorder.capture_scope(), payload={"answer": "done"},
                                        facts={"model_call_id": receipt.model_call_id, "state": "complete"})
            assert terminal.status == "accepted"
        run_id, turn_id = recorder.capture_scope().run_id, turn.receipt.turn_id
    finally:
        assert recorder.close().portable
    with closing(sqlite3.connect(path)) as connection:
        artifact_id, body = connection.execute("SELECT artifact_id,body_text FROM artifacts WHERE body_text LIKE ?", ("%retained-only%",)).fetchone()
    return path, run_id, turn_id, artifact_id, body


@pytest.mark.parametrize("allow_content", [False, True], ids=["default", "opt-in"])
def test_content_is_explicit_and_utf8_windowed(content_recording, tmp_path, run_sdk_async, allow_content):
    path, run_id, turn_id, artifact_id, body = content_recording

    async def scenario():
        async with client(path, tmp_path, allow_content=allow_content) as native:
            tools = await native.list_tools()
            check.equal({tool.name for tool in tools.tools}, TOOLS | ({"read_payload"} if allow_content else set()),
                        msg="Only startup opt-in changes payload-tool availability.")
            for name, args in [("list_runs", {}), ("summarize_problems", {"run_id": run_id}),
                               ("find_turns", {"run_id": run_id}), ("explain_turn", {"run_id": run_id, "turn_id": turn_id})]:
                ordinary = await call(native, name, args)
                check.is_not_in(SECRET, json.dumps(ordinary, ensure_ascii=False),
                                msg="Ordinary tools omit retained bodies even when explicit payload access is enabled.")
            if not allow_content:
                # The current SDK rejects unavailable modern tools during schema
                # negotiation. Either a protocol refusal or a tool refusal is valid;
                # neither may disclose the requested payload.
                try:
                    denied = await call(native, "read_payload", {"run_id": run_id, "artifact_id": artifact_id}, ok=False)
                    check.is_not_in(SECRET, json.dumps(denied, ensure_ascii=False), msg="An undiscovered tool cannot bypass content policy.")
                except MCPError as error:
                    check.is_not_in(SECRET, str(error), msg="SDK unavailable-tool rejection contains no retained body.")
                await call(native, "list_runs")
                return
            raw = body.encode()
            offset = raw.index("🌱".encode())
            arguments = {"run_id": run_id, "artifact_id": artifact_id, "offset": offset, "max_bytes": 7}
            window = await call(native, "read_payload", arguments)
            check.equal(window["data"]["text"].encode(), raw[offset:offset + 7], msg="The payload window preserves exact independently retained UTF-8 bytes.")
            check.equal(window["data"]["next_offset"], offset + 7, msg="Payload continuation is an absolute byte offset.")
            await call(native, "read_payload", {**arguments, "offset": offset + 1}, ok=False)
            await call(native, "read_payload", {**arguments, "artifact_id": "unavailable"}, ok=False)
            capped = await call(native, "read_payload", {**arguments, "offset": 0, "max_bytes": 65536, "max_output_bytes": 1024})
            check.is_true(bool(capped["cutoffs"]), msg="A byte-limited payload reports its explicit output cutoff.")
            check.equal(capped["data"]["text"].encode(), raw[:capped["data"]["returned_bytes"]], msg="Budget shrinking retains a correct UTF-8 prefix, not silently altered content.")
    run_sdk_async(scenario())


@pytest.mark.parametrize("state", ["wal", "shm", "header-only", "invalid"])
def test_startup_refuses_filesets_before_sqlite_can_touch_them(recording, tmp_path, state):
    path, _ = recording
    if state in {"wal", "shm"}:
        Path(str(path) + "-" + state).write_bytes(b"test-owned sidecar sentinel")
    elif state == "header-only":
        body = bytearray(path.read_bytes())
        body[18:20] = b"\x02\x02"
        path.write_bytes(body)
    else:
        path.write_bytes(b"PRIVATE-INVALID-DATABASE-BYTES")
    before = inventory(path)
    child = subprocess.run([FR, "mcp", str(path)], cwd=tmp_path, input="", capture_output=True, text=True, timeout=10)
    check.equal(child.returncode, 3, msg="Unsafe/nonportable startup input has the storage-class refusal exit.")
    check.equal(child.stdout, "", msg="Startup refusal never writes prose to MCP stdout.")
    check.is_not_in("Traceback", child.stderr, msg="Startup storage failure is safe and concise.")
    check.is_not_in("PRIVATE-", child.stderr, msg="Storage refusal never prints input bytes.")
    check.equal(inventory(path), before, msg="Refusal neither opens SQLite nor creates/changes/recoveries a sidecar.")


def test_portable_snapshot_with_open_run_is_valid(tmp_path, run_sdk_async):
    source, output = tmp_path / "live.sqlite", tmp_path / "snapshot.sqlite"
    recorder = Recorder(source, capture="content", coverage=COVERAGE)
    try:
        with recorder.turn("recorded open run"):
            pass
        deadline, pause = time.monotonic() + 10, threading.Event()
        while recorder.health().ordinary.used_slots and time.monotonic() < deadline:
            pause.wait(.002)
        assert recorder.health().ordinary.used_slots == 0
        snapshot = inspect.snapshot(source, output)
        assert snapshot["ok"]
        before = inventory(output)

        async def scenario():
            async with client(output, tmp_path) as native:
                runs = await call(native, "list_runs")
                check.equal(runs["data"]["items"][0]["close_state"], "open",
                            msg="A portable file is accepted without pretending its recorded run is closed.")
        run_sdk_async(scenario())
        check.equal(inventory(output), before, msg="Reading the standalone snapshot leaves its fileset unchanged.")
    finally:
        assert recorder.close().writer_stopped


@pytest.mark.parametrize("change", ["shm", "header", "replacement"])
def test_reader_owns_connections_and_refuses_changed_file(recording, monkeypatch, change):
    path, _ = recording
    fixed = _Recording(path, False)
    opened, closed = [], []
    original = sqlite3.connect

    class Tracked(sqlite3.Connection):
        def close(self):
            closed.append(id(self))
            super().close()

    def connect(*args, **kwargs):
        value = original(*args, **kwargs, factory=Tracked)
        opened.append(id(value))
        return value

    monkeypatch.setattr(sqlite3, "connect", connect)
    assert fixed.call("list_runs", {})["ok"]
    assert not fixed.call("list_runs", {"timeout": 1e-100})["ok"]
    assert fixed.call("list_runs", {})["ok"]
    check.equal(closed, opened, msg="Each successful or interrupted read closes its own connection before returning.")
    count = len(opened)
    if change == "shm":
        Path(str(path) + "-shm").write_bytes(b"late sidecar")
    elif change == "header":
        data = bytearray(path.read_bytes())
        data[18:20] = b"\x02\x02"
        path.write_bytes(data)
    else:
        replacement = path.with_suffix(".replacement")
        replacement.write_bytes(path.read_bytes())
        replacement.replace(path)
    refused = fixed.call("list_runs", {})
    check.equal(refused["error"]["code"], "recording_unreadable" if change == "replacement" else "recording_open_wal",
                msg="A later changed or unsafe fileset is refused in the same session.")
    check.equal(len(opened), count, msg="The portable-file check runs before SQLite can touch a new sidecar.")


def test_clean_stdio_eof_exits_without_nonprotocol_output(recording, tmp_path):
    path, _ = recording
    before = inventory(path)
    child = subprocess.run([FR, "mcp", str(path)], cwd=tmp_path, input="", capture_output=True, text=True, timeout=10)
    check.equal(child.returncode, 0, msg="Normal transport EOF reaps the owned server process.")
    check.equal(child.stdout, "", msg="The idle server produces no banner or other nonprotocol stdout.")
    check.equal(child.stderr, "", msg="Clean shutdown has no traceback or cleanup error.")
    check.equal(inventory(path), before, msg="Even startup/shutdown alone cannot create SQLite sidecars.")
