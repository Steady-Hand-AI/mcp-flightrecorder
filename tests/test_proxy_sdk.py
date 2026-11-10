"""Real SDK peers exercise the installed byte bridge, not an in-process adapter."""
import asyncio
from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import sys

from mcp import Client
from mcp.client.stdio import StdioServerParameters, stdio_client
from mcp.shared.exceptions import MCPError
from mcp_types import CallToolRequest, CallToolRequestParams, InputRequiredResult
import pytest
from pytest_check import check

import mcp_flightrecorder


PEER = Path(__file__).parent / "producers/proxy_sdk_peer.py"
BOOTSTRAP = Path(__file__).parent / "producers/proxy_peer.py"
FR = str(Path(sys.executable).with_name("fr"))
STATE = "opaque continuation café 🌱 /?=not-a-proxy-token"


def ledger(directory):
    return [json.loads(line) for line in (directory / "peer.jsonl").read_text().splitlines()]


def rows(path, sql):
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute(sql)]


def stored(directory):
    path = directory / "recording.sqlite"
    return rows(path, """SELECT o.*,s.details_json AS start_details,
        json_extract(s.details_json,'$.complete') AS start_complete,
        a.body_text AS arguments,json_extract(e.details_json,'$.complete') AS terminal_complete,e.state AS terminal_state,
        b.body_text AS result FROM operations o JOIN events s ON s.event_id=o.start_event_id
        LEFT JOIN artifacts a ON a.artifact_id=s.payload_ref
        LEFT JOIN events e ON e.event_id=o.end_event_id
        LEFT JOIN artifacts b ON b.artifact_id=e.payload_ref ORDER BY s.sequence""")


def prove_reaped(directory):
    pid = int((directory / "child.pid").read_text())
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


async def exercise(directory, mode, *, fault=None, scenario="ordinary"):
    directory.mkdir()
    child = [sys.executable, "-I", "-B", str(PEER), "--ledger", str(directory / "peer.jsonl"),
             "--pid", str(directory / "child.pid")]
    if scenario == "continuation-extension":
        child.append("--mrtr-extension")
    proxy_args = ["proxy"]
    if mode != "forward":
        proxy_args += ["--record", str(directory / "recording.sqlite"), "--capture", mode]
    proxy_args += ["--", *child]
    if mode == "direct":
        command, args = child[0], child[1:]
    elif fault:
        command, args = sys.executable, ["-I", "-B", str(BOOTSTRAP), "bootstrap", "--fault", fault,
            "--evidence", str(directory / "fault.jsonl"), "--", *proxy_args]
    else:
        command, args = FR, proxy_args
    assert "site-packages" in mcp_flightrecorder.__file__, "SDK acceptance must execute the installed candidate."
    (directory / "launch.json").write_text(json.dumps({"command": command, "args": args,
        "cwd": str(directory), "installed_origin": mcp_flightrecorder.__file__}, indent=2) + "\n")
    params = StdioServerParameters(command=command, args=args, cwd=directory)
    outcomes = []
    with (directory / "stderr.log").open("w") as errors:
        async with Client(stdio_client(params, errlog=errors), read_timeout_seconds=10, cache=None) as native:
            catalog = await native.list_tools()
            outcomes.append(catalog.model_dump(mode="json", by_alias=True, exclude_unset=True))
            if scenario == "ordinary":
                for name in ("echo", "echo", "refuse", "protocol"):
                    try:
                        result = await native.call_tool(name, {"text": "same input café 🌱"})
                    except MCPError as error:
                        outcomes.append({"error": error.error.model_dump(mode="json", by_alias=True, exclude_unset=True)})
                    else:
                        outcomes.append(result.model_dump(mode="json", by_alias=True, exclude_unset=True))
            elif scenario in ("continuation", "continuation-extension"):
                if scenario == "continuation-extension":
                    # Exercise the native typed request API: MCP2's broad
                    # call_tool union can choose its content-bearing arm for
                    # this unknown extension despite resultType=input_required.
                    first = await native.session.send_request(CallToolRequest(params=CallToolRequestParams(
                        name="continue", arguments={"text": "same input café 🌱"})), InputRequiredResult)
                else:
                    first = await native.session.call_tool("continue", {"text": "same input café 🌱"}, allow_input_required=True)
                assert isinstance(first, InputRequiredResult), "The first actual RPC must expose native input-required, not a completed tool value."
                assert first.request_state == STATE
                outcomes.append(first.model_dump(mode="json", by_alias=True, exclude_unset=True))
                final = await native.session.call_tool("continue", {"text": "same input café 🌱"},
                    request_state=first.request_state, input_responses={}, allow_input_required=True)
                assert not isinstance(final, InputRequiredResult)
                outcomes.append(final.model_dump(mode="json", by_alias=True, exclude_unset=True))
            elif scenario == "cancellation":
                entered = asyncio.Event()

                async def progress(value, total, message):
                    outcomes.append({"progress": value, "total": total, "message": message})
                    entered.set()

                pending = asyncio.create_task(native.call_tool("wait", {}, progress_callback=progress))
                try:
                    await asyncio.wait_for(entered.wait(), 5)
                finally:
                    pending.cancel()
                    await asyncio.gather(pending, return_exceptions=True)
                with pytest.raises(asyncio.CancelledError):
                    await pending
                after = await native.call_tool("echo", {"text": "after cancellation"})
                outcomes.append(after.model_dump(mode="json", by_alias=True, exclude_unset=True))
            else:
                raise AssertionError("Unknown test-owned scenario")
    prove_reaped(directory)
    (directory / "outcomes.json").write_text(json.dumps(outcomes, ensure_ascii=False, indent=2) + "\n")
    return outcomes


def assert_peer_equal(direct, proxied):
    check.equal(ledger(proxied), ledger(direct),
                msg="Independent genuine-server request IDs, methods, arguments, counts and native outcomes must be unchanged.")


def assert_fault_evidence(directory, fault):
    evidence = [json.loads(line) for line in (directory / "fault.jsonl").read_text().splitlines()]
    check.is_true(any(r["kind"] == "bootstrap" and "site-packages" in r["installed_origin"] for r in evidence))
    if fault in ("start-rejected", "first-rejected", "capacity"):
        check.is_true(any(r["kind"] == "start" and r["status"] == "rejected" and not r["ticket"] for r in evidence),
                      msg="The control must produce a real rejected receipt with no usable ticket.")
    else:
        expected = "injected-storage-error" if fault == "writer-failed" else "injected-initialization-error"
        check.is_true(any(r["kind"] == expected for r in evidence), msg="The exact installed core failure hook must actually execute.")
    check.is_true((directory / "stderr.log").stat().st_size > 0, msg="Capture-only failure must be visible separately from protocol stdout.")


@pytest.mark.parametrize("mode", ["content", "metadata", "forward"])
def test_installed_proxy_preserves_genuine_modern_roundtrip(tmp_path, run_sdk_async, mode):
    async def scenario():
        direct = await exercise(tmp_path / "direct", "direct")
        proxied = await exercise(tmp_path / "proxy", mode)
        check.equal(proxied, direct, msg="The actual SDK client receives the same complete results and protocol/tool errors.")
    run_sdk_async(scenario(), timeout=30)
    assert_peer_equal(tmp_path / "direct", tmp_path / "proxy")
    requests = [r for r in ledger(tmp_path / "proxy") if r["kind"] == "request"]
    check.equal([r["method"] for r in requests], ["server/discover", "tools/list"] + ["tools/call"] * 4,
                msg="Modern startup/discovery is native; forwarding must add no initialize, retry or duplicate call.")
    check.is_true(all(r["protocol_version"] == "2026-07-28" for r in requests))
    db = tmp_path / "proxy/recording.sqlite"
    if mode == "forward":
        check.is_false(db.exists(), msg="Forwarding-only mode creates no recording.")
        return
    operations = stored(tmp_path / "proxy")
    check.equal(len(operations), 5, msg="Only one tools/list and four distinct tools/call RPCs are captured.")
    check.equal([o["status"] for o in operations], ["ok", "ok", "ok", "tool_error", "protocol_error"])
    check.equal(rows(db, "SELECT count(*) AS n FROM turns"), [{"n": 0}])
    check.equal(rows(db, "SELECT count(*) AS n FROM events WHERE boundary IN ('model','host')"), [{"n": 0}])
    check.is_true(all(o["turn_id"] is None and o["model_call_id"] is None and o["decision_id"] is None
                      and o["dispatch_event_id"] is None and o["link_basis"] == "none" for o in operations))
    coverage = json.loads(rows(db, "SELECT coverage_json FROM runs")[0]["coverage_json"])
    check.equal(coverage, {"producers": {"model": None, "host": None, "mcp": "mfr.proxy"}, "model_complete": False})
    if mode == "metadata":
        check.is_true(all(o["arguments"] is None and o["result"] is None for o in operations),
                      msg="Default metadata retains no recoverable tool payload body.")
    else:
        check.is_true(all(o["arguments"] is not None and o["result"] is not None for o in operations))


@pytest.mark.parametrize("extension", [False, True])
def test_modern_input_required_and_explicit_continuation_are_separate_rpcs(tmp_path, run_sdk_async, extension):
    selected = "continuation-extension" if extension else "continuation"
    async def scenario():
        direct = await exercise(tmp_path / "direct", "direct", scenario=selected)
        proxied = await exercise(tmp_path / "proxy", "content", scenario=selected)
        check.equal(proxied, direct)
    run_sdk_async(scenario(), timeout=30)
    assert_peer_equal(tmp_path / "direct", tmp_path / "proxy")
    calls = [r for r in ledger(tmp_path / "proxy") if r["kind"] == "invocation"]
    assert len(calls) == 2
    check.not_equal(calls[0]["id"], calls[1]["id"], msg="The client's explicit continuation uses another genuine RPC ID.")
    check.equal([r["request_state"] for r in calls], [None, STATE])
    first = next(r["result"] for r in ledger(tmp_path / "proxy")
                 if r["kind"] == "result" and r["id"] == calls[0]["id"])
    check.equal(first["resultType"], "input_required")
    check.is_false("inputRequests" in first, msg="This native MRTR arm carries requestState only.")
    if extension:
        check.equal(first["content"], [], msg="The genuine server middleware must actually emit the unknown extension.")
    operations = [o for o in stored(tmp_path / "proxy") if o["method"] == "tools/call"]
    assert len(operations) == 2
    check.equal([o["terminal_complete"] for o in operations], [0, 1],
                msg="Input-required is partial evidence; only the later independent tool RPC completes.")
    check.equal(json.loads(operations[0]["result"])["requestState"], STATE)


@pytest.mark.parametrize("fault", ["start-rejected", "first-rejected", "capacity", "writer-failed", "init-failed"])
def test_real_sdk_roundtrip_survives_installed_capture_failure(tmp_path, run_sdk_async, fault):
    async def scenario():
        direct = await exercise(tmp_path / "direct", "direct")
        proxied = await exercise(tmp_path / "proxy", "content", fault=fault)
        check.equal(proxied, direct, msg="Actual storage/admission failure must not alter native results, errors or invocation count.")
    run_sdk_async(scenario(), timeout=30)
    assert_peer_equal(tmp_path / "direct", tmp_path / "proxy")
    assert_fault_evidence(tmp_path / "proxy", fault)


@pytest.mark.parametrize("mode,fault", [("content", None), ("forward", None),
    ("content", "start-rejected"), ("content", "capacity"),
    ("content", "writer-failed"), ("content", "init-failed")])
def test_real_progress_and_client_cancellation_preserve_native_connection(tmp_path, run_sdk_async, mode, fault):
    async def scenario():
        direct = await exercise(tmp_path / "direct", "direct", scenario="cancellation")
        proxied = await exercise(tmp_path / "proxy", mode, fault=fault, scenario="cancellation")
        check.equal(proxied, direct, msg="Native progress, cancellation and a later successful tool call must survive the bridge.")
    run_sdk_async(scenario(), timeout=30)
    for label in ("direct", "proxy"):
        observed = ledger(tmp_path / label)
        check.is_true(any(r["kind"] == "request" and r["method"] == "notifications/cancelled" for r in observed))
        check.equal(len([r for r in observed if r["kind"] == "invocation"]), 2)
    check.equal([r for r in ledger(tmp_path / "proxy") if r["kind"] in ("request", "invocation")],
                [r for r in ledger(tmp_path / "direct") if r["kind"] in ("request", "invocation")],
                msg="Capture failure cannot change cancellation, request IDs, arguments or native invocation counts.")
    if fault:
        assert_fault_evidence(tmp_path / "proxy", fault)
        return
    if mode == "forward":
        check.is_false((tmp_path / "proxy/recording.sqlite").exists())
        return
    operations = stored(tmp_path / "proxy")
    pending = [o for o in operations if o["name"] == "wait"]
    assert len(pending) == 1
    check.equal((pending[0]["status"], pending[0]["end_event_id"]), ("incomplete", None),
                msg="Forwarded cancellation alone cannot fabricate a remote terminal response.")
