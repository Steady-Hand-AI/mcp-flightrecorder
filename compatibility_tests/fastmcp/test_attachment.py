"""Actual FastMCP server/client over owned stdio, plain versus observed."""
import json
from pathlib import Path
import sys
import pytest
import pytest_check as check

from fastmcp import Client
from fastmcp.client.transports import StdioTransport
from fastmcp.exceptions import ToolError
from mcp_flightrecorder import inspect

from .support import (attach_observer, finish, ledger, make_recorder, native_client,
                      project, witness_native)


def check_closed(facts):
    assert facts["check"]["ok"] is True, "Health assertions require a valid public inspection envelope."
    check.equal(facts["check"]["data"]["required_failures"], [],
                msg="The public reader may disclose incomplete model coverage but must find no failed database contract.")
    run = facts["check"]["data"]["run"]
    check.equal((run["close_state"], run["dropped_events"], run["writer_errors"]), ("clean", 0, 0),
                msg="A successful native result alone does not prove the recorder admitted and persisted its observations.")
    check.equal(run["coverage"]["producers"]["model"], None,
                msg="The no-model scenario must remain explicitly unclaimed rather than fabricating LLM coverage.")
    check.equal(facts["integrity"], [{"integrity_check": "ok"}], msg="The standalone returned database must pass SQLite integrity checking.")
    check.equal(facts["foreign_keys"], [], msg="No captured relation may refer to an absent row.")
    check.equal(facts["journal_mode"], [{"journal_mode": "delete"}], msg="The returned recording must not depend on a hidden WAL sidecar.")
    check.equal(facts["links"], [], msg="No model or host decision exists in this scenario, so no attribution link may be fabricated.")
    check.is_true(all(row["resolution"] == "unknown" for row in facts["operations"]),
                  msg="The native method boundary alone is not a transport/cache resolution witness.")


def check_complete_receipts(receipts):
    for receipt in receipts:
        assert receipt.start is not None and receipt.terminal is not None, "Completeness claims require both explicit capture receipts."
        check.equal((receipt.start.status, receipt.terminal.status, receipt.losses),
                    ("accepted", "accepted", ()), msg="Both observations must be admitted with no silently omitted projection.")


def check_native_capture(facts, native_log, capture):
    selected = [event for event in facts["events"] if event["kind"] == "mcp.response"]
    check.equal(len(selected), len(native_log), msg="Every native list/call return must have exactly one terminal observation.")
    check.equal(len(facts["operations"]), len(native_log), msg="The recording must contain one operation per witnessed native invocation.")
    check.is_true(all(entry.get("observer_same_object") is True for entry in native_log),
                  msg="Decoration must return the exact native SDK objects before FastMCP's higher-level parsing.")
    if capture == "content":
        check.equal([event["body"] for event in selected], [entry["result"] for entry in native_log],
                    msg="Captured result bodies must equal independent SDK projections, including empty arrays and structured content.")
    else:
        check.is_true(all(event["body"] is None for event in facts["events"]),
                      msg="Metadata mode must retain the operation evidence without content bodies.")


@pytest.mark.parametrize("capture", ["content", "metadata"])
def test_schema_text_structured_empty_repeat(evidence_path, capture, run_sdk_async):
    recorder, path = make_recorder(evidence_path, capture)

    async def exercise(case, observed):
        async with native_client(case, recorder if observed else None) as (client, native, receipts):
            declarations = project(await client.list_tools())
            results = []
            for name, arguments in [("echo", {"text": "unicode ✓ and exact payload"}),
                                    ("structured", {"a": 7, "b": 2}),
                                    ("empty", {}), ("echo", {"text": "unicode ✓ and exact payload"})]:
                results.append(project(await client.call_tool(name, arguments)))
            host = {"declarations": declarations, "results": results}
            return host, native, receipts

    async def scenario():
        plain = await exercise(evidence_path / "plain", False)
        with recorder.turn("FastMCP tools over stdio"):
            observed = await exercise(evidence_path / "observed", True)
        return plain, observed

    try:
        plain, observed = run_sdk_async(scenario(), timeout=45)
    finally:
        facts = finish(recorder, path)
    check.equal(observed[0], plain[0], msg="Schemas and all higher-level FastMCP results must be unchanged by recording.")
    declarations = {tool["name"]: tool for tool in observed[0]["declarations"]}
    check.equal(set(declarations), {"echo", "structured", "empty", "fail"}, msg="Real server tools must cross the schema discovery boundary.")
    check.equal(declarations["structured"]["inputSchema"]["required"], ["a", "b"], msg="Input requirements must survive real schema serialization.")
    check.equal(observed[0]["results"][1]["structured_content"], {"sum": 9, "difference": 5}, msg="Native structured output must remain the independently expected arithmetic result.")
    check.equal(observed[0]["results"][2]["content"], [], msg="An explicitly empty native result must not become missing or invented content.")
    check.equal(ledger(evidence_path / "observed/server.jsonl"), ledger(evidence_path / "plain/server.jsonl"),
                msg="The independent server ledger must show the same requests and tool executions in both modes.")
    check.equal(len(observed[1]), 5, msg="One listing and four explicit tool calls require five native invocations.")
    check_native_capture(facts, observed[1], capture)
    check_complete_receipts(observed[2])
    check_closed(facts)
    if capture == "content":
        artifact = next(event["payload_ref"] for event in facts["events"] if event["kind"] == "mcp.response")
        public_payload = inspect.payload(path, facts["run_id"], artifact, max_bytes=32768)
        assert public_payload["ok"] is True, "Payload comparison requires a successful public-reader result, never an error envelope."
        check.equal(json.loads(public_payload["data"]["text"]), observed[1][0]["result"],
                    msg="The public reader must return exactly the independently witnessed native discovery body.")
        check.equal(public_payload["data"]["next_offset"], None,
                    msg="The payload comparison must cover the whole small discovery body without a hidden cutoff.")


def test_native_tool_error_preserves_high_level_exception(evidence_path, run_sdk_async):
    recorder, path = make_recorder(evidence_path)

    async def exercise(case, observed):
        async with native_client(case, recorder if observed else None, mode="legacy") as (client, native, receipts):
            await client.list_tools()
            try:
                await client.call_tool("fail", {"message": "owned deliberate tool failure"})
            except ToolError as error:
                failure = {"type": type(error).__name__, "message": str(error)}
            else:
                pytest.fail("FastMCP must raise its documented ToolError for this native server failure.")
            recovered = project(await client.call_tool("echo", {"text": "valid neighbor after error"}))
            return failure, recovered, native, receipts

    async def scenario():
        plain = await exercise(evidence_path / "plain", False)
        with recorder.turn("FastMCP legacy native error"):
            observed = await exercise(evidence_path / "observed", True)
        return plain, observed

    try:
        plain, observed = run_sdk_async(scenario(), timeout=45)
    finally:
        facts = finish(recorder, path)
    check.equal(observed[:2], plain[:2], msg="The same native high-level error and successful neighbor must reach the host in both modes.")
    check.equal([row["status"] for row in facts["operations"]], ["ok", "tool_error", "ok"],
                msg="A returned native isError result must be classified as tool_error before FastMCP raises its facade exception.")
    check.equal(ledger(evidence_path / "observed/server.jsonl"), ledger(evidence_path / "plain/server.jsonl"),
                msg="Error observation may not add a retry, drop the next call, or alter server arguments.")
    check_native_capture(facts, observed[2], "content")
    check_complete_receipts(observed[3])
    check_closed(facts)


@pytest.mark.parametrize("cache_enabled", [False, True])
def test_framework_cache_boundary_and_bypass_neighbor(evidence_path, cache_enabled, run_sdk_async):
    recorder, path = make_recorder(evidence_path)

    async def exercise(case, observed):
        async with native_client(case, recorder if observed else None, cache=cache_enabled) as (client, native, receipts):
            first = await client.list_tools_mcp()
            second = await client.list_tools_mcp()
            bypass = await client.list_tools_mcp(cache_mode="bypass")
            outcomes = {"first": project(first), "second": project(second), "bypass": project(bypass),
                        "same_cached_object": second is first}
            return outcomes, native, receipts

    async def scenario():
        plain = await exercise(evidence_path / "plain", False)
        with recorder.turn("FastMCP cache boundary"):
            observed = await exercise(evidence_path / "observed", True)
        return plain, observed

    try:
        plain, observed = run_sdk_async(scenario(), timeout=45)
    finally:
        facts = finish(recorder, path)
    expected_native = 2 if cache_enabled else 3
    check.equal(observed[0], plain[0], msg="The framework's own cache outcome and bypass behavior must remain unchanged by session decoration.")
    check.equal(observed[0]["first"], observed[0]["second"], msg="The second host discovery still returns the identical exposed declaration value.")
    check.equal(len(observed[1]), expected_native, msg="The native observer sees calls that reach ClientSession; a higher-level served cache entry bypasses that boundary.")
    check.equal(len(ledger(evidence_path / "observed/server.jsonl")), expected_native,
                msg="Independent server requests must agree with actual session invocations, including a forced cache bypass.")
    check.equal(ledger(evidence_path / "observed/server.jsonl"), ledger(evidence_path / "plain/server.jsonl"),
                msg="The observed client must issue exactly the same server requests as the plain client.")
    cache_ledger = json.loads((evidence_path / "observed/cache-store.json").read_text())
    hits = [entry for entry in cache_ledger if entry["action"] == "get" and entry["hit"]]
    check.equal(len(hits), 1 if cache_enabled else 0,
                msg="A public cache-store witness must positively expose a stored hit; absent network calls alone do not establish one.")
    if hits:
        check.equal(hits[0]["value"], observed[0]["second"],
                    msg="The positively witnessed stored declaration must equal the host-visible second result.")
    check.equal([event["kind"] for event in facts["events"]].count("cache.served"), 0,
                msg="A cache hit above this observation boundary must stay unclaimed until a host cache witness is attached.")
    check_native_capture(facts, observed[1], "content")
    check_complete_receipts(observed[2])
    check_closed(facts)


def test_reconnect_attaches_each_new_active_session_once(evidence_path, run_sdk_async):
    """The same host client owns both lifetimes; each native session needs attachment."""
    recorder, path = make_recorder(evidence_path)

    async def exercise(case, observed):
        case.mkdir(parents=True, exist_ok=False)
        transport = StdioTransport(command=sys.executable,
            args=["-I", "-B", str(Path(__file__).with_name("server.py")),
                  str(case / "server.jsonl")],
            keep_alive=False, log_file=case / "server.stderr")
        client = Client(transport, cache=False, timeout=15)
        sessions, native, receipts, host = [], [], [], []
        for index in range(2):
            with pytest.raises(RuntimeError, match="not connected"):
                client.session
            async with client:
                session = client.session
                sessions.append(session)
                witness_native(session, "list_tools", native)
                witness_native(session, "call_tool", native)
                if observed:
                    attach_observer(recorder, session, receipts, native)
                # Native nesting retains this active session; it is not a reconnect.
                async with client:
                    check.is_(client.session, session,
                              msg="Nested ownership retains one active session and does not call the attachment recipe again.")
                    declarations = project(await client.list_tools())
                    answer = project(await client.call_tool("echo", {"text": f"connection-{index}"}))
                check.is_(client.session, session,
                          msg="Leaving the inner native context must not close the host's outer session.")
                host.append({"declarations": declarations, "answer": answer})
            with pytest.raises(RuntimeError, match="not connected"):
                client.session
        check.is_not(sessions[0], sessions[1],
                     msg="Reopening the same disconnected client creates a fresh native session, so the host must attach it explicitly.")
        return host, native, receipts

    async def scenario():
        plain = await exercise(evidence_path / "plain", False)
        with recorder.turn("FastMCP reconnect attachment"):
            observed = await exercise(evidence_path / "observed", True)
        return plain, observed

    try:
        plain, observed = run_sdk_async(scenario(), timeout=45)
    finally:
        facts = finish(recorder, path)
    check.equal(observed[0], plain[0],
                msg="Explicit reattachment must preserve native schemas and results on both independently owned connections.")
    check.equal(ledger(evidence_path / "observed/server.jsonl"), ledger(evidence_path / "plain/server.jsonl"),
                msg="Recording cannot reopen, repeat, omit, or alter a request during the host's reconnect lifecycle.")
    check.equal(len(observed[1]), 4,
                msg="Each of two sessions performs one discovery and one call, with no duplicate wrapper invocation.")
    check_native_capture(facts, observed[1], "content")
    check_complete_receipts(observed[2])
    check_closed(facts)
