"""MCP adapter contracts over the installed native in-process SDK."""
import asyncio
from contextlib import closing, nullcontext
from functools import wraps
import hashlib
import inspect
import json
import logging
import sqlite3
from types import SimpleNamespace

import anyio
import httpx2
import mcp_types as types
from mcp import Client, MCPError
from mcp.client import CacheConfig
from mcp.server import Server
import pytest
import pytest_check as check

from mcp_flightrecorder import MISSING, Recorder
from mcp_flightrecorder.integrations._common import policy_retention
from mcp_flightrecorder.integrations.mcp_client import OperationEvidence, wrap_mcp


COVERAGE = {"producers": {"model": "test.model", "host": "test.host", "mcp": "test.mcp"}}
SCHEMA = {"type": "object", "properties": {"text": {"type": "string"}}}


@pytest.fixture
def recording(tmp_path):
    path = tmp_path / "mcp.sqlite"
    rec = Recorder(path, capture="content", coverage=COVERAGE)
    try:
        yield rec, path
    finally:
        closed = rec.close(timeout=3)
        assert closed.writer_stopped, "A test must reap its own recorder writer before releasing its temporary recording."


def accepted(receipt):
    assert receipt is not None and receipt.status == "accepted", (
        "The dependent evidence check requires an admitted public receipt, not an assumed operation."
    )
    return receipt


def finish(rec):
    closed = rec.close(timeout=3)
    assert closed.writer_stopped and closed.portable, "Persisted assertions require a joined writer and portable main file."
    return closed


def query(path, statement):
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        db.row_factory = sqlite3.Row
        return [dict(row) for row in db.execute(statement)]


def event_rows(path):
    rows = query(path, """SELECT e.*,a.body_text,a.stored_sha256,a.stored_bytes,a.observed_bytes,
        a.policy_id,a.redacted,a.truncated,a.omitted_reason FROM events e
        LEFT JOIN artifacts a ON a.artifact_id=e.payload_ref ORDER BY e.sequence""")
    for row in rows:
        row["details"] = json.loads(row["details_json"])
        row["body"] = None if row["body_text"] is None else json.loads(row["body_text"])
    return rows


def peer(*, on_call=None, on_list=None):
    """Only the service peer is substituted; Client and result models are native."""
    calls = {"list": [], "call": []}

    async def listing(context, params):
        calls["list"].append(params)
        if on_list is not None:
            return await on_list(context, params)
        return types.ListToolsResult(tools=[types.Tool(name="echo", input_schema=SCHEMA)])

    async def calling(context, params):
        calls["call"].append(params)
        if on_call is not None:
            return await on_call(context, params)
        text = (params.arguments or {}).get("text", "default")
        return types.CallToolResult(content=[types.TextContent(type="text", text=text)], is_error=False)

    return Server("mcp-test-peer", on_list_tools=listing, on_call_tool=calling), calls


def spy_method(monkeypatch, client, method):
    """Pass through to the real bound SDK method and expose its actual outcome."""
    native = getattr(client, method)
    calls, outcomes = [], []

    @wraps(native)
    def spy(*args, **kwargs):
        calls.append((args, kwargs))
        native_coroutine = native(*args, **kwargs)

        async def execute():
            try:
                value = await native_coroutine
            except BaseException as error:
                outcomes.append(error)
                raise
            outcomes.append(value)
            return value

        return execute()

    monkeypatch.setattr(client, method, spy)
    return calls, outcomes


@pytest.mark.parametrize("label,value", [
    ("client_id", None), ("client_id", 7), ("client_id", ""),
    ("client_id", "\ud800"), ("client_id", "x" * 257), ("client_id", "é" * 129),
    ("server_key", 7), ("server_key", ""), ("server_key", "\ud800"),
    ("server_key", "x" * 257), ("server_key", "é" * 129),
], ids=["client-null", "client-type", "client-empty", "client-unencodable", "client-long", "client-utf8-long",
        "server-type", "server-empty", "server-unencodable", "server-long", "server-utf8-long"])
def test_mcp_configuration_labels_validate_before_native_work(recording, label, value):
    rec, path = recording
    server, calls = peer()
    labels = {"client_id": "client", "server_key": None, label: value}
    with check.raises(ValueError, msg="Invalid recorder identity configuration must fail before client entry or native work."):
        wrap_mcp(rec, Client(server), **labels)
    check.equal(calls, {"list": [], "call": []}, msg="Label validation cannot invoke either SDK method or service handler.")
    finish(rec)
    check.equal(event_rows(path), [], msg="Rejected wrapper configuration must not manufacture captured operations.")


def test_mcp_exact_label_boundary_and_factory_signature(recording, run_sdk_async):
    rec, path = recording
    server, _ = peer()

    async def exercise():
        async with Client(server) as native:
            adapter = wrap_mcp(rec, native, client_id="é" * 128, server_key="s" * 256)
            check.is_false(inspect.iscoroutinefunction(adapter.call_tool),
                           msg="The public tool method must snapshot scope synchronously before returning its coroutine.")
            check.is_false(inspect.iscoroutinefunction(adapter.list_tools),
                           msg="Discovery must share the declared regular-factory invocation boundary.")
            check.equal(str(inspect.signature(adapter.list_tools)),
                        "(*, evidence=None, cursor=None, meta=None, cache_mode='use')",
                        msg="Discovery exposes only its exact native options and call-local evidence slot.")
            check.equal(str(inspect.signature(adapter.call_tool)),
                        "(name, arguments=None, read_timeout_seconds=None, progress_callback=None, *, evidence=None, decision=None, input_responses=None, request_state=None, meta=None)",
                        msg="The wrapper preserves native positional and keyword-only boundaries without whole-client proxy methods.")
            await adapter.call_tool("echo", {"text": "exact-boundary"})
    run_sdk_async(exercise())
    finish(rec)
    check.equal(query(path, "SELECT client_id,server_key FROM operations"),
                [{"client_id": "é" * 128, "server_key": "s" * 256}],
                msg="The accepted limit is UTF-8 bytes, including the exact 256-byte boundary.")


@pytest.mark.parametrize("bad_call", ["missing-name", "tool-extra", "list-positional", "list-extra"])
def test_mcp_native_argument_binding_stage_is_preserved(recording, bad_call):
    rec, path = recording
    server, calls = peer()
    native = Client(server)
    adapter = wrap_mcp(rec, native, client_id="client")
    invoke = {
        "missing-name": lambda target: target.call_tool(),
        "tool-extra": lambda target: target.call_tool("echo", unknown=True),
        "list-positional": lambda target: target.list_tools("cursor"),
        "list-extra": lambda target: target.list_tools(unknown=True),
    }[bad_call]
    for target in (native, adapter):
        with check.raises(TypeError, msg="Missing or unexpected native arguments fail at call construction, before any await."):
            invoke(target)
    check.equal(calls, {"list": [], "call": []}, msg="Argument binding errors cannot enter a client or reach its peer.")
    finish(rec)
    check.equal(event_rows(path), [], msg="Construction-time failures are not executed logical operations.")


@pytest.mark.parametrize("method", ["call_tool", "list_tools"])
def test_mcp_evidence_ownership_and_never_awaited_work(recording, monkeypatch, method):
    rec, path = recording
    server, _ = peer()
    native = Client(server)
    calls, _ = spy_method(monkeypatch, native, method)
    policy_calls = []

    def policy(kind, payload, context):
        policy_calls.append((kind, payload, context))
        return {"policy_id": "explicit-keep", "retained": payload}

    adapter = wrap_mcp(rec, native, client_id="client", retention_policy=policy)
    invoke = lambda evidence: (adapter.call_tool("echo", evidence=evidence) if method == "call_tool"
                               else adapter.list_tools(evidence=evidence))
    evidence = OperationEvidence()
    check.equal((evidence.start, evidence.terminal, evidence.losses), (None, None, ()),
                msg="An unused evidence handle contains only empty receipt and loss slots.")
    with check.raises(TypeError, msg="An unrelated evidence object is explicit instrumentation misuse before native execution."):
        invoke(object())
    coroutine = invoke(evidence)
    assert inspect.iscoroutine(coroutine), "The valid call factory must return a real Python coroutine that the host can await or close."
    try:
        with check.raises(ValueError, msg="A handle belongs to one invocation even when its first coroutine has not run."):
            invoke(evidence)
        check.equal(calls, [], msg="Factory creation cannot call even the native async method or allocate its coroutine.")
        check.equal(policy_calls, [], msg="Never-awaited work must not invoke its configured retention policy.")
        check.equal((rec.health().open_operations, rec.health().completion.used_slots), (0, 0),
                    msg="Unexecuted work owns no operation or completion reservation.")
    finally:
        coroutine.close()
    check.equal((evidence.start, evidence.terminal, evidence.losses), (None, None, ()),
                msg="Closing never-awaited work must not fabricate start or terminal receipts.")
    check.equal(policy_calls, [], msg="Closing abandoned work cannot run either policy projection.")
    finish(rec)
    check.equal(event_rows(path), [], msg="Neither valid abandoned factories nor evidence misuse constitutes native execution.")


def test_mcp_options_forward_unchanged_and_client_lifecycle_stays_owned(recording, monkeypatch, run_sdk_async):
    rec, path = recording
    server, server_calls = peer()
    lifecycle = []
    original_enter, original_exit = Client.__aenter__, Client.__aexit__

    async def enter(client):
        lifecycle.append("enter")
        return await original_enter(client)

    async def leave(client, *args):
        lifecycle.append("exit")
        return await original_exit(client, *args)

    monkeypatch.setattr(Client, "__aenter__", enter)
    monkeypatch.setattr(Client, "__aexit__", leave)

    async def exercise():
        native = Client(server)
        list_calls, list_results = spy_method(monkeypatch, native, "list_tools")
        tool_calls, tool_results = spy_method(monkeypatch, native, "call_tool")
        adapter = wrap_mcp(rec, native, client_id="client")
        check.equal(lifecycle, [], msg="Constructing the method adapter cannot enter or close the host's client.")
        check.is_false(any(hasattr(adapter, name) for name in ("__aenter__", "__aexit__", "close", "aclose")),
                       msg="The adapter exposes no client-lifecycle proxy that could obscure ownership.")
        meta = {"test-control": "native-meta"}
        arguments = {"text": "forwarded"}
        input_responses = {}

        async def progress(progress, total, message):
            return None

        async with native:
            evidence = OperationEvidence()
            result = await adapter.call_tool("echo", arguments, 3.0, progress,
                                             evidence=evidence, input_responses=input_responses,
                                             request_state=None, meta=meta)
            check.is_(result, tool_results[0], msg="Observation must return the exact object produced by the actual SDK method.")
            assert len(tool_calls) == 1, "One observed logical tool call must invoke the native method once."
            args, options = tool_calls[0]
            check.equal(args[0], "echo", msg="The tool name reaches the SDK unchanged.")
            check.is_(args[1], arguments, msg="The SDK receives the caller's original argument object, never the capture copy.")
            check.equal(args[2], 3.0, msg="The per-round native timeout is forwarded without a recorder policy.")
            check.is_(args[3], progress, msg="The progress callback remains the exact host callback.")
            check.equal(set(options), {"input_responses", "request_state", "meta"},
                        msg="Recorder evidence and decision parameters cannot leak into native keyword arguments.")
            check.is_(options["meta"], meta, msg="Native request metadata is forwarded without rewriting.")
            check.is_(options["input_responses"], input_responses, msg="Input-response routing remains owned by the native SDK.")
            page_evidence = OperationEvidence()
            page = await adapter.list_tools(evidence=page_evidence, cursor=None, meta=meta, cache_mode="bypass")
            check.is_(page, list_results[0], msg="Discovery also returns the exact native result object.")
            check.equal(list_calls[0][1], {"cursor": None, "meta": meta, "cache_mode": "bypass"},
                        msg="Every explicit discovery option is forwarded without merging pages or changing cache mode.")
            check.is_(list_calls[0][1]["meta"], meta, msg="Discovery metadata is not replaced by the evidence snapshot.")
            accepted(evidence.start)
            accepted(evidence.terminal)
            accepted(page_evidence.terminal)
            with check.raises(ValueError, msg="An already completed evidence object cannot be reused or overwrite its old receipts."):
                adapter.call_tool("echo", evidence=evidence)
            finish(rec)
            result_after_capture = await native.call_tool("echo", {"text": "still-host-owned"})
            check.equal(result_after_capture.content[0].text, "still-host-owned",
                        msg="Closing the recorder does not close a still-entered host client.")
            check.equal(lifecycle, ["enter"], msg="Only the host's explicit async context controls client lifetime.")
        check.equal(lifecycle, ["enter", "exit"], msg="Exactly the host's one entry and exit occur.")
    run_sdk_async(exercise())
    check.equal(len(server_calls["call"]), 2, msg="Capture adds no hidden native tool execution before or after shutdown.")
    starts = [row for row in event_rows(path) if row["kind"] == "mcp.request"]
    check.equal([row["body"]["arguments"] for row in starts],
                [{"text": "forwarded"}, {"cursor": None, "meta": {"test-control": "native-meta"}, "cache_mode": "bypass"}],
                msg="Tool argument digests exclude callback/transport options; discovery captures its declared option object.")


@pytest.mark.parametrize("invalid", ["arguments", "cursor"])
def test_mcp_native_value_validation_stays_at_execution(recording, monkeypatch, invalid, run_sdk_async):
    rec, path = recording
    server, _ = peer()

    async def exercise():
        async with Client(server) as native:
            method = "call_tool" if invalid == "arguments" else "list_tools"
            calls, outcomes = spy_method(monkeypatch, native, method)
            adapter = wrap_mcp(rec, native, client_id="client")
            invoke = lambda target, **extra: (target.call_tool("echo", "invalid-object-shape", **extra)
                                             if invalid == "arguments" else target.list_tools(cursor={"not": "a-cursor"}, **extra))
            native_coroutine = invoke(native)
            assert inspect.iscoroutine(native_coroutine), "The native SDK creates the coroutine before validating this value."
            with pytest.raises(Exception) as native_error:
                await native_coroutine
            evidence = OperationEvidence()
            observed_coroutine = invoke(adapter, evidence=evidence)
            assert inspect.iscoroutine(observed_coroutine), "The wrapper must preserve native execution-time validation stage."
            check.is_none(evidence.start, msg="Factory invocation does not prematurely capture or validate SDK values.")
            with pytest.raises(type(native_error.value)) as observed_error:
                await observed_coroutine
            check.is_(observed_error.value, outcomes[-1], msg="The wrapper propagates the actual SDK validation exception instance unchanged.")
            check.equal(len(calls), 2, msg="Native and observed attempts each call the actual SDK exactly once.")
            accepted(evidence.terminal)
    run_sdk_async(exercise())
    finish(rec)
    check.equal(len(query(path, "SELECT operation_id FROM operations")), 1,
                msg="Only the executed observed validation failure creates an operation; the native baseline creates none.")


@pytest.mark.parametrize("method", ["call_tool", "list_tools"], ids=["tool", "discovery"])
@pytest.mark.parametrize("execution_scope", ["other-turn", "unscoped"], ids=["execute-B", "execute-unscoped"])
@pytest.mark.parametrize("outcome", ["ok", "protocol_error", "cancelled"], ids=["return", "error", "cancellation"])
def test_mcp_factory_scope_and_submission_freeze_survive_scope_exit(
        recording, monkeypatch, run_sdk_async, method, execution_scope, outcome):
    rec, path = recording
    arguments, meta = {"text": "at-invocation"}, {"marker": ["at-invocation"]}
    evidence = OperationEvidence()
    identities = {}
    policy_calls = []

    def policy(kind, payload, context):
        policy_calls.append((kind, json.loads(json.dumps(payload)), context.copy()))
        return {"policy_id": "scoped-keep", "retained": payload}

    async def exercise():
        entered, release = asyncio.Event(), asyncio.Event()

        async def held(context, params):
            entered.set()
            await asyncio.wait_for(release.wait(), timeout=3)
            if outcome == "protocol_error":
                raise MCPError(-32602, "native scoped error")
            if method == "list_tools":
                return types.ListToolsResult(tools=[types.Tool(name="echo", input_schema=SCHEMA)])
            return types.CallToolResult(content=[types.TextContent(type="text", text="native-return")], is_error=False)

        server, _ = peer(**{"on_call" if method == "call_tool" else "on_list": held})
        async with Client(server) as native:
            calls, outcomes = spy_method(monkeypatch, native, method)
            adapter = wrap_mcp(rec, native, client_id="client", retention_policy=policy)
            with rec.turn("factory turn A") as turn_a:
                accepted(turn_a.receipt)
                identities["turn"] = turn_a.receipt.turn_id
                with rec.dispatch(action="execute", name="echo", arguments={}) as dispatch:
                    identities["dispatch"] = accepted(dispatch.receipt).event_id
                    pending = (adapter.call_tool("echo", arguments, evidence=evidence, meta=meta)
                               if method == "call_tool" else adapter.list_tools(evidence=evidence, meta=meta))
            check.equal(calls, [], msg="Exiting factory scope cannot invoke the SDK or construct its native coroutine.")
            check.equal(policy_calls, [], msg="Scope capture does not execute policy before the lazy work runs.")
            check.equal((rec.health().open_operations, rec.health().completion.used_slots), (0, 0),
                        msg="Lexical scope exit alone cannot start or complete deferred operations.")
            arguments["text"] = "at-execution"
            meta["marker"][0] = "at-execution"
            context = rec.turn("execution turn B") if execution_scope == "other-turn" else nullcontext()
            with context:
                ambient = rec.capture_scope()
                task = asyncio.create_task(pending)
                try:
                    await asyncio.wait_for(entered.wait(), timeout=3)
                    check.equal(accepted(evidence.start).turn_id, identities["turn"],
                                msg="Actual native execution belongs to invocation turn A after its lexical exit.")
                    check.equal(rec.capture_scope(), ambient,
                                msg="The observer must not restore a copied ambient Context over the host's current turn B.")
                    check.equal(rec.health().open_operations, 1,
                                msg="A gated real SDK request owns one live operation despite the factory scope having ended.")
                    arguments["text"] = "after-observation"
                    meta["marker"].append("after-observation")
                    if outcome == "cancelled":
                        task.cancel("the host cancelled its own work")
                    else:
                        release.set()
                    if outcome == "ok":
                        returned = await asyncio.wait_for(task, timeout=3)
                        check.is_(returned, outcomes[-1], msg="Scoped capture returns the exact actual SDK result after its gate opens.")
                    else:
                        error_type = asyncio.CancelledError if outcome == "cancelled" else MCPError
                        with pytest.raises(error_type) as caught:
                            await asyncio.wait_for(task, timeout=3)
                        check.is_(caught.value, outcomes[-1],
                                  msg="Terminal observation after scope exit preserves the native exception or cancellation object.")
                    check.equal(rec.capture_scope(), ambient,
                                msg="Completion/error/cancellation cannot replace the host's later ambient scope.")
                    check.equal(accepted(evidence.terminal).turn_id, identities["turn"],
                                msg="The terminal uses its accepted ticket's immutable turn rather than the ambient completion turn.")
                finally:
                    release.set()
                    if not task.done():
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
    run_sdk_async(exercise())
    finish(rec)
    rows = event_rows(path)
    starts = [row for row in rows if row["kind"] == "mcp.request"]
    assert len(starts) == 1, "The held native method must have exactly one recorded logical start."
    expected = {"text": "at-execution"} if method == "call_tool" else {
        "cursor": None, "meta": {"marker": ["at-execution"]}, "cache_mode": "use"}
    check.equal(starts[0]["body"]["arguments"], expected,
                msg="Capture observes changes before execution and freezes the submitted JSON against later caller mutation.")
    check.equal([item[0] for item in policy_calls], ["mcp.request", "mcp.response"],
                msg="Success, error and cancellation each expose one request and one terminal policy projection.")
    check.equal(policy_calls[0][1], {"arguments": expected},
                msg="Policy input freezes execution-time values under the invocation-time scope.")
    check.equal([item[2] for item in policy_calls],
                [{"client_id": "client", "server_key": None,
                  "method": "tools/call" if method == "call_tool" else "tools/list",
                  "name": "echo" if method == "call_tool" else None}] * 2,
                msg="Both policy contexts retain the original invocation's exact method and name.")
    check.equal(query(path, "SELECT turn_id,dispatch_event_id,link_basis,status,resolution,input_rounds FROM operations"),
                [{"turn_id": identities["turn"], "dispatch_event_id": identities["dispatch"],
                  "link_basis": "host_hook", "status": outcome, "resolution": "unknown", "input_rounds": None}],
                msg="Every terminal path retains factory dispatch and turn while making no hidden round/cache-source claim.")


def make_decision(rec):
    request = accepted(rec.observe("model.request", scope=rec.capture_scope(), payload={"messages": []},
                                   facts={"messages": []}))
    decision = accepted(rec.observe("model.tool_call", scope=rec.capture_scope(),
                                    payload={"arguments_raw": '{"text":"selected"}'},
                                    facts={"model_call_id": request.model_call_id, "name": "echo", "provider_ref": "ambiguous-provider-id"}))
    accepted(rec.observe("model.response", scope=rec.capture_scope(), payload={},
                         facts={"model_call_id": request.model_call_id}))
    return decision


@pytest.mark.parametrize("explicit_decision", [False, True], ids=["unassigned", "explicit-propagation"])
def test_mcp_rejected_inner_dispatch_cannot_recover_outer_association(recording, run_sdk_async, explicit_decision):
    rec, path = recording
    ids = {}

    async def exercise():
        server, _ = peer()
        async with Client(server) as native:
            adapter = wrap_mcp(rec, native, client_id="client")
            with rec.turn("nested dispatch"):
                decision = make_decision(rec) if explicit_decision else None
                ids["decision"] = None if decision is None else decision.decision_id
                with rec.dispatch(action="execute", name="echo", arguments={}) as outer:
                    ids["outer"] = accepted(outer.receipt).event_id
                    with rec.dispatch(action="invalid-action", name="echo", arguments={}) as inner:
                        assert inner.receipt.status == "rejected", "The inner dispatch must actually be rejected before proving disabled association."
                        inner_call = adapter.call_tool("echo", {}, decision=decision)
                    outer_call = adapter.call_tool("echo", {})
            await inner_call
            await outer_call
    run_sdk_async(exercise())
    finish(rec)
    check.equal(query(path, "SELECT decision_id,dispatch_event_id,link_basis FROM operations ORDER BY rowid"),
                [{"decision_id": ids["decision"], "dispatch_event_id": None,
                  "link_basis": "propagated" if explicit_decision else "none"},
                 {"decision_id": None, "dispatch_event_id": ids["outer"], "link_basis": "host_hook"}],
                msg="Rejected inner dispatch is an explicit association boundary; only a separately captured valid outer call uses its event.")


@pytest.mark.parametrize("decision_kind", ["accepted", "foreign", "rejected", "missing-id", "empty-id", "bad-id", "bad-encoding", "long-id"],
                         ids=["owned", "foreign-structural", "rejected", "null-id", "empty-id", "nonstring-id", "unencodable-id", "oversized-id"])
@pytest.mark.parametrize("dispatch_present", [False, True], ids=["no-dispatch", "valid-dispatch"])
def test_mcp_explicit_decision_structure_and_dispatch_precedence(
        recording, run_sdk_async, decision_kind, dispatch_present):
    rec, path = recording
    evidence = OperationEvidence()
    ids = {}

    async def exercise():
        server, _ = peer()
        async with Client(server) as native:
            adapter = wrap_mcp(rec, native, client_id="client")
            with rec.turn("explicit attribution"):
                owned = make_decision(rec)
                supplied = {
                    "accepted": owned,
                    "foreign": SimpleNamespace(status="accepted", decision_id="foreign-symbolic-decision"),
                    "rejected": SimpleNamespace(status="rejected", decision_id=owned.decision_id),
                    "missing-id": SimpleNamespace(status="accepted", decision_id=None),
                    "empty-id": SimpleNamespace(status="accepted", decision_id=""),
                    "bad-id": SimpleNamespace(status="accepted", decision_id=7),
                    "bad-encoding": SimpleNamespace(status="accepted", decision_id="\ud800"),
                    "long-id": SimpleNamespace(status="accepted", decision_id="x" * 257),
                }[decision_kind]
                dispatch = rec.dispatch(action="execute", name="echo", arguments={},
                                        decision_event_ids=[owned.event_id]) if dispatch_present else nullcontext()
                with dispatch as entered:
                    ids["dispatch"] = accepted(entered.receipt).event_id if dispatch_present else None
                    result = await adapter.call_tool("echo", {"text": "returned"}, evidence=evidence, decision=supplied)
                check.equal(result.content[0].text, "returned",
                            msg="An invalid recorder-only decision association cannot prevent an otherwise valid native tool call.")
                ids["decision"] = owned.decision_id if decision_kind == "accepted" else "foreign-symbolic-decision"
    run_sdk_async(exercise())
    finish(rec)
    valid = decision_kind in ("accepted", "foreign")
    expected = {"decision_id": ids["decision"] if valid else None,
                "dispatch_event_id": ids["dispatch"] if valid else None,
                "link_basis": ("host_hook" if dispatch_present else "propagated") if valid else "none"}
    check.equal(query(path, "SELECT decision_id,dispatch_event_id,link_basis FROM operations"), [expected],
                msg="Usable explicit receipts preserve symbolic IDs; invalid receipts leave all attribution unassigned, even inside a dispatch.")
    check.equal(evidence.losses, () if valid else ("invalid_decision",),
                msg="The adapter checks only public receipt structure and cannot falsely claim knowledge of recorder ownership.")
    start = next(row for row in event_rows(path) if row["kind"] == "mcp.request")
    check.equal(start["details"]["complete"], valid,
                msg="An unusable explicit association must persist incomplete start evidence, not only an ephemeral warning.")


@pytest.mark.parametrize("mode", ["native", "enabled", "closed", "failed"],
                         ids=["no-recorder", "capture-enabled", "capture-closed", "writer-failed"])
@pytest.mark.parametrize("outcome", ["return", "error", "cancelled"])
def test_mcp_capture_state_never_replaces_native_outcome(
        recording, monkeypatch, run_sdk_async, mode, outcome):
    rec, path = recording
    if mode == "closed":
        finish(rec)
    elif mode == "failed":
        def fail_write(connection, bundle):
            raise sqlite3.OperationalError("test-owned integration writer failure")
        monkeypatch.setattr(rec, "_persist_bundle", fail_write)
        accepted(rec.observe("cache.served", scope=rec.capture_scope(), facts={"owner": "host"}))
        with rec._condition:
            assert rec._condition.wait_for(lambda: rec._state == "failed", timeout=3), (
                "The real SQLite writer must fail before this host-transparency control begins."
            )
    terminal_attempts = []
    original_finish = rec.finish_operation

    def notice_finish(*args, **kwargs):
        terminal_attempts.append((args, kwargs))
        return original_finish(*args, **kwargs)

    monkeypatch.setattr(rec, "finish_operation", notice_finish)
    evidence = OperationEvidence()
    policy_calls = []

    def failed_policy(kind, payload, context):
        policy_calls.append(kind)
        raise ValueError("synthetic-private-policy-error")

    async def exercise():
        entered, release, neighbor_entered, neighbor_release = (asyncio.Event() for _ in range(4))

        async def calling(context, params):
            if params.arguments.get("text") == "neighbor":
                return types.CallToolResult(content=[types.TextContent(type="text", text="neighbor-alive")])
            entered.set()
            if outcome == "cancelled":
                await asyncio.wait_for(release.wait(), timeout=3)
            if outcome == "error":
                raise MCPError(-32602, "actual SDK peer error")
            return types.CallToolResult(content=[types.TextContent(type="text", text="native-result")], is_error=False)

        async def unrelated():
            neighbor_entered.set()
            await asyncio.wait_for(neighbor_release.wait(), timeout=3)
            return "unrelated-result"

        server, server_calls = peer(on_call=calling)
        async with Client(server) as native:
            calls, actual = spy_method(monkeypatch, native, "call_tool")
            target = native if mode == "native" else wrap_mcp(
                rec, native, client_id="client",
                retention_policy=failed_policy if mode == "enabled" else None)
            neighbor = asyncio.create_task(unrelated())
            await asyncio.wait_for(neighbor_entered.wait(), timeout=3)
            kwargs = {} if mode == "native" else {"evidence": evidence}
            task = asyncio.create_task(target.call_tool("echo", {"text": "probe"}, **kwargs))
            try:
                await asyncio.wait_for(entered.wait(), timeout=3)
                if outcome == "cancelled":
                    task.cancel("original host cancellation")
                if outcome == "return":
                    value = await asyncio.wait_for(task, timeout=3)
                    check.is_(value, actual[-1], msg="Absent, healthy, closed and failed capture return the same actual SDK object.")
                else:
                    error_type = asyncio.CancelledError if outcome == "cancelled" else MCPError
                    with pytest.raises(error_type) as caught:
                        await asyncio.wait_for(task, timeout=3)
                    check.is_(caught.value, actual[-1],
                              msg="Recorder state cannot replace the SDK's original exception or cancellation instance.")
                    if outcome == "cancelled":
                        check.equal(caught.value.args, ("original host cancellation",),
                                    msg="Capture preserves the caller's cancellation payload as well as its type.")
                check.equal(len(calls), 1, msg="Capture success/failure cannot initiate an extra native request or retry.")
                check.is_false(neighbor.done(), msg="Closing or failing the recorder cannot cancel unrelated host work.")
                neighbor_release.set()
                check.equal(await neighbor, "unrelated-result", msg="The host's independently released task finishes normally.")
                neighbor_value = await native.call_tool("echo", {"text": "neighbor"})
                check.equal(neighbor_value.content[0].text, "neighbor-alive",
                            msg="The host's native client stays usable after every observed result, error, or cancellation.")
                check.equal(len(server_calls["call"]), 2,
                            msg="Only the explicit probe and subsequent host-owned neighbor call reach the actual peer.")
            finally:
                release.set()
                neighbor_release.set()
                for owned in (task, neighbor):
                    if not owned.done():
                        owned.cancel()
                await asyncio.gather(task, neighbor, return_exceptions=True)
    run_sdk_async(exercise())
    if mode == "enabled":
        accepted(evidence.start)
        accepted(evidence.terminal)
        check.equal(len(terminal_attempts), 1, msg="An admitted start has one attempted terminal for the actual native outcome.")
        finish(rec)
        check.equal(policy_calls, ["mcp.request", "mcp.response"],
                    msg="Failed policies still run once per available observation for native return, error and cancellation.")
        check.equal(evidence.losses, ("retention_policy_failed",),
                    msg="Policy faults have one bounded side-channel reason without replacing the actual native outcome.")
        check.equal([(row["body_text"], row["policy_id"], row["omitted_reason"]) for row in event_rows(path)],
                    [(None, "adapter_policy_error", "retention_policy_failed")] * 2,
                    msg="Failed policy request and native terminal bodies are omitted before capture without a raw retry.")
        check.equal((rec.health().open_operations, rec.health().completion.used_slots), (0, 0),
                    msg="Native error and cancellation with failed policies still release their operation reservation.")
        check.equal(query(path, "SELECT status,resolution,input_rounds FROM operations"),
                    [{"status": {"return": "ok", "error": "protocol_error", "cancelled": "cancelled"}[outcome],
                      "resolution": "unknown", "input_rounds": None}],
                    msg="The recorded operation reflects the native boundary outcome without invented cache or round attribution.")
    elif mode in ("closed", "failed"):
        assert evidence.start is not None, "Rejected capture must still preserve its explicit core start receipt."
        check.equal(evidence.start.reason, "closed" if mode == "closed" else "writer_failed",
                    msg="A capture failure retains its detailed core reason without replacing the host result.")
        check.is_none(evidence.start.ticket, msg="A rejected start has no completion reservation to hand to finish_operation.")
        check.is_none(evidence.terminal, msg="The adapter cannot invent a terminal receipt when no start ticket was accepted.")
        check.equal(terminal_attempts, [], msg="No nonexistent or rejected operation ticket is ever finished.")
        check.equal(evidence.losses, ("capture_rejected",), msg="Repeated capture failure has one bounded distinct side-channel code.")
    else:
        finish(rec)
        check.equal(event_rows(path), [], msg="The explicitly native baseline contains no observed operation.")


@pytest.mark.parametrize("outcome", ["tool-error", "native-timeout", "timeout-code", "protocol-error"])
def test_mcp_native_error_outcomes_have_exact_terminal_mapping(recording, monkeypatch, run_sdk_async, outcome):
    rec, path = recording
    evidence = OperationEvidence()

    async def exercise():
        entered, release = asyncio.Event(), asyncio.Event()

        async def calling(context, params):
            entered.set()
            if outcome == "native-timeout":
                await asyncio.wait_for(release.wait(), timeout=3)
            if outcome == "timeout-code":
                raise MCPError(types.REQUEST_TIMEOUT, "peer timeout explanation")
            if outcome == "protocol-error":
                raise MCPError(-32602, "peer protocol explanation")
            return types.CallToolResult(content=[types.TextContent(type="text", text="tool's retained error")], is_error=True)

        server, _ = peer(on_call=calling)
        async with Client(server) as native:
            calls, actual = spy_method(monkeypatch, native, "call_tool")
            adapter = wrap_mcp(rec, native, client_id="client")
            try:
                pending = adapter.call_tool("echo", {}, read_timeout_seconds=0.1,
                                            evidence=evidence)
                if outcome == "tool-error":
                    returned = await pending
                    check.is_(returned, actual[-1], msg="An is_error result is a returned native value, not a raised recorder exception.")
                else:
                    with pytest.raises(MCPError) as caught:
                        await pending
                    check.is_(caught.value, actual[-1], msg="The actual native MCP exception survives terminal recording unchanged.")
                    if outcome == "native-timeout":
                        check.is_true(entered.is_set() and not release.is_set(),
                                      msg="The actual SDK timeout fires while the real peer is still held, before test cleanup releases it.")
                    check.equal(caught.value.code, -32602 if outcome == "protocol-error" else types.REQUEST_TIMEOUT,
                                msg="Timeout-code and other protocol errors remain distinct exposed native outcomes.")
                check.equal(len(calls), 1, msg="Terminal classification cannot add a native retry.")
            finally:
                release.set()
    run_sdk_async(exercise())
    finish(rec)
    expected_status = {"tool-error": "tool_error", "native-timeout": "timeout",
                       "timeout-code": "timeout", "protocol-error": "protocol_error"}[outcome]
    expected_code = None if outcome == "tool-error" else -32602 if outcome == "protocol-error" else types.REQUEST_TIMEOUT
    check.equal(query(path, "SELECT status,error_code,resolution,input_rounds FROM operations"),
                [{"status": expected_status, "error_code": expected_code, "resolution": "unknown", "input_rounds": None}],
                msg="The logical operation keeps the declared native outcome/code and leaves unobserved source/round information unknown.")
    terminal = next(row for row in event_rows(path) if row["kind"] == "mcp.response")
    if outcome == "tool-error":
        check.equal(terminal["body"]["content"][0]["text"], "tool's retained error",
                    msg="Returned tool-error content remains available for the host and retained recording.")
        check.is_none(terminal["details"]["error"], msg="A returned tool error is not fabricated as a client-side exception.")
    else:
        check.equal(terminal["details"]["error"], {"origin": "client", "type": "MCPError", "code": expected_code},
                    msg="Control metadata contains only bounded origin/type/code, not the native error message.")
        check.equal(set(terminal["body"]["error"]), {"type", "message"},
                    msg="An exposed native error message belongs only in ordinarily retained payload content.")


@pytest.mark.parametrize("exception_kind", ["httpx", "end-of-stream", "broken-resource", "closed-resource", "timeout", "local", "long-type", "unencodable-message"])
def test_mcp_labelled_injected_exception_classification_preserves_actual_classes(
        recording, monkeypatch, run_sdk_async, exception_kind):
    """Class mapping/pass-through control; this does not qualify HTTP/stdio transport."""
    rec, path = recording
    error = {
        "httpx": lambda: httpx2.ConnectError("injected transport explanation"),
        "end-of-stream": anyio.EndOfStream,
        "broken-resource": anyio.BrokenResourceError,
        "closed-resource": anyio.ClosedResourceError,
        "timeout": lambda: TimeoutError("injected timeout explanation"),
        "local": lambda: ValueError("injected local explanation"),
        "long-type": lambda: type("LocalFailure" * 10, (ValueError,), {})("bounded label"),
        "unencodable-message": lambda: RuntimeError("unencodable-\ud800"),
    }[exception_kind]()
    evidence = OperationEvidence()

    async def exercise():
        server, server_calls = peer()
        async with Client(server) as native:
            actual_method = native.call_tool

            @wraps(actual_method)
            async def injected(*args, **kwargs):
                await actual_method(*args, **kwargs)
                raise error

            monkeypatch.setattr(native, "call_tool", injected)
            with pytest.raises(type(error)) as native_error:
                await native.call_tool("echo", {})
            adapter = wrap_mcp(rec, native, client_id="client")
            with pytest.raises(type(error)) as observed_error:
                await adapter.call_tool("echo", {}, evidence=evidence)
            check.is_(native_error.value, error, msg="The labelled native spy control exposes the exact injected actual exception instance.")
            check.is_(observed_error.value, error, msg="The adapter cannot replace the same exposed exception with a projection or recording failure.")
            check.equal(len(server_calls["call"]), 2,
                        msg="Both labelled controls first traverse the real Client/Server method once; capture adds no call.")
    run_sdk_async(exercise())
    finish(rec)
    transport = exception_kind in ("httpx", "end-of-stream", "broken-resource", "closed-resource")
    status = "transport_error" if transport else "timeout" if exception_kind == "timeout" else "local_error"
    terminal = next(row for row in event_rows(path) if row["kind"] == "mcp.response")
    check.equal(terminal["state"], status, msg="The fixed exception-class map is independent of the error text or recorder state.")
    check.equal(terminal["details"]["error"], {"origin": "client", "type": type(error).__name__[:64], "code": None},
                msg="Exception control metadata uses a bounded valid type and never stores messages or fabricated protocol codes.")
    check.equal(evidence.losses, ("projection_failed",) if exception_kind == "unencodable-message" else (),
                msg="Only failed optional error-body projection is observation loss; a native error itself is an observed outcome.")
    if exception_kind == "unencodable-message":
        check.is_none(terminal["body"], msg="Unsafe error text becomes unavailable payload instead of a corrupt JSON artifact.")
        check.is_false(terminal["details"]["complete"], msg="A failed error-body projection persists incomplete evidence.")


@pytest.mark.parametrize("method", ["call_tool", "list_tools"], ids=["tool-arguments", "discovery-meta"])
def test_mcp_non_json_capture_preserves_native_consumption(recording, monkeypatch, run_sdk_async, method):
    rec, path = recording
    evidence = OperationEvidence()
    policy_calls = []

    def policy(kind, payload, context):
        policy_calls.append(kind)
        return {"policy_id": "available-only", "retained": payload}

    async def exercise():
        server, server_calls = peer()
        async with Client(server) as native:
            calls, actual = spy_method(monkeypatch, native, method)
            adapter = wrap_mcp(rec, native, client_id="client", retention_policy=policy)
            original = {"text": "native-value", "extra": (1, 2)}
            invoke = lambda target, **kwargs: (target.call_tool("echo", original, **kwargs)
                                             if method == "call_tool" else target.list_tools(meta=original, **kwargs))
            baseline = await invoke(native)
            returned = await invoke(adapter, evidence=evidence)
            check.equal(returned, baseline, msg="Declining non-ordinary JSON capture cannot change native SDK serialization or output.")
            check.is_(returned, actual[-1], msg="Unavailable input capture still returns the exact SDK result object.")
            check.equal(len(calls), 2, msg="The native baseline and wrapper each execute one actual SDK method.")
            if method == "call_tool":
                check.equal(server_calls["call"][0].arguments, server_calls["call"][1].arguments,
                            msg="Capture cannot consume or rewrite SDK-supported non-ordinary argument values before submission.")
                check.is_(calls[1][0][1], original, msg="The original non-JSON argument container reaches the SDK unchanged.")
            else:
                check.is_(calls[1][1]["meta"], original, msg="Discovery forwards the original metadata even when capture declines its type.")
    run_sdk_async(exercise())
    finish(rec)
    start = next(row for row in event_rows(path) if row["kind"] == "mcp.request")
    check.equal(evidence.losses, ("projection_failed",), msg="Uncapturable JSON input has one bounded projection warning.")
    check.is_false(start["details"]["complete"], msg="Unavailable submission capture must persist an incomplete start.")
    check.is_none(start["details"]["args_digest"]["value"], msg="Unknown original arguments cannot receive a complete comparison token.")
    check.is_none(start["body"], msg="The adapter cannot fabricate an argument object from an unsupported exposed value.")
    check.equal(policy_calls, ["mcp.response"],
                msg="An unavailable request projection skips policy while its genuinely exposed terminal still invokes it.")


def test_mcp_null_arguments_are_observed_not_missing(recording, run_sdk_async):
    rec, path = recording

    async def exercise():
        server, _ = peer()
        async with Client(server) as native:
            await wrap_mcp(rec, native, client_id="client").call_tool("echo", None)
    run_sdk_async(exercise())
    finish(rec)
    start = next(row for row in event_rows(path) if row["kind"] == "mcp.request")
    check.equal(start["body"], {"arguments": None}, msg="Explicit JSON null arguments remain observed null instead of an invented empty object.")
    check.is_true(start["details"]["complete"], msg="An exposed null value is complete observation, unlike unavailable input.")


def test_mcp_result_projection_failure_is_partial_success(recording, monkeypatch, run_sdk_async):
    rec, path = recording
    evidence = OperationEvidence()
    actual = []
    policy_calls = []

    def policy(kind, payload, context):
        policy_calls.append(kind)
        return {"policy_id": "available-only", "retained": payload}

    async def exercise():
        server, calls = peer()
        async with Client(server) as native:
            real = native.call_tool

            @wraps(real)
            async def unprojectable(*args, **kwargs):
                result = await real(*args, **kwargs)
                actual.append(result)

                def failed_dump(**kwargs):
                    raise ValueError("test-owned trusted model projection failure")

                object.__setattr__(result, "model_dump", failed_dump)
                return result

            monkeypatch.setattr(native, "call_tool", unprojectable)
            result = await wrap_mcp(rec, native, client_id="client", retention_policy=policy).call_tool("echo", {}, evidence=evidence)
            check.is_(result, actual[0], msg="A trusted model projection failure must preserve its exact successfully returned SDK object.")
            check.equal(len(calls["call"]), 1, msg="Projection failure cannot retry the native operation.")
    run_sdk_async(exercise())
    closed = finish(rec)
    terminal = next(row for row in event_rows(path) if row["kind"] == "mcp.response")
    check.equal(terminal["state"], "ok", msg="Observer projection failure must never relabel a successful SDK outcome as a host error.")
    check.is_false(terminal["details"]["complete"], msg="Projection loss persists incomplete evidence instead of a falsely complete empty result.")
    check.is_none(terminal["body"], msg="An unavailable native result has no manufactured retained body.")
    check.is_none(terminal["details"]["result_text_digest"]["value"], msg="Failed result projection cannot claim a complete text comparison token.")
    check.equal(evidence.losses, ("projection_failed",), msg="The side channel records a distinct bounded projection-loss code.")
    check.equal(policy_calls, ["mcp.request"],
                msg="A failed terminal projection is MISSING and cannot call policy with an invented body.")
    check.equal(closed.run_close_state, "incomplete", msg="Persisted capture loss prevents a clean-run claim even though the native call succeeded.")


def test_mcp_capacity_rejection_never_finishes_a_nonexistent_ticket(tmp_path, monkeypatch, run_sdk_async):
    path = tmp_path / "capacity.sqlite"
    rec = Recorder(path, capture="content", coverage=COVERAGE, limits={"completion_items": 1})
    first, second = OperationEvidence(), OperationEvidence()
    attempts = []
    original = rec.finish_operation

    def finish_notice(*args, **kwargs):
        attempts.append(args)
        return original(*args, **kwargs)

    monkeypatch.setattr(rec, "finish_operation", finish_notice)

    async def exercise():
        entered, release = asyncio.Event(), asyncio.Event()

        async def calling(context, params):
            if params.arguments["text"] == "held":
                entered.set()
                await asyncio.wait_for(release.wait(), timeout=3)
            return types.CallToolResult(content=[types.TextContent(type="text", text=params.arguments["text"])])

        server, calls = peer(on_call=calling)
        async with Client(server) as native:
            adapter = wrap_mcp(rec, native, client_id="client")
            task = asyncio.create_task(adapter.call_tool("echo", {"text": "held"}, evidence=first))
            try:
                await asyncio.wait_for(entered.wait(), timeout=3)
                accepted(first.start)
                returned = await adapter.call_tool("echo", {"text": "still-executed"}, evidence=second)
                check.equal(returned.content[0].text, "still-executed", msg="Full capture completion capacity does not suppress valid native work.")
                assert second.start is not None, "Capacity rejection must retain its public start receipt."
                check.equal((second.start.status, second.start.reason, second.start.ticket), ("rejected", "capacity", None),
                            msg="The rejected operation creates no ticket while the first real SDK call owns the only reservation.")
                check.equal(attempts, [], msg="A rejected second start must not finish an invented ticket before the held first call completes.")
                check.is_none(second.terminal, msg="The receipt side channel cannot pretend a rejected operation had a terminal event.")
                check.equal(second.losses, ("capture_rejected",), msg="Capacity loss remains visible without changing the native result.")
                release.set()
                await asyncio.wait_for(task, timeout=3)
                check.equal(len(attempts), 1, msg="Only the real admitted first ticket reaches the terminal API once.")
                check.equal(len(calls["call"]), 2, msg="Capture pressure adds no retries and removes no native operation.")
            finally:
                release.set()
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
    try:
        run_sdk_async(exercise())
        finish(rec)
        check.equal(len(query(path, "SELECT operation_id FROM operations")), 1,
                    msg="The database contains only the single accepted operation, with no synthetic rejected-start terminal.")
        check.equal(rec.health().open_operations, 0, msg="The admitted ticket releases its reservation after its real terminal commits.")
    finally:
        rec.close(timeout=3)


def test_mcp_discovery_preserves_ordered_pages_cursor_and_metadata(recording, monkeypatch, run_sdk_async):
    rec, path = recording
    evidence = [OperationEvidence(), OperationEvidence()]
    projected = []

    async def listing(context, params):
        cursor = None if params is None else params.cursor
        if cursor is None:
            return types.ListToolsResult(
                tools=[types.Tool(name="first", description="first declaration", input_schema=SCHEMA),
                       types.Tool(name="second", input_schema={"type": "object", "properties": {"count": {"type": "integer"}}})],
                next_cursor="page-two", ttl_ms=60000, cache_scope="private", meta={"page": 1})
        assert cursor == "page-two", "The second peer handler must receive the actual exposed cursor, not an invented paging token."
        return types.ListToolsResult(tools=[types.Tool(name="last", input_schema={"type": "object"})],
                                     ttl_ms=1000, cache_scope="public", meta={"page": 2})

    async def exercise():
        server, counter = peer(on_list=listing)
        async with Client(server) as native:
            calls, actual = spy_method(monkeypatch, native, "list_tools")
            adapter = wrap_mcp(rec, native, client_id="client", server_key="server")
            first = await adapter.list_tools(evidence=evidence[0])
            check.equal(len(counter["list"]), 1, msg="The first discovery call must not automatically fetch its exposed continuation page.")
            second = await adapter.list_tools(evidence=evidence[1], cursor=first.next_cursor,
                                              meta={"page-request": 2}, cache_mode="bypass")
            check.is_(first, actual[0], msg="The first page retains exact native result identity.")
            check.is_(second, actual[1], msg="The cursor page is returned independently, without merging or replacing the native object.")
            check.equal(len(calls), 2, msg="Two explicit page requests create exactly two high-level SDK method invocations.")
            check.equal(len(counter["list"]), 2, msg="The observer adds no hidden automatic discovery pages.")
            projected.extend(result.model_dump(mode="json", by_alias=True, exclude_unset=True) for result in (first, second))
    run_sdk_async(exercise())
    finish(rec)
    rows = event_rows(path)
    results = [row["body"] for row in rows if row["kind"] == "mcp.response"]
    check.equal(results, projected, msg="Each persisted discovery body is the exposed native JSON projection, including schema and metadata.")
    check.equal([[tool["name"] for tool in result["tools"]] for result in results], [["first", "second"], ["last"]],
                msg="Declared tool order and page boundaries survive independently instead of a global current catalog.")
    check.equal((results[0]["nextCursor"], results[0]["ttlMs"], results[0]["cacheScope"], results[0]["_meta"]),
                ("page-two", 60000, "private", {"page": 1, "io.modelcontextprotocol/serverInfo":
                                               {"name": "mcp-test-peer", "version": ""}}),
                msg="Exposed pagination and cache-eligibility metadata are retained without claiming a served hit.")
    check.equal(results[0]["tools"][0]["inputSchema"], SCHEMA, msg="Discovery preserves the actual tool input schema without provider conversion.")
    check.equal(query(path, "SELECT method,name,status,resolution,input_rounds FROM operations ORDER BY rowid"),
                [{"method": "tools/list", "name": None, "status": "ok", "resolution": "unknown", "input_rounds": None}] * 2,
                msg="Each page is a separate logical discovery operation with unknown resolution and unobserved rounds.")
    starts = [row["body"]["arguments"] for row in rows if row["kind"] == "mcp.request"]
    check.equal(starts, [{"cursor": None, "meta": None, "cache_mode": "use"},
                         {"cursor": "page-two", "meta": {"page-request": 2}, "cache_mode": "bypass"}],
                msg="Discovery starts freeze the native defaults and each explicitly forwarded page option.")


def test_mcp_native_cache_is_unchanged_but_resolution_stays_unknown(recording, monkeypatch, run_sdk_async):
    rec, path = recording
    counts, outputs = {}, {}
    evidence = [OperationEvidence(), OperationEvidence()]

    async def listing(context, params):
        return types.ListToolsResult(tools=[types.Tool(name="echo", input_schema=SCHEMA)],
                                     ttl_ms=60000, cache_scope="private")

    async def exercise():
        for mode in ("native", "observed"):
            server, server_calls = peer(on_list=listing)
            async with Client(server, cache=CacheConfig(clock=lambda: 1000.0)) as native:
                calls, actual = spy_method(monkeypatch, native, "list_tools")
                target = native if mode == "native" else wrap_mcp(rec, native, client_id="client")
                first = await target.list_tools(**({"evidence": evidence[0]} if mode == "observed" else {}))
                second = await target.list_tools(**({"evidence": evidence[1]} if mode == "observed" else {}))
                check.is_(first, actual[0], msg="The observer preserves exact native discovery result identity on the first fetch.")
                check.is_(second, actual[1], msg="The native SDK's cache-return object reaches the host unchanged.")
                check.equal(len(calls), 2, msg="Two host list requests remain two native logical invocations.")
                counts[mode] = len(server_calls["list"])
                outputs[mode] = [result.model_dump(mode="json", by_alias=True, exclude_unset=True) for result in (first, second)]
    run_sdk_async(exercise())
    finish(rec)
    check.equal(counts, {"native": 1, "observed": 1},
                msg="A fixed-clock positive cache control proves the SDK still serves its second default call without a server invocation.")
    check.equal(outputs["observed"], outputs["native"], msg="Capture cannot change the SDK's cached declaration content or TTL/scope metadata.")
    check.not_equal(accepted(evidence[0].start).operation_id, accepted(evidence[1].start).operation_id,
                    msg="A native cache hit is still a distinct observed logical SDK invocation.")
    check.equal(query(path, "SELECT status,resolution,input_rounds FROM operations ORDER BY rowid"),
                [{"status": "ok", "resolution": "unknown", "input_rounds": None}] * 2,
                msg="Private test knowledge of a cache hit must not become the adapter's unsupported served-source or round claim.")
    check.equal([row["kind"] for row in event_rows(path)], ["mcp.request", "mcp.response"] * 2,
                msg="High-level method observation creates two logical operations and no inferred cache.served event.")


def test_mcp_host_cache_records_only_the_actual_served_branch(tmp_path, monkeypatch, run_sdk_async):
    path = tmp_path / "host-cache.sqlite"
    rec = Recorder(path, capture="content", coverage={"producers": {"model": None, "host": "host", "mcp": "mcp"},
                                                      "model_complete": False})
    counters = {}

    async def listing(context, params):
        return types.ListToolsResult(tools=[types.Tool(name="echo", input_schema=SCHEMA)],
                                     ttl_ms=60000, cache_scope="private")

    async def exercise():
        server, server_calls = peer(on_list=listing)
        async with Client(server) as native:
            native_calls, _ = spy_method(monkeypatch, native, "list_tools")
            adapter = wrap_mcp(rec, native, client_id="client")
            host_cache = None

            async def host_catalog():
                nonlocal host_cache
                if host_cache is not None:
                    accepted(rec.observe("cache.served", scope=rec.capture_scope(),
                        payload=host_cache.model_dump(mode="json", by_alias=True, exclude_unset=True),
                        facts={"owner": "host", "cache_id": "test-host-catalog", "cache_scope": "private", "ttl_ms": 60000}))
                    return host_cache
                host_cache = await adapter.list_tools()
                return host_cache

            with rec.turn("actual host cache branch"):
                first = await host_catalog()
                before = (len(native_calls), len(server_calls["list"]))
                second = await host_catalog()
                after = (len(native_calls), len(server_calls["list"]))
                check.is_(second, first, msg="The actual host cache branch serves its stored native declaration.")
                check.equal((before, after), ((1, 1), (1, 1)),
                            msg="The positively observed host hit invokes neither the adapter/native method nor the service handler again.")
                counters["before"], counters["after"] = before, after
    try:
        run_sdk_async(exercise())
        finish(rec)
        check.equal([row["kind"] for row in event_rows(path)], ["mcp.request", "mcp.response", "cache.served"],
                    msg="The fixed host-cache oracle has exactly discovery start/result and one actual served-hit observation.")
        check.equal(query(path, "SELECT resolution,input_rounds FROM operations"), [{"resolution": "unknown", "input_rounds": None}],
                    msg="Host-hit evidence neither creates a synthetic second operation nor retroactively rewrites discovery resolution.")
        for table, expected in (("runs", 1), ("turns", 1), ("operations", 1), ("events", 3), ("links", 0), ("findings", 0)):
            check.equal(query(path, "SELECT count(*) AS count FROM " + table)[0]["count"], expected,
                        msg="The separate positive host-cache control preserves its fixed independent " + table + " count.")
        turn = query(path, "SELECT coverage_json FROM turns")[0]
        coverage = json.loads(turn["coverage_json"])
        check.is_false(coverage["model_complete"], msg="An unavailable model producer cannot gain complete decision coverage from zero tool choices.")
        served = event_rows(path)[2]
        check.equal(served["boundary"], "host", msg="Only the host-owned actual served branch asserts this cache observation.")
        check.equal(served["details"]["owner"], "host", msg="The hit remains attributed to its real cache owner.")
    finally:
        rec.close(timeout=3)


def test_mcp_nontext_content_survives_and_only_ordered_text_blocks_compare(recording, run_sdk_async):
    rec, path = recording
    returned = []
    evidence = [OperationEvidence() for _ in range(5)]

    async def calling(context, params):
        mode = params.arguments["text"]
        image = types.ImageContent(type="image", data="AA==", mime_type="image/png", meta={"retained": True})
        resource = types.EmbeddedResource(type="resource", resource=types.TextResourceContents(
            uri="fixture://resource", text="nested resource text is not a text content block", mime_type="text/plain"))
        left, right = types.TextContent(type="text", text="left"), types.TextContent(type="text", text="right")
        contents = {"mixed": [left, image, right, resource], "text-only": [left, right],
                    "reverse": [right, left], "nontext-only": [image, resource], "empty": []}
        return types.CallToolResult(content=contents[mode], is_error=False, structured_content={"preserved": mode}, meta={"result": "meta"})

    async def exercise():
        server, _ = peer(on_call=calling)
        async with Client(server) as native:
            adapter = wrap_mcp(rec, native, client_id="client")
            for index, mode in enumerate(("mixed", "text-only", "reverse", "nontext-only", "empty")):
                result = await adapter.call_tool("echo", {"text": mode}, evidence=evidence[index])
                returned.append(result.model_dump(mode="json", by_alias=True, exclude_unset=True))
    run_sdk_async(exercise())
    finish(rec)
    terminal = [row for row in event_rows(path) if row["kind"] == "mcp.response"]
    check.equal([row["body"] for row in terminal], returned,
                msg="Retained MCP results preserve native nontext content, structured fields, metadata and order without coercion.")
    assert len(terminal) == 5, "The text-comparison controls require all five actual native result observations."
    tokens = [row["details"]["result_text_digest"]["value"] for row in terminal]
    check.is_true(all(token is not None for token in tokens),
                  msg="Complete projection distinguishes observed absence of text from an unavailable text token.")
    check.equal(tokens[0], tokens[1], msg="Only actual text content blocks contribute; image/resource values must not be coerced into text.")
    check.not_equal(tokens[1], tokens[2], msg="Text comparison preserves block order rather than sorting or concatenating without boundaries.")
    check.equal(tokens[3], tokens[4], msg="Nontext-only content has an explicitly observed empty text projection, equal to an empty result's text list.")
    check.equal(returned[0]["content"][1], {"type": "image", "data": "AA==", "mimeType": "image/png", "_meta": {"retained": True}},
                msg="The native image block survives exactly rather than being rewritten as a textual placeholder.")
    check.equal([item.losses for item in evidence], [()] * 5,
                msg="Supported nontext native content and empty text projections are complete observations, not automatic capture loss.")


@pytest.mark.parametrize("kind", ["noncallable", "coroutine-function", "async-generator"])
def test_mcp_policy_configuration_rejects_async_before_native_work(recording, monkeypatch, kind):
    rec, path = recording
    server, server_calls = peer()
    native = Client(server)
    calls, _ = spy_method(monkeypatch, native, "call_tool")

    async def coroutine_policy(*args):
        raise AssertionError("Configuration must not execute this coroutine")

    async def generator_policy(*args):
        yield {"policy_id": "unused", "retained": None}

    policy = {"noncallable": object(), "coroutine-function": coroutine_policy,
              "async-generator": generator_policy}[kind]
    with pytest.raises(TypeError, match="synchronous callable"):
        wrap_mcp(rec, native, client_id="client", retention_policy=policy)
    check.equal(calls, [], msg="Invalid policy configuration cannot even invoke the native method.")
    check.equal(server_calls, {"list": [], "call": []}, msg="Invalid policy configuration cannot reach the peer.")
    finish(rec)
    check.equal(event_rows(path), [], msg="Invalid configuration makes no recorded operation.")


def test_mcp_selective_retention_preserves_originals_and_detaches_policy_output(
        recording, monkeypatch, run_sdk_async):
    rec, path = recording
    marker = "policy-sensitive-marker"
    arguments = {"text": marker}
    evidence = [OperationEvidence() for _ in range(3)]
    policy_calls, retained_refs = [], []

    async def listing(context, params):
        return types.ListToolsResult(tools=[types.Tool(name="echo", description=marker, input_schema=SCHEMA)])

    def policy(kind, payload, context):
        policy_calls.append((kind, json.loads(json.dumps(payload)), context.copy()))
        method = context["method"]
        context.update(client_id="changed", server_key="changed", method="changed", name="changed")
        payload.clear()
        payload["policy_private_mutation"] = True
        if method == "tools/list":
            mapping = {"policy_id": "catalog-private", "omitted_reason": "catalog_private"}
        else:
            retained = ({"arguments": {"text": "MASKED"}} if kind == "mcp.request" else
                        {"content": [{"type": "text", "text": "MASKED"}], "isError": False})
            mapping = {"policy_id": "tool-mask", "retained": retained, "redacted": True}
        retained_refs.append(mapping)
        return mapping

    async def exercise():
        server, server_calls = peer(on_list=listing)
        async with Client(server) as native:
            calls, outcomes = spy_method(monkeypatch, native, "call_tool")
            lists, pages = spy_method(monkeypatch, native, "list_tools")
            default = wrap_mcp(rec, native, client_id="client", server_key="server", retention_policy=None)
            original = await default.call_tool("echo", arguments, evidence=evidence[0])
            check.is_(original, outcomes[0], msg="Explicit default None preserves the actual SDK result object.")
            check.equal(policy_calls, [], msg="The default route does not execute the configured selective policy.")
            check.equal((len(calls), len(lists), len(server_calls["list"])), (1, 0, 1),
                        msg="The SDK validates its first tool result with one session-level discovery, without a Client.list_tools facade invocation.")
            selected = wrap_mcp(rec, native, client_id="client", server_key="server", retention_policy=policy)
            page = await selected.list_tools(evidence=evidence[1], meta={"private": marker}, cache_mode="bypass")
            returned = await selected.call_tool("echo", arguments, evidence=evidence[2])
            check.is_(page, pages[0], msg="Selective discovery returns the exact native page despite omitted recording content.")
            check.is_(returned, outcomes[1], msg="Selective tool retention returns the exact native result despite replacement bodies.")
            check.equal([value.content[0].text for value in outcomes], [marker, marker],
                        msg="The callback's payload mutation cannot alter either native result.")
            check.equal(page.tools[0].description, marker, msg="Discovery redaction cannot mutate native declarations.")
            check.equal(arguments, {"text": marker}, msg="The callback cannot mutate the host's original argument object.")
            check.is_(calls[1][0][1], arguments, msg="The selected route forwards the same original argument container.")
            check.equal([call.arguments for call in server_calls["call"]], [{"text": marker}] * 2,
                        msg="The genuine peer sees both original requests, never either retained replacement.")
            check.equal((len(calls), len(lists), len(server_calls["list"])), (2, 1, 2),
                        msg="Three explicit facade invocations retain the SDK's one earlier validation discovery plus the requested discovery, without adapter retries.")
            for mapping in retained_refs:
                if "retained" in mapping:
                    mapping["retained"].clear()
                    mapping["retained"]["late"] = marker
                mapping.update(policy_id="late-policy", redacted=False)
    run_sdk_async(exercise())
    closed = finish(rec)
    rows = event_rows(path)
    check.equal([row["kind"] for row in rows], ["mcp.request", "mcp.response"] * 3,
                msg="Each native invocation owns exactly one start and one terminal.")
    assert len(rows) == 6
    original_request = b'{"arguments":{"text":"policy-sensitive-marker"}}'
    retained_request = b'{"arguments":{"text":"MASKED"}}'
    retained_response = b'{"content":[{"type":"text","text":"MASKED"}],"isError":false}'
    check.equal(rows[0]["body_text"].encode(), original_request, msg="The default None route retains the original request bytes.")
    check.equal(rows[1]["body"]["content"][0]["text"], marker, msg="The default None route retains original result text.")
    for row, body, observed in ((rows[4], retained_request, len(original_request)),
                                (rows[5], retained_response, rows[1]["stored_bytes"])):
        check.equal((row["body_text"].encode(), row["stored_sha256"], row["stored_bytes"]),
                    (body, hashlib.sha256(body).hexdigest(), len(body)),
                    msg="Stored replacement bytes, length and hash match independent literals before later policy mutation.")
        check.equal((row["observed_bytes"], row["policy_id"], row["redacted"], row["truncated"], row["omitted_reason"]),
                    (observed, "tool-mask", 1, 0, None),
                    msg="Original observed size and frozen policy flags remain distinct from retained storage.")
    for row in rows[2:4]:
        check.equal((row["body_text"], row["stored_sha256"], row["stored_bytes"], row["policy_id"], row["omitted_reason"]),
                    (None, None, 0, "catalog-private", "catalog_private"),
                    msg="Both discovery projections use the declared omission without retaining private catalog content.")
        check.greater(row["observed_bytes"], 0, msg="An omitted discovery body still has an actually observed original size.")
    for first, second, key in ((rows[0], rows[4], "args_digest"), (rows[1], rows[5], "result_text_digest")):
        left, right = first["details"][key], second["details"][key]
        check.is_not_none(left["value"], msg="The chosen supported original projection must have an available comparison token.")
        check.equal(left, right, msg="Equal originals in one run have equal tokens regardless of different retained bodies.")
        check.equal((left.get("basis"), left.get("reason")), ("observed_original", None),
                    msg="Available comparison evidence remains based on originals with no unavailable reason.")
    check.equal([(kind, context) for kind, _, context in policy_calls],
                [(kind, {"client_id": "client", "server_key": "server", "method": method, "name": name})
                 for method, name in (("tools/list", None), ("tools/call", "echo"))
                 for kind in ("mcp.request", "mcp.response")],
                msg="Policy kinds and the exact four context fields are detached and local to each invocation.")
    check.equal(policy_calls[0][1], {"arguments": {"cursor": None, "meta": {"private": marker}, "cache_mode": "bypass"}},
                msg="Discovery policy receives the complete native-options request envelope.")
    check.equal(policy_calls[2][1], {"arguments": {"text": marker}},
                msg="Tool request policy receives the complete original request envelope.")
    check.equal(policy_calls[3][1]["content"][0]["text"], marker,
                msg="Terminal policy receives the original translated result, before its own mutations.")
    check.equal(len({accepted(item.start).operation_id for item in evidence}), 3,
                msg="Equal originals and discovery remain distinct recorded operations.")
    for item in evidence:
        accepted(item.terminal)
        check.equal(item.losses, (), msg="Declared replacement or omission is not adapter policy failure.")
    check.is_true(all(row["details"]["complete"] for row in rows), msg="Selective retention alone preserves complete observations.")
    check.equal((closed.run_close_state, rec.health().open_operations, rec.health().completion.used_slots), ("clean", 0, 0),
                msg="The joined selected-retention recording is complete and releases every reservation.")


@pytest.mark.parametrize("failure", ["exception", "callback-cancel", "none", "unknown-field", "conflict",
                                     "unsupported", "nonfinite", "invalid-utf8", "coroutine"])
def test_mcp_policy_failures_omit_once_without_raw_fallback(
        recording, monkeypatch, run_sdk_async, caplog, failure):
    rec, path = recording
    marker = "policy-failure-private-marker"
    policy_calls, coroutines, attempts = [], [], []
    evidence = OperationEvidence()

    async def forbidden_coroutine():
        raise AssertionError("Returned policy coroutine must never execute")

    def policy(kind, payload, context):
        policy_calls.append(kind)
        if failure == "exception":
            raise ValueError(marker)
        if failure == "callback-cancel":
            raise asyncio.CancelledError(marker)
        if failure == "coroutine":
            value = forbidden_coroutine()
            coroutines.append(value)
            return value
        return {"none": None,
                "unknown-field": {"policy_id": "invalid", "retained": {}, "extra": marker},
                "conflict": {"policy_id": "invalid", "retained": {}, "omitted_reason": "conflict"},
                "unsupported": {"policy_id": "invalid", "retained": object()},
                "nonfinite": {"policy_id": "invalid", "retained": float("nan")},
                "invalid-utf8": {"policy_id": "invalid", "retained": "\ud800"}}[failure]

    for method in ("start_operation", "finish_operation"):
        original = getattr(rec, method)

        def witness(*args, _method=method, _original=original, **kwargs):
            attempts.append((_method, kwargs["retention"].copy()))
            return _original(*args, **kwargs)

        monkeypatch.setattr(rec, method, witness)

    async def exercise():
        server, server_calls = peer()
        async with Client(server) as native:
            calls, results = spy_method(monkeypatch, native, "call_tool")
            value = await wrap_mcp(rec, native, client_id="client", retention_policy=policy).call_tool(
                "echo", {"text": marker}, evidence=evidence)
            check.is_(value, results[0], msg="Every contained policy fault returns the exact actual native result.")
            check.equal(value.content[0].text, marker, msg="Policy failure cannot redact or replace the native result.")
            check.equal((len(calls), len(server_calls["call"])), (1, 1), msg="A policy failure neither retries nor suppresses the native call.")
            check.equal(server_calls["call"][0].arguments, {"text": marker}, msg="The real peer receives the original private arguments.")
    with caplog.at_level(logging.WARNING, logger="mcp_flightrecorder.integrations._common"):
        run_sdk_async(exercise())
    closed = finish(rec)
    fixed = {"policy_id": "adapter_policy_error", "omitted_reason": "retention_policy_failed"}
    check.equal(attempts, [("start_operation", fixed), ("finish_operation", fixed)],
                msg="Validation establishes fixed omission before exactly one admission attempt at each boundary.")
    check.equal(policy_calls, ["mcp.request", "mcp.response"], msg="Both exposed projections receive one policy invocation.")
    check.equal(evidence.losses, ("retention_policy_failed",), msg="Two policy failures retain one bounded distinct side-channel loss.")
    for receipt in (accepted(evidence.start), accepted(evidence.terminal)):
        check.equal(receipt.losses, ("body_omitted",), msg="Core receipts keep their existing omission semantics.")
    rows = event_rows(path)
    check.equal([row["kind"] for row in rows], ["mcp.request", "mcp.response"], msg="No raw fallback or second observation is recorded.")
    for row in rows:
        check.equal((row["body_text"], row["stored_bytes"], row["stored_sha256"], row["policy_id"], row["omitted_reason"]),
                    (None, 0, None, "adapter_policy_error", "retention_policy_failed"),
                    msg="Invalid policy content cannot escape into a recorded body or hash.")
        check.is_true(row["details"]["complete"], msg="Omitting policy-failed content does not erase the complete original observation.")
        check.is_not_in(marker, row["details_json"], msg="Private callback text cannot leak into control metadata.")
    check.equal(query(path, "SELECT status FROM operations"), [{"status": "ok"}], msg="The native success remains the recorded terminal status.")
    messages = [record.getMessage() for record in caplog.records if record.name == "mcp_flightrecorder.integrations._common"]
    check.equal(messages, ["integration_capture_loss reason=retention_policy_failed"],
                msg="The observer logs only its fixed bounded reason, once for the invocation.")
    check.is_not_in(marker, caplog.text + repr((evidence.start, evidence.terminal, evidence.losses)),
                    msg="Private payload and exception text stay out of observer logs and public receipts.")
    check.equal([inspect.getcoroutinestate(value) for value in coroutines], [inspect.CORO_CLOSED] * len(coroutines),
                msg="Returned ordinary coroutines are closed without awaiting or scheduling them.")
    check.equal((closed.run_close_state, rec.health().open_operations, rec.health().completion.used_slots), ("clean", 0, 0),
                msg="Contained policy failures preserve complete operation cleanup.")


@pytest.mark.parametrize("fails", [False, True], ids=["retained-content", "failed-policy"])
def test_mcp_metadata_capture_overrides_retained_or_failed_policy(tmp_path, run_sdk_async, fails):
    path = tmp_path / "metadata-policy.sqlite"
    rec = Recorder(path, capture="metadata", coverage=COVERAGE)
    evidence, seen = OperationEvidence(), []

    def policy(kind, payload, context):
        seen.append(kind)
        if fails:
            raise ValueError("metadata-private-marker")
        return {"policy_id": "explicit-content", "retained": {"visible": "metadata-private-marker"}, "redacted": True}

    async def exercise():
        server, _ = peer()
        async with Client(server) as native:
            value = await wrap_mcp(rec, native, client_id="client", retention_policy=policy).call_tool(
                "echo", {"text": "metadata-private-marker"}, evidence=evidence)
            check.equal(value.content[0].text, "metadata-private-marker", msg="Global metadata capture does not change native content.")
    try:
        run_sdk_async(exercise())
        finish(rec)
        rows = event_rows(path)
        check.equal(seen, ["mcp.request", "mcp.response"], msg="Explicit configured policies still execute in metadata mode.")
        check.equal([row["omitted_reason"] for row in rows], ["capture_metadata"] * 2,
                    msg="The existing global metadata omission reason takes precedence over content and policy failure.")
        check.equal([(row["body_text"], row["stored_bytes"], row["stored_sha256"]) for row in rows], [(None, 0, None)] * 2,
                    msg="Neither retained content nor failed policy can create stored bodies in metadata mode.")
        check.equal(evidence.losses, ("retention_policy_failed",) if fails else (), msg="Policy failure remains visible independently of metadata omission.")
        for item in (evidence.start, evidence.terminal):
            accepted(item)
        check.equal((rec.health().open_operations, rec.health().completion.used_slots), (0, 0), msg="Metadata omission leaves no reservation.")
    finally:
        rec.close(timeout=3)


def test_mcp_policy_helper_detaches_mutable_context_and_distinguishes_missing_from_null():
    evidence = OperationEvidence()
    name = ["invalid-native-name"]
    context = {"client_id": "client", "server_key": "server", "method": "tools/call", "name": name}
    payload = {"arguments": {"text": "original"}}
    seen = []
    retained = {"visible": ["before"]}

    def policy(kind, copied, local):
        seen.append((kind, copied, local))
        if copied is not None:
            copied["arguments"]["text"] = "changed"
        local["name"].append("changed")
        return {"policy_id": "detached", "retained": retained if copied is not None else None}

    value = policy_retention(evidence, policy, "mcp.request", payload, context)
    retained["visible"].append("late")
    check.equal(value, {"policy_id": "detached", "retained": {"visible": ["before"]}},
                msg="Returned nested policy ownership is detached before the capture boundary.")
    check.equal((payload, context["name"], name), ({"arguments": {"text": "original"}}, ["invalid-native-name"], ["invalid-native-name"]),
                msg="A mutable invalid name at the shared boundary cannot mutate the original value forwarded by the public facade.")
    check.is_none(policy_retention(evidence, policy, "mcp.request", MISSING, context), msg="MISSING skips the callback entirely.")
    check.equal(len(seen), 1, msg="Unavailable projection cannot invent a policy invocation.")
    check.equal(policy_retention(evidence, policy, "mcp.response", None, context), {"policy_id": "detached", "retained": None},
                msg="Exposed JSON null invokes policy and remains a valid explicit retained null.")
    check.equal(len(seen), 2, msg="Null and missing have distinct callback behavior.")
    check.equal(evidence.losses, (), msg="Detached ordinary JSON including null is valid policy behavior.")


@pytest.mark.parametrize("boundary", ["mcp.request", "mcp.response"], ids=["request", "terminal"])
@pytest.mark.parametrize("exception_type", [SystemExit, KeyboardInterrupt], ids=["system-exit", "keyboard-interrupt"])
def test_mcp_policy_process_control_propagates_at_its_boundary(
        recording, monkeypatch, run_sdk_async, boundary, exception_type):
    rec, path = recording
    interruption = exception_type("test-owned process control")
    evidence, seen = OperationEvidence(), []

    def policy(kind, payload, context):
        seen.append(kind)
        if kind == boundary:
            raise interruption
        return {"policy_id": "keep", "retained": payload}

    async def exercise():
        server, server_calls = peer()
        async with Client(server) as native:
            calls, actual = spy_method(monkeypatch, native, "call_tool")
            adapter = wrap_mcp(rec, native, client_id="client", retention_policy=policy)
            with pytest.raises(exception_type) as caught:
                await adapter.call_tool("echo", {"text": "original"}, evidence=evidence)
            check.is_(caught.value, interruption, msg="Process-control callbacks propagate their exact exception object.")
            expected = 0 if boundary == "mcp.request" else 1
            check.equal((len(calls), len(server_calls["call"])), (expected, expected),
                        msg="A request interruption precedes native work; a terminal interruption follows exactly one real call.")
            if expected:
                check.equal(actual[0].content[0].text, "original", msg="The interrupted terminal follows a real successful native result.")
                accepted(evidence.start)
            else:
                check.is_none(evidence.start, msg="A request policy interruption precedes start admission.")
            check.is_none(evidence.terminal, msg="An unhandled process-control interruption is not fabricated as completed capture.")
            check.equal((rec.health().open_operations, rec.health().completion.used_slots), (expected, expected),
                        msg="Only a response interruption leaves its admitted operation and completion reservation unfinished.")
    run_sdk_async(exercise())
    closed = finish(rec)
    check.equal(seen, ["mcp.request"] if boundary == "mcp.request" else ["mcp.request", "mcp.response"],
                msg="No policy retry follows process-control propagation.")
    check.equal(evidence.losses, (), msg="Process control is explicitly outside ordinary policy-failure containment.")
    check.equal([row["kind"] for row in event_rows(path)], [] if boundary == "mcp.request" else ["mcp.request"],
                msg="The recording preserves the real interrupted boundary instead of inventing a terminal.")
    check.equal(closed.run_close_state, "clean" if boundary == "mcp.request" else "incomplete",
                msg="An unfinished response-side operation is honestly incomplete at joined portable close.")


def test_mcp_overlapping_policies_keep_method_and_name_invocation_local(recording, monkeypatch, run_sdk_async):
    rec, path = recording
    evidence = [OperationEvidence(), OperationEvidence()]
    seen = []

    def policy(kind, payload, context):
        seen.append((kind, context.copy()))
        return {"policy_id": "context", "retained": {"method": context["method"], "name": context["name"]}}

    async def exercise():
        listed, called, release_list, release_call = (asyncio.Event() for _ in range(4))
        setup_complete = False

        async def listing(context, params):
            if setup_complete:
                listed.set()
                await asyncio.wait_for(release_list.wait(), timeout=3)
            return types.ListToolsResult(tools=[types.Tool(name="echo", input_schema=SCHEMA)])

        async def calling(context, params):
            called.set()
            await asyncio.wait_for(release_call.wait(), timeout=3)
            return types.CallToolResult(content=[types.TextContent(type="text", text="original")])

        server, server_calls = peer(on_list=listing, on_call=calling)
        async with Client(server) as native:
            lists, list_values = spy_method(monkeypatch, native, "list_tools")
            calls, call_values = spy_method(monkeypatch, native, "call_tool")
            setup_page = await native.list_tools(cache_mode="bypass")
            check.is_(setup_page, list_values[0], msg="A genuine unobserved public discovery establishes the SDK's output-schema state.")
            check.equal((len(lists), len(calls), len(server_calls["list"])), (1, 0, 1),
                        msg="The separate setup discovery is retained in both native and peer call ledgers.")
            check.equal((seen, rec.health().open_operations), ([], 0),
                        msg="Native setup precedes the observed overlap and creates no policy invocation or operation.")
            setup_complete = True
            adapter = wrap_mcp(rec, native, client_id="client", server_key="server", retention_policy=policy)
            tasks = [asyncio.create_task(adapter.list_tools(evidence=evidence[0], cache_mode="bypass"))]
            try:
                await asyncio.wait_for(listed.wait(), timeout=3)
                tasks.append(asyncio.create_task(adapter.call_tool("echo", {"text": "original"}, evidence=evidence[1])))
                await asyncio.wait_for(called.wait(), timeout=3)
                check.equal(rec.health().open_operations, 2, msg="Both real SDK invocations overlap before either terminal policy runs.")
                release_call.set()
                tool = await asyncio.wait_for(tasks[1], timeout=3)
                check.is_(tool, call_values[0], msg="The first released native tool result preserves identity.")
                release_list.set()
                page = await asyncio.wait_for(tasks[0], timeout=3)
                check.is_(page, list_values[1], msg="The later observed discovery result preserves identity after another method completed.")
                check.equal((len(lists), len(calls), len(server_calls["list"]), len(server_calls["call"])), (2, 1, 2, 1),
                            msg="One genuine setup discovery and the two overlapping observed invocations account for every native and peer call.")
            finally:
                release_list.set()
                release_call.set()
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
    run_sdk_async(exercise())
    finish(rec)
    expected = [("mcp.request", "tools/list", None), ("mcp.request", "tools/call", "echo"),
                ("mcp.response", "tools/call", "echo"), ("mcp.response", "tools/list", None)]
    check.equal(seen, [(kind, {"client_id": "client", "server_key": "server", "method": method, "name": name})
                       for kind, method, name in expected], msg="Terminal context belongs to its original invocation despite interleaving.")
    check.equal([(row["kind"], row["body"]) for row in event_rows(path)],
                [(kind, {"method": method, "name": name}) for kind, method, name in expected],
                msg="Durable retained content agrees with each actual interleaved method and name.")
    check.not_equal(accepted(evidence[0].start).operation_id, accepted(evidence[1].start).operation_id,
                    msg="Concurrent different methods own distinct operation identities.")
    for item in evidence:
        accepted(item.terminal)
        check.equal(item.losses, (), msg="An overlapping valid policy creates no observation loss.")
    check.equal((rec.health().open_operations, rec.health().completion.used_slots), (0, 0),
                msg="Both interleaved operations release their own reservations.")
