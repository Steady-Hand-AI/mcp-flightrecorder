"""A2–A8: real MCP 2.1.1 stdio boundaries and separately labelled doubles."""
import asyncio
from contextlib import asynccontextmanager, closing
from functools import wraps
import hashlib
import inspect as python_inspect
import json
from pathlib import Path
import sqlite3
import sys
from types import SimpleNamespace

from mcp import ClientSession, MCPError
from mcp.client.stdio import StdioServerParameters, stdio_client
import mcp_types as types
from pydantic import ConfigDict, ValidationError
import pytest
import pytest_check as check

from mcp_flightrecorder import Recorder
from mcp_flightrecorder.integrations.mcp_client import (
    OperationEvidence as ClientEvidence, wrap_mcp,
)
from mcp_flightrecorder.integrations.mcp_session import OperationEvidence, wrap_session


COVERAGE = {"producers": {"model": "compat.explicit-receipts", "host": "compat.session",
                           "mcp": "mcp_session/2.1.1"}, "model_complete": False}


def rows(path, statement):
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as connection:
        connection.row_factory = sqlite3.Row
        return [dict(row) for row in connection.execute(statement)]


def events(path):
    result = rows(path, """SELECT e.*,a.body_text FROM events e
        LEFT JOIN artifacts a ON a.artifact_id=e.payload_ref ORDER BY e.sequence""")
    for row in result:
        row["details"] = json.loads(row["details_json"])
        row["body"] = None if row["body_text"] is None else json.loads(row["body_text"])
    return result


def accepted(receipt):
    assert receipt is not None and receipt.status == "accepted", (
        "Dependent scope/identity checks require an admitted public receipt."
    )
    return receipt


def finish(recorder):
    closed = recorder.close(timeout=5)
    assert closed.writer_stopped and closed.portable, (
        "Every owned recording must join its writer and produce a portable file before inspection."
    )
    return closed


def wire_log(case):
    path = case / "server.jsonl"
    return [] if not path.exists() else [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


@asynccontextmanager
async def native_session(case, *, modern=False, paged=False, malformed=False):
    args = ["-I", "-B", str(Path(__file__).with_name("session_server.py")), str(case / "server.jsonl")]
    if paged:
        args.append("--paged")
    if malformed:
        args.append("--malformed-wire")
    parameters = StdioServerParameters(command=sys.executable, args=args)
    with (case / "server.stderr").open("w", encoding="utf-8") as errors:
        async with stdio_client(parameters, errlog=errors) as streams:
            async with ClientSession(*streams) as session:
                if modern:
                    await session.discover()
                else:
                    await session.initialize()
                yield session


def spy_method(monkeypatch, session, name):
    """Narrow forwarding/outcome witness around the actual bound SDK method."""
    original = getattr(session, name)
    calls, outcomes = [], []

    @wraps(original)
    def spy(*args, **kwargs):
        calls.append((args, kwargs))
        native = original(*args, **kwargs)

        async def execute():
            try:
                result = await native
            except BaseException as error:
                outcomes.append(error)
                raise
            outcomes.append(result)
            return result

        return execute()

    monkeypatch.setattr(session, name, spy)
    return calls, outcomes


@pytest.fixture
def recording(evidence_path):
    path = evidence_path / "recording.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    try:
        yield recorder, path
    finally:
        closed = recorder.close(timeout=5)
        assert closed.writer_stopped, "Each case must reap its own recorder writer even after a failed assertion."


def test_session_surface_shared_receipts_and_unawaited_ownership(recording):
    """Labelled factory-only double establishes no native execution or reservation."""
    recorder, path = recording
    calls = []

    class NativeFactoryDouble:
        def list_tools(self, **kwargs):
            calls.append(kwargs)
            raise AssertionError("The unawaited control must not invoke its native callable")

        def call_tool(self, *args, **kwargs):
            calls.append((args, kwargs))
            raise AssertionError("The unawaited control must not invoke its native callable")

    native = NativeFactoryDouble()
    observer = wrap_session(recorder, native, client_id="é" * 128, server_key="s" * 256)
    check.is_(ClientEvidence, OperationEvidence,
              msg="Both public MCP entry points re-export the exact receipt type required by exact-type ownership checks.")
    check.equal(str(python_inspect.signature(observer.list_tools)), "(*, params=None, evidence=None)",
                msg="The session listing facade exposes params only, with the recorder evidence slot after it.")
    check.equal(str(python_inspect.signature(observer.call_tool)),
                "(name, arguments=None, read_timeout_seconds=None, progress_callback=None, *, input_responses=None, request_state=None, meta=None, allow_input_required=False, allow_claimed=False, evidence=None, decision=None)",
                msg="The complete session signature preserves native positional and keyword-only option boundaries.")
    check.is_false(any(hasattr(observer, name) for name in ("__aenter__", "__aexit__", "close", "aclose")),
                   msg="A two-method observer must not take ownership of session transport or lifecycle.")
    for method in (observer.list_tools, observer.call_tool):
        check.is_false(python_inspect.iscoroutinefunction(method),
                       msg="Invocation is a synchronous factory so its scope is captured before a later await.")
    check.equal(list(python_inspect.signature(ClientSession.list_tools).parameters), ["self", "params"],
                msg="The installed MCP session pin, rather than another Client interface, supplies this listing contract.")
    with check.raises(ValueError, msg="Invalid recorder labels must fail before constructing any native work."):
        wrap_session(recorder, native, client_id="")
    with check.raises(ValueError, msg="The common label limit counts UTF-8 bytes, including multibyte labels."):
        wrap_session(recorder, native, client_id="é" * 129)
    with check.raises(TypeError, msg="An unrelated evidence object is explicit recorder-only API misuse at invocation."):
        observer.list_tools(evidence=object())

    class DerivedEvidence(OperationEvidence):
        pass

    with check.raises(TypeError, msg="Evidence subclasses cannot bypass the exact shared ownership type."):
        observer.call_tool("echo", evidence=DerivedEvidence())
    evidence = OperationEvidence()
    with recorder.turn("invocation-only A"):
        pending = observer.call_tool("echo", evidence=evidence)
    assert python_inspect.iscoroutine(pending), "Valid observer invocation must return a closeable Python coroutine."
    try:
        with check.raises(ValueError, msg="An evidence object belongs to its first invocation even before that coroutine executes."):
            observer.list_tools(evidence=evidence)
        check.equal(calls, [], msg="Creating unawaited work cannot even construct a native coroutine.")
        check.equal((recorder.health().open_operations, recorder.health().completion.used_slots), (0, 0),
                    msg="Unexecuted factories own neither an operation nor a reserved completion slot.")
    finally:
        pending.close()
    finish(recorder)
    check.equal((evidence.start, evidence.terminal, evidence.losses), (None, None, ()),
                msg="Never-awaited work has no fabricated receipt or capture-loss outcome.")
    check.equal(events(path), [], msg="Closing a factory coroutine is not an executed logical MCP operation.")


@pytest.mark.parametrize("shape", ["null", "empty", "explicit-null"])
def test_session_listing_distinguishes_null_empty_and_explicit_null_params(
        recording, evidence_path, monkeypatch, run_sdk_async, shape):
    recorder, path = recording
    params = {"null": None, "empty": types.PaginatedRequestParams(),
              "explicit-null": types.PaginatedRequestParams(cursor=None, meta=None)}[shape]
    expected = {"null": {"params": None}, "empty": {"params": {}},
                "explicit-null": {"params": {"cursor": None, "_meta": None}}}[shape]
    evidence = OperationEvidence()
    projected = []

    async def scenario():
        async with native_session(evidence_path) as native:
            calls, outcomes = spy_method(monkeypatch, native, "list_tools")
            observer = wrap_session(recorder, native, client_id="session", server_key="peer")
            result = (await observer.list_tools(evidence=evidence) if shape == "null" else
                      await observer.list_tools(params=params, evidence=evidence))
            check.is_(result, outcomes[0], msg="Discovery returns the exact actual SDK result without merging or copying it.")
            check.equal(len(calls), 1, msg="One explicit default/page invocation calls the native listing once.")
            check.equal(set(calls[0][1]), {"params"}, msg="Client-only cursor/meta/cache_mode keywords must never reach ClientSession.")
            check.is_(calls[0][1]["params"], params, msg="The original typed params or None reaches the SDK unchanged.")
            check.equal([tool.name for tool in result.tools], ["echo", "second"],
                        msg="The actual stdio server's ordered declarations remain ordered in the native result.")
            projected.append(result.model_dump(mode="json", by_alias=True, exclude_unset=True))

    run_sdk_async(scenario())
    finish(recorder)
    observed = events(path)
    check.equal([row["kind"] for row in observed], ["mcp.request", "mcp.response"],
                msg="One actual discovery has exactly one logical start and one terminal observation.")
    check.equal(observed[0]["body"], {"arguments": expected},
                msg="Omitted/default params, a typed empty object, and explicitly exposed null fields remain distinguishable.")
    check.equal(observed[1]["body"], projected[0], msg="Retained discovery is the trusted SDK projection including exposed metadata.")
    check.equal(len(wire_log(evidence_path)), 1, msg="The observer fetches no additional page or hidden service request.")
    check.equal(rows(path, "SELECT method,status,resolution,input_rounds FROM operations"),
                [{"method": "tools/list", "status": "ok", "resolution": "unknown", "input_rounds": None}],
                msg="A normal session return does not establish transport resolution or internal input-round count.")
    check.equal(evidence.losses, (), msg="All three explicitly supported params shapes are complete observations.")


def test_session_explicit_pages_keep_typed_params_order_and_metadata(
        recording, evidence_path, monkeypatch, run_sdk_async):
    recorder, path = recording
    projected, params_seen = [], []

    class ExtendedParams(types.PaginatedRequestParams):
        """Trusted typed extension projection; no claim that the SDK invents extras."""
        model_config = ConfigDict(extra="allow")

    first_params = ExtendedParams(meta={"page-request": 1}, cursor=None,
                                  future_flag={"exposed": "✓"})
    second_params = types.PaginatedRequestParams(cursor="page-two", meta={"page-request": 2})

    async def scenario():
        async with native_session(evidence_path, modern=True, paged=True) as native:
            calls, outcomes = spy_method(monkeypatch, native, "list_tools")
            observer = wrap_session(recorder, native, client_id="session")
            first = await observer.list_tools(params=first_params)
            check.equal(len(wire_log(evidence_path)), 1, msg="Exposed nextCursor cannot cause automatic page consumption.")
            check.equal(first.next_cursor, "page-two", msg="The caller sees the actual continuation token from the first native page.")
            second = await observer.list_tools(params=second_params)
            for index, result in enumerate((first, second)):
                check.is_(result, outcomes[index], msg="Each explicit page retains its independent native object identity.")
                projected.append(result.model_dump(mode="json", by_alias=True, exclude_unset=True))
            params_seen.extend([calls[0][1]["params"], calls[1][1]["params"]])
            check.equal(len(calls), 2, msg="Two caller-selected pages cause exactly two native listing invocations.")

    run_sdk_async(scenario())
    finish(recorder)
    check.is_(params_seen[0], first_params, msg="The exposed typed extension instance is passed through rather than reconstructed.")
    check.is_(params_seen[1], second_params, msg="The cursor/_meta instance is also forwarded with its original identity.")
    observed = events(path)
    check.equal([row["body"]["arguments"] for row in observed if row["kind"] == "mcp.request"],
                [{"params": {"_meta": {"page-request": 1}, "cursor": None, "future_flag": {"exposed": "✓"}}},
                 {"params": {"_meta": {"page-request": 2}, "cursor": "page-two"}}],
                msg="The trusted params dump preserves explicit null, aliases, ordered request scope, and genuinely exposed extras.")
    results = [row["body"] for row in observed if row["kind"] == "mcp.response"]
    check.equal(results, projected, msg="Page bodies retain exactly the SDK-exposed values without default or catalog synthesis.")
    check.equal([[tool["name"] for tool in body["tools"]] for body in results], [["echo", "second"], ["last"]],
                msg="Ordered schema pages stay separate rather than becoming one inferred current catalog.")
    check.equal([body["ttlMs"] for body in results], [60000, 1000], msg="Modern native page cache eligibility metadata remains available.")
    check.equal([body["_meta"]["page"] for body in results], [1, 2], msg="Per-page metadata remains attached to the actual page.")
    ledger = wire_log(evidence_path)
    check.equal(len(ledger), 2, msg="Only the two explicit pages reach the actual stdio service.")
    check.equal([(entry["params"].get("cursor"), entry["params"]["_meta"]["page-request"]) for entry in ledger],
                [(None, 1), ("page-two", 2)], msg="The server independently observes the intended cursor and metadata on each request.")


def test_session_default_call_keeps_sdk_schema_refresh_inside_one_operation(
        recording, evidence_path, monkeypatch, run_sdk_async):
    recorder, path = recording

    async def scenario():
        async with native_session(evidence_path) as native:
            calls, outcomes = spy_method(monkeypatch, native, "call_tool")
            internal_lists, _ = spy_method(monkeypatch, native, "list_tools")
            result = await wrap_session(recorder, native, client_id="session").call_tool("echo")
            check.is_(result, outcomes[0], msg="Default-call observation preserves the exact native result.")
            check.equal(result.content[0].text, "default ✓", msg="The native service supplies its unchanged default text for null arguments.")
            check.equal(calls[0], (("echo", None, None, None), {"input_responses": None,
                        "request_state": None, "meta": None, "allow_input_required": False, "allow_claimed": False}),
                        msg="Declared None/False defaults are forwarded exactly, with no recorder-only keyword leakage.")
            check.equal((len(calls), len(internal_lists)), (1, 1),
                        msg="The actual SDK performs its own schema refresh within one outer tool invocation.")

    run_sdk_async(scenario())
    finish(recorder)
    check.equal([entry["method"] for entry in wire_log(evidence_path)], ["tools/call", "tools/list"],
                msg="The independent native ledger distinguishes SDK-internal schema discovery from the observed outer operation.")
    check.equal(rows(path, "SELECT method,status FROM operations"), [{"method": "tools/call", "status": "ok"}],
                msg="A native internal refresh is not fabricated as another host-observed operation.")
    check.equal(events(path)[0]["body"], {"arguments": None}, msg="Exposed null tool arguments cannot become an invented empty object.")


def test_session_all_nondefaults_forward_identity_and_native_progress(
        recording, evidence_path, monkeypatch, run_sdk_async):
    recorder, path = recording
    arguments, responses, meta = {"text": "submitted café ✓"}, {}, {"test-control": "native-meta"}
    progress_events = []

    async def progress(value, total, message):
        progress_events.append([value, total, message])

    async def scenario():
        async with native_session(evidence_path, modern=True) as native:
            await native.list_tools()
            calls, outcomes = spy_method(monkeypatch, native, "call_tool")
            evidence = OperationEvidence()
            result = await wrap_session(recorder, native, client_id="session").call_tool(
                "echo", arguments, 3.0, progress, input_responses=responses,
                request_state="continue-opaque", meta=meta, allow_input_required=True,
                allow_claimed=True, evidence=evidence)
            check.is_(result, outcomes[0], msg="Nondefault forwarding still returns the actual native SDK object.")
            assert len(calls) == 1, "Forwarding assertions require exactly one witnessed native invocation."
            args, options = calls[0]
            check.equal(args[0], "echo", msg="The requested tool name is not rewritten by capture.")
            check.is_(args[1], arguments, msg="Native argument object identity is preserved, not replaced with the frozen capture copy.")
            check.equal(args[2], 3.0, msg="The host's explicit native timeout is forwarded without policy changes.")
            check.is_(args[3], progress, msg="The host callback is forwarded directly without an observer callback wrapper.")
            check.is_(options["input_responses"], responses, msg="Input-response routing remains owned by the host and native SDK.")
            check.is_(options["meta"], meta, msg="The original native metadata object is preserved.")
            check.equal({key: options[key] for key in ("request_state", "allow_input_required", "allow_claimed")},
                        {"request_state": "continue-opaque", "allow_input_required": True, "allow_claimed": True},
                        msg="Both boundary flags and opaque request state reach the SDK unchanged.")
            check.equal(set(options), {"input_responses", "request_state", "meta", "allow_input_required", "allow_claimed"},
                        msg="Evidence and decision parameters never leak into the native invocation.")
            accepted(evidence.terminal)

    run_sdk_async(scenario())
    finish(recorder)
    check.equal(progress_events, [[1, 2, "entered ✓"]], msg="The actual stdio notification reaches the unchanged host progress callback once.")
    native_call = [entry for entry in wire_log(evidence_path) if entry["method"] == "tools/call"]
    assert len(native_call) == 1, "The independent native ledger must contain the one submitted nondefault tool call."
    submitted = native_call[0]["params"]
    check.equal(submitted["arguments"], arguments, msg="The native server receives the host's submitted tool argument values unchanged.")
    check.equal((submitted["inputResponses"], submitted["requestState"], submitted["_meta"]["test-control"]),
                ({}, "continue-opaque", "native-meta"), msg="Actual protocol submission retains input responses, state and exposed metadata.")
    check.equal(events(path)[0]["body"], {"arguments": arguments},
                msg="Transport, callbacks, flags, input responses and request state never enter the tool-argument digest body.")


@pytest.mark.parametrize("outcome", ["tool-error", "protocol-error", "timeout", "cancelled"])
def test_session_native_terminal_categories_preserve_original_outcome(
        recording, evidence_path, monkeypatch, run_sdk_async, outcome):
    recorder, path = recording
    evidence = OperationEvidence()

    async def scenario():
        entered = asyncio.Event()

        async def progress(value, total, message):
            entered.set()

        async with native_session(evidence_path) as native:
            calls, outcomes = spy_method(monkeypatch, native, "call_tool")
            pending = wrap_session(recorder, native, client_id="session").call_tool(
                "echo", {"case": outcome, "text": "retained native error"},
                0.1 if outcome == "timeout" else 3.0,
                progress if outcome == "cancelled" else None, evidence=evidence)
            task = asyncio.create_task(pending)
            try:
                if outcome == "cancelled":
                    await asyncio.wait_for(entered.wait(), 3)
                    task.cancel("host cancellation payload")
                if outcome == "tool-error":
                    result = await task
                    check.is_(result, outcomes[0], msg="A tool error is the unchanged native returned object, not an observer exception.")
                    check.is_true(result.is_error, msg="The actual native peer explicitly returns the tool-error flag.")
                else:
                    error_type = asyncio.CancelledError if outcome == "cancelled" else MCPError
                    with pytest.raises(error_type) as caught:
                        await task
                    check.is_(caught.value, outcomes[0], msg="Error/cancellation observation preserves the exact native surfaced exception object.")
                    if outcome == "cancelled":
                        check.equal(caught.value.args, ("host cancellation payload",),
                                    msg="The host's cancellation payload survives the capture terminal path unchanged.")
                    else:
                        check.equal(caught.value.code, -32602 if outcome == "protocol-error" else types.REQUEST_TIMEOUT,
                                    msg="The actual native timeout and protocol error expose their distinct SDK codes.")
                check.equal(len(calls), 1, msg="Classifying any native outcome must not invoke the SDK twice or add a retry.")
            finally:
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    run_sdk_async(scenario())
    finish(recorder)
    expected = {"tool-error": "tool_error", "protocol-error": "protocol_error",
                "timeout": "timeout", "cancelled": "cancelled"}[outcome]
    check.equal(rows(path, "SELECT status,resolution,input_rounds FROM operations"),
                [{"status": expected, "resolution": "unknown", "input_rounds": None}],
                msg="The single justified terminal uses the existing error category with no invented operation resolution or rounds.")
    terminals = [row for row in events(path) if row["kind"] == "mcp.response"]
    check.equal(len(terminals), 1, msg="Each native return/error/cancellation produces at most one logical terminal.")
    check.equal(evidence.losses, (), msg="A native error or cancellation is observed evidence, not inherently a capture failure.")
    accepted(evidence.terminal)


@pytest.mark.parametrize("outcome", ["intermediate", "empty", "denied-intermediate"])
def test_session_native_intermediate_empty_and_denied_results_stay_distinct(
        recording, evidence_path, monkeypatch, run_sdk_async, outcome):
    recorder, path = recording
    evidence, projected = OperationEvidence(), []

    async def scenario():
        async with native_session(evidence_path, modern=True) as native:
            await native.list_tools()
            _, outcomes = spy_method(monkeypatch, native, "call_tool")
            observer = wrap_session(recorder, native, client_id="session")
            pending = observer.call_tool("echo", {"case": "empty" if outcome == "empty" else "intermediate"},
                                         allow_input_required=outcome != "denied-intermediate", evidence=evidence)
            if outcome == "denied-intermediate":
                with pytest.raises(RuntimeError) as caught:
                    await pending
                check.is_(caught.value, outcomes[0], msg="A caller-disallowed input-required response preserves the native SDK exception.")
            else:
                result = await pending
                check.is_(result, outcomes[0], msg="Allowed intermediate and complete empty returns retain exact native object identity.")
                check.is_instance(result, types.InputRequiredResult if outcome == "intermediate" else types.CallToolResult,
                                  msg="These controls exercise actual native SDK model conversion rather than constructed projection doubles.")
                projected.append(result.model_dump(mode="json", by_alias=True, exclude_unset=True))

    run_sdk_async(scenario())
    closed = finish(recorder)
    terminal = next(row for row in events(path) if row["kind"] == "mcp.response")
    check.equal(evidence.losses, (), msg="A trusted intermediate body and a native rejection require no invented integration loss code.")
    if outcome == "denied-intermediate":
        check.equal((terminal["state"], terminal["details"]["error"]["type"]), ("local_error", "RuntimeError"),
                    msg="Caller-disallowed intermediate work follows the actual native local exception path.")
    else:
        check.equal(terminal["body"], projected[0], msg="Retained result JSON is the exposed actual native model projection.")
        check.equal(terminal["state"], "ok", msg="Normal return means the method returned, without asserting completion of an intermediate tool.")
        if outcome == "intermediate":
            check.equal(terminal["body"]["requestState"], "resume-state", msg="The known intermediate's opaque state remains available under content retention.")
            check.is_false(terminal["details"]["complete"], msg="InputRequiredResult is observed partial work, not a completed tool result.")
            check.is_none(terminal["details"]["result_text_digest"]["value"], msg="An intermediate has no fabricated complete-result text token.")
            check.equal(closed.run_close_state, "incomplete", msg="Persisted partial intermediate evidence prevents a clean-run claim.")
        else:
            check.equal(terminal["body"]["content"], [], msg="Explicit empty content is preserved as an observed empty collection.")
            check.is_true(terminal["details"]["complete"], msg="A valid native empty result is complete observation rather than missing content.")
            check.is_not_none(terminal["details"]["result_text_digest"]["value"], msg="Observed empty text has a valid comparison token unlike an unavailable result.")
    check.equal(len([entry for entry in wire_log(evidence_path) if entry["method"] == "tools/call"]), 1,
                msg="The observer does not implement hidden retries or input subrounds for any return category.")


def test_session_real_missing_content_wire_preserves_sdk_validation_error(
        recording, evidence_path, monkeypatch, run_sdk_async):
    """Actual stdio/native parsing, with the peer explicitly labelled malformed."""
    recorder, path = recording
    evidence, validation = OperationEvidence(), []

    async def scenario():
        async with native_session(evidence_path, malformed=True) as native:
            with pytest.raises(ValidationError) as control:
                await native.call_tool("echo", {})
            calls, outcomes = spy_method(monkeypatch, native, "call_tool")
            with pytest.raises(ValidationError) as observed:
                await wrap_session(recorder, native, client_id="session").call_tool("echo", {}, evidence=evidence)
            check.is_(observed.value, outcomes[0], msg="The real malformed-wire SDK exception is surfaced identically through the observer.")
            check.equal(type(observed.value), type(control.value), msg="The native positive neighbor establishes the same parsing-error class without capture.")
            check.equal(len(calls), 1, msg="One observed malformed reply remains one actual native request.")
            validation.extend(observed.value.errors(include_url=False))
            check.is_true(any("content" in item["loc"] and item["type"] == "missing" for item in validation),
                          msg="The raw wire control must fail specifically for required content omission, not an unrelated malformed envelope.")

    run_sdk_async(scenario())
    finish(recorder)
    terminal = next(row for row in events(path) if row["kind"] == "mcp.response")
    check.equal((terminal["state"], terminal["details"]["error"]["type"]), ("local_error", "ValidationError"),
                msg="The SDK parsing failure is an exception terminal, never a fabricated successful missing-body result.")
    check.equal(len(wire_log(evidence_path)), 2, msg="The raw peer independently receives one native control and one observed call only.")


def test_session_metadata_intermediate_omits_the_known_partial_body(evidence_path, run_sdk_async):
    path = evidence_path / "recording.sqlite"
    recorder = Recorder(path, capture="metadata", coverage=COVERAGE)
    evidence = OperationEvidence()

    async def scenario():
        async with native_session(evidence_path, modern=True) as native:
            result = await wrap_session(recorder, native, client_id="session").call_tool(
                "echo", {"case": "intermediate"}, allow_input_required=True, evidence=evidence)
            check.is_instance(result, types.InputRequiredResult,
                              msg="Metadata retention must still return the actual known native intermediate model.")
            check.equal(result.request_state, "resume-state", msg="Omitting capture content does not omit the host's native result body.")

    try:
        run_sdk_async(scenario())
        closed = finish(recorder)
    finally:
        recorder.close(timeout=5)
    terminal = next(row for row in events(path) if row["kind"] == "mcp.response")
    check.equal((terminal["state"], terminal["body"], terminal["details"]["complete"]), ("ok", None, False),
                msg="The known intermediate remains a normal partial return when its retained payload body is omitted.")
    check.is_none(terminal["details"]["result_text_digest"]["value"], msg="Metadata retention cannot invent a completed-tool text token.")
    check.is_true(all(row["body_text"] is None for row in rows(path, "SELECT body_text FROM artifacts")),
                  msg="The intermediate exception to ordinary result projection still follows metadata body omission.")
    check.is_not_in("resume-state", json.dumps(events(path)), msg="The opaque input state cannot leak into control metadata.")
    check.equal((evidence.losses, closed.run_close_state), ((), "incomplete"),
                msg="Known partial evidence needs no invented loss code and cannot close as complete capture.")


@pytest.mark.parametrize("kind", ["unknown-claimed", "malformed-projection", "intermediate-without-allow", "legacy-intermediate"])
def test_session_labelled_projection_doubles_preserve_unknown_and_old_contract(recording, run_sdk_async, kind):
    """Collector-only doubles: these are not claimed as native SDK-produced models."""
    recorder, path = recording
    result = {"unknown-claimed": lambda: types.Result(meta={"opaque-claim": "unknown"}),
              "malformed-projection": lambda: types.CallToolResult.model_construct(is_error=False),
              "intermediate-without-allow": lambda: types.InputRequiredResult(request_state="opaque"),
              "legacy-intermediate": lambda: types.InputRequiredResult(request_state="opaque")}[kind]()
    native_calls = []

    class ProjectionDouble:
        async def list_tools(self, **kwargs):
            raise AssertionError("Projection-only tool controls must not invoke listing")

        async def call_tool(self, *args, **kwargs):
            native_calls.append((args, kwargs))
            return result

    evidence = OperationEvidence()
    observer = (wrap_mcp if kind == "legacy-intermediate" else wrap_session)(recorder, ProjectionDouble(), client_id="double")

    async def scenario():
        options = {"allow_claimed": True} if kind == "unknown-claimed" else {}
        returned = await observer.call_tool("echo", {}, evidence=evidence, **options)
        check.is_(returned, result, msg="Unknown/malformed supplied results must return as the exact original double object.")

    run_sdk_async(scenario())
    finish(recorder)
    terminal = next(row for row in events(path) if row["kind"] == "mcp.response")
    check.equal((terminal["state"], terminal["body"], terminal["details"]["complete"]), ("ok", None, False),
                msg="Unsupported results keep successful native return distinct from unavailable, incomplete projection.")
    check.is_none(terminal["details"]["result_text_digest"]["value"], msg="Unavailable or malformed content cannot receive a complete-result text token.")
    check.equal(evidence.losses, ("projection_failed",), msg="All labelled unsupported branches use the existing bounded projection-loss code.")
    check.equal(len(native_calls), 1, msg="A failed projection cannot execute the supplied native callable twice.")


@pytest.mark.parametrize("method", ["call_tool", "list_tools"], ids=["tool", "typed-listing"])
def test_session_invocation_scope_and_execution_snapshot_survive_later_completion(
        recording, evidence_path, monkeypatch, run_sdk_async, method):
    """Real stdio result plus a labelled completion gate around the native method."""
    recorder, path = recording
    evidence, never = OperationEvidence(), OperationEvidence()
    arguments, identities = {"text": "at-factory"}, {}
    params = types.PaginatedRequestParams(meta={"text": "at-factory"})

    async def scenario():
        returned_native, release = asyncio.Event(), asyncio.Event()
        async with native_session(evidence_path) as native:
            if method == "call_tool":
                await native.list_tools()
            original, actual = getattr(native, method), []

            @wraps(original)
            async def held(*args, **kwargs):
                result = await original(*args, **kwargs)
                actual.append(result)
                returned_native.set()
                await asyncio.wait_for(release.wait(), 3)
                return result

            monkeypatch.setattr(native, method, held)
            observer = wrap_session(recorder, native, client_id="session")
            with recorder.turn("factory A") as turn_a:
                identities["turn"] = accepted(turn_a.receipt).turn_id
                with recorder.dispatch(action="execute", name="echo", arguments=arguments) as dispatch:
                    identities["dispatch"] = accepted(dispatch.receipt).event_id
                    pending = (observer.call_tool("echo", arguments, evidence=evidence) if method == "call_tool"
                               else observer.list_tools(params=params, evidence=evidence))
                    abandoned = (observer.call_tool("echo", {"text": "never-executed"}, evidence=never) if method == "call_tool"
                                 else observer.list_tools(params=params, evidence=never))
            abandoned.close()
            check.equal(actual, [], msg="Exiting invocation scope and closing an unawaited factory cannot call the SDK.")
            check.is_none(evidence.start, msg="The executed call also has no start receipt until its coroutine actually runs.")
            arguments["text"] = "at-execution"
            params.meta["text"] = "at-execution"
            with recorder.turn("execution B"):
                ambient = recorder.capture_scope()
                task = asyncio.create_task(pending)
                try:
                    await asyncio.wait_for(returned_native.wait(), 3)
                    check.equal(accepted(evidence.start).turn_id, identities["turn"], msg="The start uses invocation A despite execution in B.")
                    arguments["text"] = "after-capture"
                    params.meta["text"] = "after-capture"
                    release.set()
                    result = await task
                    check.is_(result, actual[0], msg="The completion gate cannot replace the actual native SDK result object.")
                    check.equal(recorder.capture_scope(), ambient, msg="Observation never restores invocation context over the host's later turn.")
                    check.equal(accepted(evidence.terminal).turn_id, identities["turn"], msg="The immutable operation ticket preserves the original terminal scope.")
                finally:
                    release.set()
                    if not task.done():
                        task.cancel()
                    await asyncio.gather(task, return_exceptions=True)

    run_sdk_async(scenario())
    finish(recorder)
    start = next(row for row in events(path) if row["kind"] == "mcp.request")
    expected = {"text": "at-execution"} if method == "call_tool" else {"params": {"_meta": {"text": "at-execution"}}}
    check.equal(start["body"], {"arguments": expected},
                msg="Ordinary arguments and typed params freeze execution-time changes against later mutation.")
    check.equal(rows(path, "SELECT turn_id,dispatch_event_id,link_basis FROM operations"),
                [{"turn_id": identities["turn"], "dispatch_event_id": identities["dispatch"], "link_basis": "host_hook"}],
                msg="A later await cannot recover ambient B or lose the explicitly captured dispatch in A.")
    check.equal((never.start, never.terminal, never.losses), (None, None, ()), msg="The never-awaited neighbor contributes no operation or diagnostics.")
    wire_method = "tools/call" if method == "call_tool" else "tools/list"
    calls = [entry for entry in wire_log(evidence_path) if entry["method"] == wire_method]
    native_values = [entry["params"]["arguments"] if method == "call_tool" else entry["params"]["_meta"] for entry in calls]
    check.equal(native_values, [{"text": "at-execution"}],
                msg="The independent service ledger confirms only the one executed submitted value.")


def explicit_decision(recorder):
    request = accepted(recorder.observe("model.request", scope=recorder.capture_scope(), payload={"messages": []},
                                        facts={"messages": []}))
    decision = accepted(recorder.observe("model.tool_call", scope=recorder.capture_scope(),
        payload={"arguments_raw": '{"text":"secret café λ"}'},
        facts={"model_call_id": request.model_call_id, "name": "echo", "provider_ref": "reused-provider-ref"}))
    accepted(recorder.observe("model.response", scope=recorder.capture_scope(), payload={},
                              facts={"model_call_id": request.model_call_id}))
    return decision


@pytest.mark.parametrize("capture", ["content", "metadata"])
def test_session_retention_receipt_attribution_and_closed_recorder(
        evidence_path, run_sdk_async, capture):
    path = evidence_path / "recording.sqlite"
    recorder = Recorder(path, capture=capture, coverage=COVERAGE)
    valid, invalid, propagated, closed_evidence = (OperationEvidence() for _ in range(4))
    identities, hashes = {}, []
    arguments = {"text": "secret café λ"}

    async def scenario():
        async with native_session(evidence_path) as native:
            await native.list_tools()
            observer = wrap_session(recorder, native, client_id="session", server_key="peer")
            with recorder.turn("receipt-controlled A"):
                decision = explicit_decision(recorder)
                identities["decision"] = decision.decision_id
                with recorder.dispatch(action="execute", name="echo", arguments=arguments,
                                       decision_event_ids=[decision.event_id]) as dispatch:
                    identities["dispatch"] = accepted(dispatch.receipt).event_id
                    await observer.call_tool("echo", arguments, evidence=valid, decision=decision)
                    await observer.call_tool("echo", arguments, evidence=invalid,
                        decision=SimpleNamespace(status="rejected", decision_id=decision.decision_id))
                await observer.call_tool("echo", arguments, evidence=propagated, decision=decision)
                await observer.call_tool("echo", arguments)
                with check.raises(ValueError, msg="A completed operation's evidence cannot be reused or overwrite prior receipts."):
                    observer.call_tool("echo", arguments, evidence=valid)
            finish(recorder)
            hashes.append(hashlib.sha256(path.read_bytes()).hexdigest())
            result = await observer.call_tool("echo", {"text": "host still owns session"}, evidence=closed_evidence)
            check.equal(result.content[0].text, "host still owns session", msg="Recorder shutdown leaves the native transport usable by its host.")
            hashes.append(hashlib.sha256(path.read_bytes()).hexdigest())

    try:
        run_sdk_async(scenario())
    finally:
        closed = recorder.close(timeout=5)
        assert closed.writer_stopped, "Retention controls own and reap their recorder writer on every outcome."
    operations = rows(path, "SELECT decision_id,dispatch_event_id,link_basis,resolution,input_rounds FROM operations ORDER BY rowid")
    check.equal(operations, [
        {"decision_id": identities["decision"], "dispatch_event_id": identities["dispatch"], "link_basis": "host_hook", "resolution": "unknown", "input_rounds": None},
        {"decision_id": None, "dispatch_event_id": None, "link_basis": "none", "resolution": "unknown", "input_rounds": None},
        {"decision_id": identities["decision"], "dispatch_event_id": None, "link_basis": "propagated", "resolution": "unknown", "input_rounds": None},
        {"decision_id": None, "dispatch_event_id": None, "link_basis": "none", "resolution": "unknown", "input_rounds": None},
    ], msg="Only explicit usable receipts and captured dispatch establish attribution; invalid or absent receipts never infer it.")
    check.equal(invalid.losses, ("invalid_decision",), msg="Structurally invalid explicit receipts retain the existing bounded diagnostic.")
    starts = [row for row in events(path) if row["kind"] == "mcp.request"]
    check.equal([row["details"]["complete"] for row in starts], [True, False, True, True],
                msg="Invalid attribution is persisted as an incomplete start, not merely an ephemeral side-channel warning.")
    if capture == "metadata":
        artifacts = rows(path, "SELECT representation,body_text FROM artifacts")
        retained = [row for row in artifacts if row["body_text"] is not None]
        assert len(retained) == 1, "This one-request fixture retains exactly its public structural manifest before its shape is inspected."
        check.equal(retained[0]["representation"], "request_refs/2",
                    msg="Metadata may retain the structural manifest; ordinary model, argument and native-result bodies stay omitted.")
        manifest = json.loads(retained[0]["body_text"])
        check.equal(set(manifest), {"format", "template_ref", "message_path", "message_refs", "field_refs"},
                    msg="The retained structure contains only the existing manifest fields, never hidden submitted content.")
        check.equal((manifest["format"], manifest["message_path"], manifest["message_refs"], manifest["field_refs"]),
                    ("request_refs/2", "/messages", [], []),
                    msg="The declared empty model request retains its known empty membership without manufacturing payload data.")
        check.is_not_in("secret café λ", json.dumps(artifacts, ensure_ascii=False),
                        msg="No retained or omitted artifact metadata may conceal the private argument/result content.")
        check.is_not_in("secret café λ", json.dumps(events(path), ensure_ascii=False),
                        msg="Payload content cannot be hidden in event control metadata after body omission.")
    else:
        check.equal(starts[0]["body"], {"arguments": arguments}, msg="Content retention preserves the actual ordinary JSON argument body.")
    assert closed_evidence.start is not None, "Closed recording must retain its explicit rejected start receipt."
    check.equal((closed_evidence.start.status, closed_evidence.start.reason, closed_evidence.start.ticket),
                ("closed", "closed", None), msg="The public closed status cannot allocate a completion ticket after recorder shutdown.")
    check.is_none(closed_evidence.terminal, msg="A rejected late start must not manufacture a terminal operation.")
    check.equal(hashes[0], hashes[1], msg="Native work after recorder shutdown performs no later database write.")
    check.equal(len([entry for entry in wire_log(evidence_path) if entry["method"] == "tools/call"]), 5,
                msg="The service sees exactly four observed calls and the one explicit late host call, with no evidence-reuse call.")


def test_session_selective_retention_preserves_stdio_contract(
        recording, evidence_path, monkeypatch, run_sdk_async):
    """One genuine session proves policy routing without repeating the Client fault matrix."""
    recorder, path = recording
    marker = "session-policy-private café"
    params = types.PaginatedRequestParams(cursor=None, meta={"page-request": 1})
    arguments = [{"text": marker, "case": case} for case in ("return", "tool-error", "protocol-error")]
    meta, responses = {"owner": "native-meta"}, {}
    hooks, progress_events, evidences = [], [], [OperationEvidence() for _ in range(4)]
    never = OperationEvidence()

    def policy(kind, payload, context):
        hooks.append((kind, payload, context))
        if context["method"] == "tools/list":
            return {"policy_id": "catalog-omit", "omitted_reason": "catalog-policy"}
        return {"policy_id": "tool-redaction", "retained": {
            "phase": kind, "tool": context["name"], "visible": "MASKED"}, "redacted": True}

    async def progress(value, total, message):
        progress_events.append([value, total, message])

    async def scenario():
        async with native_session(evidence_path, modern=True, paged=True) as native:
            listings, listing_outcomes = spy_method(monkeypatch, native, "list_tools")
            calls, outcomes = spy_method(monkeypatch, native, "call_tool")
            observer = wrap_session(recorder, native, client_id="session-policy", server_key="peer",
                                    retention_policy=policy)
            pending = observer.call_tool("echo", {"text": "never-executed"}, evidence=never)
            try:
                check.equal((hooks, listings, calls, wire_log(evidence_path)), ([], [], [], []),
                            msg="An unawaited real-session call invokes neither policy nor native work.")
                check.equal((recorder.health().open_operations, recorder.health().completion.used_slots), (0, 0),
                            msg="Policy configuration cannot reserve capacity for work that never executes.")
            finally:
                pending.close()
            listing = await observer.list_tools(params=params, evidence=evidences[0])
            check.is_(listing, listing_outcomes[0], msg="Discovery omission preserves the exact SDK-created page.")
            check.equal(([tool.name for tool in listing.tools], listing.next_cursor), (["echo", "second"], "page-two"),
                        msg="The caller still receives the real ordered page and continuation token.")
            check.equal(len(listings), 1, msg="Policy routing cannot consume an unrequested page.")
            check.is_(listings[0][1]["params"], params, msg="The original typed pagination object reaches native listing.")
            for index, submitted in enumerate(arguments):
                pending = observer.call_tool("echo", submitted, 3.0, progress,
                    input_responses=responses, request_state="retention-state", meta=meta,
                    allow_input_required=True, allow_claimed=True, evidence=evidences[index + 1])
                if index == 2:
                    with pytest.raises(MCPError) as caught:
                        await pending
                    check.is_(caught.value, outcomes[index], msg="Protocol-exception redaction preserves native exception identity.")
                    check.equal(caught.value.code, -32602, msg="The real peer supplies the protocol error being redacted.")
                else:
                    returned = await pending
                    check.is_(returned, outcomes[index], msg="Normal and tool-error redaction preserve native result identity.")
                    check.equal((returned.content[0].text, returned.is_error), (marker, index == 1),
                                msg="Capture redaction changes neither original result text nor the native tool-error flag.")
                args, options = calls[index]
                check.is_(args[1], submitted, msg="Native code receives the host's original argument object.")
                check.equal((args[0], args[2]), ("echo", 3.0), msg="Policy does not rewrite the tool name or timeout.")
                check.is_(args[3], progress, msg="The native progress callback remains the host's exact callable.")
                check.is_(options["input_responses"], responses, msg="Input responses remain native options, outside capture payloads.")
                check.is_(options["meta"], meta, msg="Native metadata is forwarded by identity.")
                check.equal({key: options[key] for key in ("request_state", "allow_input_required", "allow_claimed")},
                            {"request_state": "retention-state", "allow_input_required": True, "allow_claimed": True},
                            msg="All current-session option values survive selective retention.")
            check.equal((len(listings), len(calls), len(outcomes)), (1, 3, 3),
                        msg="Listing plus three terminal categories cause exactly four native invocations.")

    run_sdk_async(scenario())
    closed = finish(recorder)
    check.equal(closed.run_close_state, "clean", msg="Declared omission/redaction preserves complete operation capture.")
    check.equal((never.start, never.terminal, never.losses), (None, None, ()),
                msg="Never-awaited work fabricates neither a receipt nor an evidence-loss outcome.")
    check.equal(progress_events, [[1, 2, "entered ✓"]] * 3,
                msg="The original progress callback receives the peer's real notifications for all three calls.")
    check.equal([hook[0] for hook in hooks], ["mcp.request", "mcp.response"] * 4,
                msg="Each exposed session request and terminal invokes the policy once.")
    check.equal([hook[2] for hook in hooks],
                [{"client_id": "session-policy", "server_key": "peer", "method": "tools/list", "name": None}] * 2 +
                [{"client_id": "session-policy", "server_key": "peer", "method": "tools/call", "name": "echo"}] * 6,
                msg="Each policy call has the exact invocation-local four-field context.")
    check.equal(hooks[0][1], {"arguments": {"params": {"_meta": {"page-request": 1}, "cursor": None}}},
                msg="Policy receives the complete typed-pagination request envelope, including explicit null and aliases.")
    check.equal([hooks[index][1] for index in (2, 4, 6)], [{"arguments": value} for value in arguments],
                msg="Tool policies receive only original tool arguments, without transport or callback options.")
    check.is_in("native protocol explanation café", hooks[-1][1]["error"]["message"],
                msg="The genuine protocol-error body reaches the policy before its recorded replacement is chosen.")
    ledger = wire_log(evidence_path)
    check.equal([entry["method"] for entry in ledger], ["tools/list"] + ["tools/call"] * 3,
                msg="The independent wire ledger proves no hidden page, retry or abandoned call ran.")
    check.equal((ledger[0]["params"].get("cursor"), ledger[0]["params"]["_meta"]["page-request"]), (None, 1),
                msg="The service receives the original pagination fields alongside any native SDK protocol metadata.")
    check.equal([entry["params"]["arguments"] for entry in ledger[1:]], arguments,
                msg="All tool outcomes originate from the unredacted values on the real wire.")
    observed = events(path)
    operations = rows(path, "SELECT * FROM operations ORDER BY rowid")
    assert len(observed) == 8 and len(operations) == 4, "The scenario requires four admitted request/terminal pairs."
    check.equal([operation["status"] for operation in operations], ["ok", "ok", "tool_error", "protocol_error"],
                msg="Selective content retention preserves the actual native terminal categories.")
    check.equal(len({operation["operation_id"] for operation in operations}), 4,
                msg="Even identically redacted operations keep distinct occurrence identities.")
    for operation, evidence in zip(operations, evidences):
        start, terminal = accepted(evidence.start), accepted(evidence.terminal)
        check.equal((operation["operation_id"], operation["start_event_id"], operation["end_event_id"]),
                    (start.operation_id, start.event_id, terminal.event_id), msg="Each operation binds its own admitted start and terminal.")
        check.equal([row["kind"] for row in observed if row["operation_id"] == operation["operation_id"]],
                    ["mcp.request", "mcp.response"], msg="A policy cannot add a second capture or lose a terminal.")
        check.equal(evidence.losses, (), msg="An explicit valid omission or redaction is not an adapter policy failure.")
    stored = rows(path, """SELECT a.body_text,a.stored_sha256,a.stored_bytes,a.policy_id,
        a.omitted_reason,a.redacted,a.truncated FROM events e
        JOIN artifacts a ON a.artifact_id=e.payload_ref ORDER BY e.sequence""")
    assert len(stored) == 8, "Every admitted request and terminal must own its asserted retained or omitted artifact."
    check.equal(stored[:2], [{"body_text": None, "stored_sha256": None, "stored_bytes": 0,
                             "policy_id": "catalog-omit", "omitted_reason": "catalog-policy", "redacted": 0, "truncated": 0}] * 2,
                msg="Discovery request and response have the exact declared omission metadata and no body.")
    expected_bodies = [b'{"phase":"mcp.request","tool":"echo","visible":"MASKED"}',
                       b'{"phase":"mcp.response","tool":"echo","visible":"MASKED"}'] * 3
    for artifact, expected in zip(stored[2:], expected_bodies):
        check.equal(artifact, {"body_text": expected.decode(), "stored_sha256": hashlib.sha256(expected).hexdigest(),
                              "stored_bytes": len(expected), "policy_id": "tool-redaction", "omitted_reason": None,
                              "redacted": 1, "truncated": 0},
                    msg="Normal, tool-error and protocol-error artifacts retain exactly the literal declared replacement.")
    check.is_not_in(marker, json.dumps(stored, ensure_ascii=False), msg="Native private arguments/results never survive in retained artifacts.")
    check.is_not_in("native protocol explanation café", json.dumps(stored, ensure_ascii=False),
                    msg="Native exception text is redacted along with ordinary result content.")
    health = recorder.health()
    check.equal((health.open_operations, health.completion.used_slots, health.completion.used_bytes), (0, 0, 0),
                msg="Joined completion releases every operation and reserved completion byte exactly once.")
