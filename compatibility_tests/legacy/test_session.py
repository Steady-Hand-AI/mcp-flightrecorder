"""A2–A6: actual SDK1 calls plus explicitly labelled loss/lifecycle doubles."""
import asyncio
from datetime import timedelta
import hashlib
import inspect as python_inspect
import json
from types import SimpleNamespace

import anyio
import httpx
from mcp import ClientSession
from mcp.shared.exceptions import McpError
import mcp.types as types
from pydantic import ValidationError
import pytest
import pytest_check as check

from mcp_flightrecorder import Recorder
from mcp_flightrecorder.integrations._mcp_capture import OperationEvidence as SharedEvidence
from mcp_flightrecorder.integrations.mcp_session_legacy import OperationEvidence, wrap_session
from .support import (COVERAGE, events, finish, load_example, native_session,
                      rows, spy_method, wire_log)


@pytest.fixture
def recording(evidence_path):
    path = evidence_path / "recording.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    try:
        yield recorder, path
    finally:
        closed = recorder.close(timeout=5)
        assert closed.writer_stopped, "Each test must join its owned writer even after an assertion fails."


def accepted(receipt):
    assert receipt is not None and receipt.status == "accepted", (
        "Receipt-dependent scope and identity comparisons require actual admission."
    )
    return receipt


def test_public_signature_evidence_ownership_and_never_awaited(recording):
    recorder, path = recording
    calls = []

    class FactoryDouble:
        def list_tools(self, *args, **kwargs):
            calls.append("list")
            raise AssertionError("Unawaited factories must not call native methods.")

        def call_tool(self, *args, **kwargs):
            calls.append("call")
            raise AssertionError("Unawaited factories must not call native methods.")

    native = FactoryDouble()
    observer = wrap_session(recorder, native, client_id="é" * 128, server_key="s" * 256)
    check.is_(OperationEvidence, SharedEvidence, msg="All MCP facades must use the exact one-invocation evidence class.")
    check.equal(str(python_inspect.signature(observer.list_tools)), "(cursor=None, *, params=None, evidence=None)",
                msg="SDK1 exposes positional cursor and keyword-only typed params without SDK2 options.")
    check.equal(str(python_inspect.signature(observer.call_tool)),
                "(name, arguments=None, read_timeout_seconds=None, progress_callback=None, *, meta=None, evidence=None, decision=None)",
                msg="Legacy tool calls preserve the native positional/keyword boundaries and add only recorder options.")
    check.equal(list(python_inspect.signature(ClientSession.list_tools).parameters), ["self", "cursor", "params"],
                msg="The installed SDK1 method, rather than a guessed signature, is this facade's native boundary.")
    check.is_false(any(hasattr(observer, name) for name in ("__aenter__", "__aexit__", "close", "aclose")),
                   msg="A two-method observer never assumes transport lifecycle ownership.")
    for label in ("", "é" * 129, "\ud800", 42):
        with check.raises(ValueError, msg="Invalid UTF-8/nonempty/bounded labels fail before any native construction."):
            wrap_session(recorder, native, client_id=label)
    with check.raises(TypeError, msg="Unrelated evidence is explicit instrumentation misuse before native work."):
        observer.list_tools(evidence=object())
    evidence = OperationEvidence()
    with recorder.turn("invocation only"):
        pending = observer.call_tool("echo", evidence=evidence)
    assert python_inspect.iscoroutine(pending), "The public factory must return a closeable lazy Python coroutine."
    try:
        with check.raises(ValueError, msg="Evidence belongs to its first invocation, even before execution."):
            observer.list_tools(evidence=evidence)
        check.equal(calls, [], msg="Unawaited work cannot create a native coroutine, invocation, event or reservation.")
        check.equal((recorder.health().open_operations, recorder.health().completion.used_slots), (0, 0),
                    msg="Lazy factories own neither operations nor completion capacity.")
    finally:
        pending.close()
    finish(recorder, path)
    check.equal((evidence.start, evidence.terminal, evidence.losses), (None, None, ()),
                msg="Closing never-executed work does not fabricate a lifecycle outcome.")
    check.equal(events(path), [], msg="A never-awaited call leaves no MCP events.")


@pytest.mark.parametrize("shape", ["null", "empty", "explicit-null"])
def test_typed_params_preserve_absent_empty_and_explicit_null(
        recording, evidence_path, monkeypatch, run_sdk_async, shape):
    recorder, path = recording
    params = {"null": None, "empty": types.PaginatedRequestParams(),
              "explicit-null": types.PaginatedRequestParams(cursor=None, _meta=None)}[shape]
    expected = {"null": None, "empty": {}, "explicit-null": {"cursor": None, "_meta": None}}[shape]
    body = []

    async def scenario():
        async with native_session(evidence_path) as native:
            calls, outcomes = spy_method(monkeypatch, native, "list_tools")
            result = await wrap_session(recorder, native, client_id="legacy").list_tools(params=params)
            check.is_(result, outcomes[0], msg="Discovery returns the identical SDK-created result object.")
            check.equal(len(calls), 1, msg="One facade invocation must produce one actual native call.")
            check.equal(calls[0][0], (None,), msg="The declared legacy cursor default reaches the SDK unchanged.")
            check.is_(calls[0][1]["params"], params, msg="Typed params are forwarded by identity, not replaced by their projection.")
            check.equal(set(calls[0][1]), {"params"}, msg="Recorder-only arguments never leak to native discovery.")
            body.append(result.model_dump(mode="json", by_alias=True, exclude_unset=True))

    run_sdk_async(scenario())
    facts = finish(recorder, path)
    check.equal(facts["events"][0]["body"], {"arguments": {"cursor": None, "params": expected}},
                msg="Null, empty and explicit-null params are distinct capture values.")
    check.equal(facts["events"][1]["body"], body[0], msg="Exposed page fields and aliases remain exactly as the SDK returned them.")
    check.equal([entry["method"] for entry in wire_log(evidence_path)], ["tools/list"],
                msg="Observing a page does not fetch another or fabricate network traffic.")


@pytest.mark.parametrize("style", ["cursor", "params"])
def test_explicit_pagination_keeps_order_hints_and_native_both_supplied_error(
        recording, evidence_path, monkeypatch, run_sdk_async, style):
    recorder, path = recording
    expected_bodies, errors = [], []
    first_params = types.PaginatedRequestParams(cursor=None, _meta={"page-request": 1})
    next_params = types.PaginatedRequestParams(cursor="page-two", _meta={"page-request": 2})

    async def scenario():
        async with native_session(evidence_path, paged=True) as native:
            calls, outcomes = spy_method(monkeypatch, native, "list_tools")
            observer = wrap_session(recorder, native, client_id="legacy")
            first = await observer.list_tools(params=first_params) if style == "params" else await observer.list_tools()
            check.equal(first.nextCursor, "page-two", msg="The actual page exposes the continuation token to its caller.")
            check.equal(len(wire_log(evidence_path)), 1, msg="The observer must not eagerly consume the exposed next page.")
            second = await observer.list_tools(params=next_params) if style == "params" else await observer.list_tools("page-two")
            for index, result in enumerate((first, second)):
                check.is_(result, outcomes[index], msg="Each explicit page retains its own native identity.")
                expected_bodies.append(result.model_dump(mode="json", by_alias=True, exclude_unset=True))
            try:
                await native.list_tools("page-two", params=next_params)
            except ValueError as error:
                errors.append(error)
            try:
                await observer.list_tools("page-two", params=next_params)
            except ValueError as error:
                errors.append(error)
                check.is_(error, outcomes[-1], msg="Simultaneous cursor/params preserves the precise native raised exception.")
            check.equal(len(errors), 2, msg="Both plain and recorded simultaneous inputs must reach native rejection.")
            check.equal(str(errors[1]), str(errors[0]), msg="The SDK owns the unchanged precedence/rejection message and timing.")

    run_sdk_async(scenario())
    facts = finish(recorder, path)
    responses = [event for event in facts["events"] if event["kind"] == "mcp.response"]
    check.equal([event["body"] for event in responses[:2]], expected_bodies,
                msg="Ordered pages remain independent exposed bodies, including cache eligibility hints.")
    check.equal([[tool["name"] for tool in body["tools"]] for body in expected_bodies],
                [["echo", "refuse", "wait"], ["last"]], msg="Native declaration ordering must survive pagination.")
    check.equal([body["ttlMs"] for body in expected_bodies], [1000, 1000],
                msg="Exposed eligibility metadata is retained without claiming served cache provenance.")
    check.equal([event["state"] for event in responses], ["ok", "ok", "local_error"],
                msg="Native both-supplied rejection is a local-error terminal, not a projection-only gap.")
    check.equal(len(wire_log(evidence_path)), 2, msg="Both rejected calls must fail before sending another protocol request.")


@pytest.mark.parametrize("case", ["return", "empty", "tool-error"])
def test_native_result_content_schema_options_and_progress(
        recording, evidence_path, monkeypatch, run_sdk_async, case):
    recorder, path = recording
    arguments, meta, timeout = {"text": "submitted café ✓", "case": case}, {"owner": "native-meta"}, timedelta(seconds=3)
    progress_events, projected = [], []
    evidence = OperationEvidence()

    async def progress(value, total, message):
        progress_events.append([value, total, message])

    async def scenario():
        async with native_session(evidence_path) as native:
            await native.list_tools()
            calls, outcomes = spy_method(monkeypatch, native, "call_tool")
            returned = await wrap_session(recorder, native, client_id="legacy").call_tool(
                "echo", arguments, timeout, progress, meta=meta, evidence=evidence)
            check.is_(returned, outcomes[0], msg="Success, explicit empty content and tool error all preserve native result identity.")
            assert len(calls) == 1, "Forwarding facts require exactly one native method invocation."
            args, kwargs = calls[0]
            check.is_(args[1], arguments, msg="The native argument object must remain the host's original object.")
            check.is_(args[2], timeout, msg="SDK1 read_timeout_seconds is the original timedelta, not a float conversion.")
            check.is_(args[3], progress, msg="Progress delivery uses the original host callback unchanged.")
            check.is_(kwargs["meta"], meta, msg="Native metadata forwarding preserves identity.")
            check.equal(set(kwargs), {"meta"}, msg="No SDK2 flags or recorder options may leak into the SDK1 call.")
            projected.append(returned.model_dump(mode="json", by_alias=True, exclude_unset=True))

    run_sdk_async(scenario())
    facts = finish(recorder, path)
    terminal = facts["events"][1]
    check.equal(facts["events"][0]["body"], {"arguments": arguments},
                msg="Only tool arguments enter the operation body; callbacks, metadata and transport options remain outside it.")
    check.equal(terminal["body"], projected[0], msg="The exposed structured/nontext/text result body is retained without defaults.")
    check.equal(terminal["state"], "tool_error" if case == "tool-error" else "ok",
                msg="SDK1's actual isError field, rather than SDK2 spelling or text, determines tool-error status.")
    check.is_true(terminal["details"]["complete"], msg="Both observed empty content and exposed ordinary results are complete observations.")
    check.is_not_none(terminal["details"]["result_text_digest"]["value"],
                      msg="A known empty text list has a valid comparison token, unlike missing content.")
    check.equal(progress_events, [[1.0, 2.0, "entered ✓"]], msg="The real protocol progress notification reaches the host once.")
    check.equal(evidence.losses, (), msg="Native outcomes and nontext content do not create invented capture loss.")
    check.equal([(o["resolution"], o["input_rounds"]) for o in facts["operations"]], [("unknown", None)],
                msg="SDK1 schema refresh and returned hints do not prove resolution or input rounds.")


@pytest.mark.parametrize("case", ["protocol-error", "timeout", "cancelled", "transport-close"])
def test_actual_sdk_errors_and_cancellation_preserve_native_identity(
        recording, evidence_path, monkeypatch, run_sdk_async, case):
    recorder, path = recording
    evidence, errors = OperationEvidence(), []

    async def scenario():
        async with native_session(evidence_path) as native:
            calls, outcomes = spy_method(monkeypatch, native, "call_tool")
            pending = wrap_session(recorder, native, client_id="legacy").call_tool(
                "echo", {"case": case}, timedelta(seconds=0.1 if case == "timeout" else 3),
                evidence=evidence)
            task = asyncio.create_task(pending)
            try:
                if case == "cancelled":
                    for _ in range(200):
                        if any(entry["method"] == "tools/call" for entry in wire_log(evidence_path)):
                            break
                        await asyncio.sleep(0.01)
                    assert any(entry["method"] == "tools/call" for entry in wire_log(evidence_path)), (
                        "Cancellation must follow an independently witnessed submitted native request."
                    )
                    task.cancel("host cancellation payload")
                try:
                    await task
                except BaseException as error:
                    errors.append(error)
                    check.is_(error, outcomes[0], msg="Native errors and cancellation propagate as the identical surfaced object.")
                check.equal(len(calls), 1, msg="No terminal classification may retry native work.")
            finally:
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    run_sdk_async(scenario())
    assert len(errors) == 1, "A terminal-error comparison requires the actual native failure to have surfaced."
    facts = finish(recorder, path)
    terminal = facts["events"][1]
    expected = {"protocol-error": ("protocol_error", -32602), "timeout": ("timeout", 408),
                "cancelled": ("cancelled", None), "transport-close": ("protocol_error", -32000)}[case]
    check.equal((terminal["state"], terminal["details"]["error"]["code"]), expected,
                msg="SDK1 wraps closed transport as McpError -32000 and implements read timeout as code408.")
    check.equal(evidence.losses, (), msg="An accurately observed native failure is distinct from projection/capture loss.")
    if case == "cancelled":
        check.equal(errors[0].args, ("host cancellation payload",), msg="The caller's cancellation payload must survive unchanged.")


def test_malformed_wire_is_a_native_validation_error(recording, evidence_path, monkeypatch, run_sdk_async):
    recorder, path = recording
    validation = []

    async def scenario():
        async with native_session(evidence_path, malformed=True) as native:
            with pytest.raises(ValidationError) as plain:
                await native.call_tool("echo", {})
            _, outcomes = spy_method(monkeypatch, native, "call_tool")
            with pytest.raises(ValidationError) as captured:
                await wrap_session(recorder, native, client_id="legacy").call_tool("echo", {})
            check.is_(captured.value, outcomes[0], msg="Malformed wire remains the identical SDK exception through observation.")
            check.equal(type(captured.value), type(plain.value), msg="The plain neighboring call establishes native parser behavior.")
            validation.extend(captured.value.errors(include_url=False))
            check.is_true(any(item["type"] == "missing" and "content" in item["loc"] for item in validation),
                          msg="The labelled wire double must fail for required content omission, not an unrelated transport issue.")

    run_sdk_async(scenario())
    facts = finish(recorder, path)
    check.equal(facts["events"][1]["state"], "local_error",
                msg="A native parser rejection is never rewritten as a successful missing-body return.")
    check.equal(len([entry for entry in wire_log(evidence_path) if entry["method"] == "tools/call"]), 2,
                msg="One plain control plus one recorded call must be the only actual tool submissions.")


@pytest.mark.parametrize("case", ["missing", "nonarray", "malformed-text", "unknown"])
def test_labelled_returned_projection_doubles_remain_partial(recording, case):
    recorder, path = recording
    result = {"missing": lambda: types.CallToolResult.model_construct(isError=False),
              "nonarray": lambda: types.CallToolResult.model_construct(content={}, isError=False),
              "malformed-text": lambda: types.CallToolResult.model_construct(
                  content=[types.TextContent.model_construct(type="text", text=17)], isError=False),
              "unknown": object}[case]()
    calls, evidence = [], OperationEvidence()

    class ReturnedDouble:
        async def list_tools(self, *args, **kwargs):
            raise AssertionError("This projection-only double has no discovery work.")

        async def call_tool(self, *args, **kwargs):
            calls.append((args, kwargs))
            return result

    async def scenario():
        actual = await wrap_session(recorder, ReturnedDouble(), client_id="double").call_tool("echo", {}, evidence=evidence)
        check.is_(actual, result, msg="Unsupported native returns are passed through without replacement or fabricated validation.")

    asyncio.run(scenario())
    facts = finish(recorder, path)
    terminal = facts["events"][1]
    check.equal((terminal["state"], terminal["body"], terminal["details"]["complete"]), ("ok", None, False),
                msg="A successful native return remains distinct from unavailable or malformed capture content.")
    check.is_none(terminal["details"]["result_text_digest"]["value"],
                  msg="Missing/malformed content cannot receive a complete empty-text comparison token.")
    check.equal((len(calls), evidence.losses), (1, ("projection_failed",)),
                msg="Failed projection invokes native work once and reports only the bounded projection-loss code.")


ERROR_CASES = ["cancel", "builtin-timeout", "sdk-timeout", "sdk-inactive-code", "sdk-protocol",
               "httpx-read", "httpx-timeout", "end", "broken", "closed", "local", "oversized-code"]


@pytest.mark.parametrize("case", ERROR_CASES)
def test_labelled_exception_identity_and_bounded_metadata(recording, case):
    recorder, path = recording
    constructors = {
        "cancel": lambda: asyncio.CancelledError("unchanged"),
        "builtin-timeout": lambda: TimeoutError("native timeout"),
        "sdk-timeout": lambda: McpError(types.ErrorData(code=408, message="SDK timeout")),
        "sdk-inactive-code": lambda: McpError(types.ErrorData(code=-32001, message="not implemented timeout")),
        "sdk-protocol": lambda: McpError(types.ErrorData(code=-32602, message="protocol")),
        "httpx-read": lambda: httpx.ReadError("native transport"),
        "httpx-timeout": lambda: httpx.ReadTimeout("native transport timeout"),
        "end": anyio.EndOfStream, "broken": anyio.BrokenResourceError, "closed": anyio.ClosedResourceError,
        "local": lambda: ValueError("local"), "oversized-code": lambda: McpError(types.ErrorData(code=2**70, message="large")),
    }
    error, evidence, calls = constructors[case](), OperationEvidence(), []

    class ExceptionDouble:
        async def list_tools(self, *args, **kwargs):
            raise AssertionError("The exception-only tool double must not run discovery.")

        async def call_tool(self, *args, **kwargs):
            calls.append("entered")
            raise error

    async def scenario():
        try:
            await wrap_session(recorder, ExceptionDouble(), client_id="double").call_tool("echo", {}, evidence=evidence)
        except BaseException as caught:
            check.is_(caught, error, msg="Every classified error retains exact native object identity.")
        else:
            pytest.fail("The supplied native exception must propagate unchanged rather than being swallowed.")

    asyncio.run(scenario())
    facts = finish(recorder, path)
    expected_status = ("cancelled" if case == "cancel" else "timeout" if case in ("builtin-timeout", "sdk-timeout")
                       else "protocol_error" if case.startswith("sdk-") or case == "oversized-code"
                       else "transport_error" if case in ("httpx-read", "httpx-timeout", "end", "broken", "closed")
                       else "local_error")
    expected_code = {"sdk-timeout": 408, "sdk-inactive-code": -32001, "sdk-protocol": -32602}.get(case)
    terminal = facts["events"][1]
    check.equal((terminal["state"], terminal["details"]["error"]["code"]), (expected_status, expected_code),
                msg="Legacy native categories use the implemented code408 rule and bounded signed-64-bit control codes.")
    check.equal(terminal["details"]["error"]["type"], type(error).__name__,
                msg="Control metadata keeps the native type label, never its message.")
    check.equal(len(calls), 1, msg="Exception translation cannot repeat the underlying call.")
    check.equal(evidence.losses, ("projection_failed",) if case == "oversized-code" else (),
                msg="An unrepresentable native code makes projection partial while ordinary native failures remain complete observations.")


@pytest.mark.parametrize("assigned", [False, True], ids=["direct-proxy", "assigned-hook"])
@pytest.mark.parametrize("discovered", [False, True], ids=["call-first", "listed-first"])
def test_actual_internal_discovery_counts_at_the_chosen_boundary(
        recording, evidence_path, monkeypatch, run_sdk_async, assigned, discovered):
    recorder, path = recording

    async def scenario():
        async with native_session(evidence_path) as native:
            if discovered:
                await native.list_tools()
            list_calls, _ = spy_method(monkeypatch, native, "list_tools")
            tool_calls, results = spy_method(monkeypatch, native, "call_tool")
            if assigned:
                hook = load_example("mcp_agent_session").make_session_init_hook(recorder, client_id="hook")
                check.is_true(hook(native, object()), msg="The maintained hook attaches both methods to an initialized native session.")
                observed = native
            else:
                observed = wrap_session(recorder, native, client_id="proxy")
            result = await observed.call_tool("echo", {"text": "native refresh"})
            check.is_(result, results[0], msg="Direct and assigned observation return the actual SDK-created object.")
            check.equal(len(tool_calls), 1, msg="Attachment cannot duplicate the outer native tool invocation.")
            check.equal(len(list_calls), 0 if discovered else 1,
                        msg="SDK1 owns one cache-schema refresh only when the tool was not already discovered.")

    run_sdk_async(scenario())
    facts = finish(recorder, path)
    check.equal(sorted(operation["method"] for operation in facts["operations"]),
                ["tools/call", "tools/list"] if assigned and not discovered else ["tools/call"],
                msg="Assigned methods observe actual internal discovery; a direct proxy records only explicit proxy calls.")
    check.equal([entry["method"] for entry in wire_log(evidence_path) if entry["method"] in ("tools/list", "tools/call")],
                ["tools/list", "tools/call"] if discovered else ["tools/call", "tools/list"],
                msg="The independent wire ledger proves native ordering without suppression, deadlock, extra call or inferred operation.")
    check.is_true(all(operation["decision_id"] is None and operation["dispatch_event_id"] is None
                      for operation in facts["operations"]), msg="Schema refresh and host success provide no causal receipt attribution.")


@pytest.mark.parametrize("method", ["call", "list"])
def test_invocation_scope_execution_snapshot_and_overlapping_equal_calls(recording, method):
    recorder, path = recording
    arguments = {"text": "at-factory"}
    params = types.PaginatedRequestParams(_meta={"text": "at-factory"})
    evidences, results, entered, releases, seen = [OperationEvidence(), OperationEvidence()], [], [], [], []

    class GatedDouble:
        async def list_tools(self, cursor=None, *, params=None):
            return await self.wait(params)

        async def call_tool(self, name, arguments=None, *args, **kwargs):
            return await self.wait(arguments)

        async def wait(self, original):
            position = len(seen)
            seen.append(original)
            result = (types.ListToolsResult(tools=[]) if method == "list" else
                      types.CallToolResult(content=[types.TextContent(type="text", text="returned")]))
            results.append(result)
            entered[position].set()
            await releases[position].wait()
            return result

    identities = {}

    async def scenario():
        entered.extend([asyncio.Event(), asyncio.Event()])
        releases.extend([asyncio.Event(), asyncio.Event()])
        observer = wrap_session(recorder, GatedDouble(), client_id="gated")
        with recorder.turn("invocation A") as turn:
            identities["turn"] = accepted(turn.receipt).turn_id
            with recorder.dispatch(action="execute", name="echo", arguments=arguments) as dispatch:
                identities["dispatch"] = accepted(dispatch.receipt).event_id
                pending = [observer.call_tool("echo", arguments, evidence=item) if method == "call" else
                           observer.list_tools(params=params, evidence=item) for item in evidences]
        arguments["text"] = "at-execution"
        params.meta.text = "at-execution"
        with recorder.turn("await B"):
            ambient = recorder.capture_scope()
            tasks = [asyncio.create_task(item) for item in pending]
            try:
                await asyncio.wait_for(asyncio.gather(*(event.wait() for event in entered)), 2)
                check.is_not(accepted(evidences[0].start).operation_id, None,
                             msg="Overlapping equal calls each receive their own operation identity.")
                check.not_equal(evidences[0].start.operation_id, evidences[1].start.operation_id,
                                msg="Equal arguments cannot merge independent invocations.")
                arguments["text"] = "after-capture"
                params.meta.text = "after-capture"
                releases[1].set()
                check.is_(await tasks[1], results[1], msg="Reverse completion returns its own native object, not the equal neighbor's.")
                releases[0].set()
                check.is_(await tasks[0], results[0], msg="Earlier invocation retains its own result despite later completion.")
                check.equal(recorder.capture_scope(), ambient, msg="The adapter cannot restore creation context over the host's ambient turn.")
            finally:
                for release in releases:
                    release.set()
                await asyncio.gather(*tasks, return_exceptions=True)

    asyncio.run(scenario())
    facts = finish(recorder, path)
    expected = {"text": "at-execution"} if method == "call" else {
        "cursor": None, "params": {"_meta": {"text": "at-execution"}}}
    check.equal([event["body"] for event in facts["events"] if event["kind"] == "mcp.request"],
                [{"arguments": expected}] * 2, msg="Inputs freeze at execution and survive later shared-object mutation.")
    check.equal([(operation["turn_id"], operation["dispatch_event_id"], operation["link_basis"])
                 for operation in facts["operations"]], [(identities["turn"], identities["dispatch"], "host_hook")] * 2,
                msg="Both operations and their late terminals preserve captured invocation scope and exact dispatch.")


@pytest.mark.parametrize("mode", ["start-raises", "finish-raises", "start-rejected", "closed-before", "closed-during"])
def test_capture_failure_and_shutdown_do_not_own_native_work(recording, monkeypatch, mode):
    recorder, path = recording
    evidence, calls = OperationEvidence(), []
    result = types.CallToolResult(content=[])

    class NativeDouble:
        async def list_tools(self, *args, **kwargs):
            raise AssertionError("This terminal control owns no listing.")

        async def call_tool(self, *args, **kwargs):
            calls.append("entered")
            if mode == "closed-during":
                recorder.close(timeout=5)
            return result

    def refused(*args, **kwargs):
        raise RuntimeError("test-only capture failure")

    if mode == "start-raises":
        monkeypatch.setattr(recorder, "start_operation", refused)
    if mode == "finish-raises":
        monkeypatch.setattr(recorder, "finish_operation", refused)
    if mode == "start-rejected":
        monkeypatch.setattr(recorder, "start_operation", lambda **kwargs:
                            SimpleNamespace(status="rejected", ticket=None))
    if mode == "closed-before":
        recorder.close(timeout=5)
    before = hashlib.sha256(path.read_bytes()).hexdigest() if mode == "closed-before" else None

    async def scenario():
        returned = await wrap_session(recorder, NativeDouble(), client_id="double").call_tool("echo", {}, evidence=evidence)
        check.is_(returned, result, msg="Capture failure or recorder shutdown cannot replace or prevent the native result.")

    asyncio.run(scenario())
    facts = finish(recorder, path)
    check.equal(calls, ["entered"], msg="Every loss/closure branch still invokes the host's native callable exactly once.")
    if mode in ("start-raises", "start-rejected", "closed-before"):
        check.equal(facts["operations"], [], msg="No accepted start means no invented ticket or persisted operation.")
        check.is_none(evidence.terminal, msg="A rejected or absent start cannot acquire a synthetic terminal.")
    if mode in ("start-raises", "finish-raises"):
        check.is_in("capture_rejected", evidence.losses, msg="Contained capture faults remain visible in bounded side-channel diagnostics.")
    if mode == "closed-before":
        check.equal(hashlib.sha256(path.read_bytes()).hexdigest(), before,
                    msg="Native work after recorder close cannot mutate the closed database.")
    if mode == "closed-during":
        check.equal(evidence.terminal.status, "closed", msg="Late native completion reports recorder closure without reopening it.")


@pytest.mark.parametrize("capture", ["content", "metadata"])
def test_invalid_and_foreign_receipts_preserve_uncertainty_and_retention(evidence_path, capture):
    path = evidence_path / "recording.sqlite"
    recorder = Recorder(path, capture=capture, coverage=COVERAGE)
    arguments = {"secret": "private café λ"}
    result = types.CallToolResult(content=[types.TextContent(type="text", text=arguments["secret"])])
    evidence = [OperationEvidence(), OperationEvidence()]

    class NativeDouble:
        async def list_tools(self, *args, **kwargs):
            raise AssertionError("Receipt controls do not invoke discovery.")

        async def call_tool(self, *args, **kwargs):
            return result

    async def scenario():
        observer = wrap_session(recorder, NativeDouble(), client_id="double")
        with recorder.turn("receipts"):
            with recorder.dispatch(action="execute", name="echo", arguments=arguments):
                await observer.call_tool("echo", arguments, evidence=evidence[0],
                    decision=SimpleNamespace(status="rejected", decision_id="invalid-decision"))
            await observer.call_tool("echo", arguments, evidence=evidence[1],
                decision=SimpleNamespace(status="accepted", decision_id="foreign-decision"))
        with check.raises(ValueError, msg="Completed evidence cannot be reused or overwrite another call's receipts."):
            observer.call_tool("echo", evidence=evidence[0])

    try:
        asyncio.run(scenario())
        facts = finish(recorder, path)
    finally:
        recorder.close(timeout=5)
    check.equal([(o["decision_id"], o["dispatch_event_id"], o["link_basis"]) for o in facts["operations"]],
                [(None, None, "none"), ("foreign-decision", None, "propagated")],
                msg="Malformed receipts erase attribution; structurally usable foreign identity stays explicit and unresolved.")
    check.equal(evidence[0].losses, ("invalid_decision",), msg="Invalid receipt structure uses the existing bounded diagnostic.")
    starts = [e for e in facts["events"] if e["kind"] == "mcp.request"]
    check.equal([e["details"]["complete"] for e in starts], [False, True],
                msg="Invalid attribution is persisted incomplete, while ownership resolution stays with the core.")
    check.equal(rows(path, "SELECT * FROM links"), [], msg="Equal values and a foreign receipt cannot fabricate valid causal links.")
    if capture == "metadata":
        check.is_true(all(row["body_text"] is None for row in rows(path, "SELECT body_text FROM artifacts")),
                      msg="Metadata capture omits all tool argument, result and prompt payload bodies.")
        check.is_not_in(arguments["secret"], json.dumps(facts, ensure_ascii=False),
                        msg="Omitted payload text cannot leak into event control metadata.")
    else:
        check.equal(starts[0]["body"], {"arguments": arguments}, msg="Content capture retains the actual tool argument body.")


def test_selective_retention_preserves_stdio_contract(
        recording, evidence_path, monkeypatch, run_sdk_async):
    """One genuine SDK1 session proves policy routing without a second fault matrix."""
    recorder, path = recording
    marker = "session-policy-private café"
    params = types.PaginatedRequestParams(cursor=None, _meta={"page-request": 1})
    arguments = [{"text": marker, "case": case} for case in ("return", "tool-error", "protocol-error")]
    timeout, meta = timedelta(seconds=3), {"owner": "native-meta"}
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
        async with native_session(evidence_path, paged=True) as native:
            listings, listing_outcomes = spy_method(monkeypatch, native, "list_tools")
            calls, outcomes = spy_method(monkeypatch, native, "call_tool")
            observer = wrap_session(recorder, native, client_id="legacy-policy", server_key="peer",
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
            check.is_(listing, listing_outcomes[0], msg="Discovery omission preserves the exact SDK1-created page.")
            check.equal(([tool.name for tool in listing.tools], listing.nextCursor),
                        (["echo", "refuse", "wait"], "page-two"),
                        msg="The caller still receives the real ordered page and continuation token.")
            check.equal((len(listings), listings[0][0]), (1, (None,)),
                        msg="One typed listing retains the SDK1 cursor default without fetching another page.")
            check.is_(listings[0][1]["params"], params, msg="The original typed pagination object reaches native listing.")
            for index, submitted in enumerate(arguments):
                pending = observer.call_tool("echo", submitted, timeout, progress, meta=meta, evidence=evidences[index + 1])
                if index == 2:
                    with pytest.raises(McpError) as caught:
                        await pending
                    check.is_(caught.value, outcomes[index], msg="Protocol-exception redaction preserves native exception identity.")
                    check.equal(caught.value.error.code, -32602, msg="The real peer supplies the SDK1 protocol error being redacted.")
                else:
                    returned = await pending
                    check.is_(returned, outcomes[index], msg="Normal and tool-error redaction preserve native result identity.")
                    check.equal((returned.content[0].text, returned.isError), (marker, index == 1),
                                msg="Capture redaction changes neither original result text nor the SDK1 tool-error flag.")
                args, options = calls[index]
                check.equal(args[0], "echo", msg="The policy does not rewrite the tool name.")
                check.is_(args[1], submitted, msg="Native code receives the host's original argument object.")
                check.is_(args[2], timeout, msg="SDK1 receives the original timedelta, not a current-SDK timeout conversion.")
                check.is_(args[3], progress, msg="The native progress callback remains the host's exact callable.")
                check.is_(options["meta"], meta, msg="Native metadata is forwarded by identity.")
                check.equal(set(options), {"meta"}, msg="Retention does not leak recorder parameters or current-SDK options.")
            check.equal((len(listings), len(calls), len(outcomes)), (1, 3, 3),
                        msg="Listing plus three terminal categories cause exactly four native invocations.")

    run_sdk_async(scenario())
    facts = finish(recorder, path)
    check.equal(rows(path, "SELECT close_state FROM runs"), [{"close_state": "clean"}],
                msg="Declared omission/redaction preserves complete operation capture.")
    check.equal((never.start, never.terminal, never.losses), (None, None, ()),
                msg="Never-awaited work fabricates neither a receipt nor an evidence-loss outcome.")
    check.equal(progress_events, [[1.0, 2.0, "entered ✓"]] * 2,
                msg="SDK1 delivers both real result notifications; the protocol-error peer emits none.")
    check.equal([hook[0] for hook in hooks], ["mcp.request", "mcp.response"] * 4,
                msg="Each exposed session request and terminal invokes the policy once.")
    check.equal([hook[2] for hook in hooks],
                [{"client_id": "legacy-policy", "server_key": "peer", "method": "tools/list", "name": None}] * 2 +
                [{"client_id": "legacy-policy", "server_key": "peer", "method": "tools/call", "name": "echo"}] * 6,
                msg="Each policy call has the exact invocation-local four-field context.")
    check.equal(hooks[0][1], {"arguments": {"cursor": None, "params": {"cursor": None, "_meta": {"page-request": 1}}}},
                msg="Policy receives the full SDK1 cursor/typed-params envelope with explicit null and aliases.")
    check.equal([hooks[index][1] for index in (2, 4, 6)], [{"arguments": value} for value in arguments],
                msg="Tool policies receive original tool arguments without timeout, progress or native metadata options.")
    check.is_in("native protocol explanation café", hooks[-1][1]["error"]["message"],
                msg="The genuine SDK1 exception body reaches the policy before replacement.")
    ledger = [entry for entry in wire_log(evidence_path) if entry["method"] in ("tools/list", "tools/call")]
    check.equal([entry["method"] for entry in ledger], ["tools/list"] + ["tools/call"] * 3,
                msg="The independent wire ledger proves no hidden page, retry or abandoned call ran.")
    check.equal((ledger[0]["params"].get("cursor"), ledger[0]["params"]["_meta"]), (None, {"page-request": 1}),
                msg="The service receives the original pagination cursor and metadata.")
    check.equal([entry["params"]["arguments"] for entry in ledger[1:]], arguments,
                msg="All tool outcomes originate from the unredacted values on the real wire.")
    observed, operations = facts["events"], facts["operations"]
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
                msg="Discovery request and response have exact declared omission metadata and no body.")
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
