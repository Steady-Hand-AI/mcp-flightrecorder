"""Connected installed SDK paths and explicit submitted-history provenance."""
import asyncio
from contextlib import closing
from copy import deepcopy
from functools import wraps
import json
from pathlib import Path
import sqlite3

import httpx2
from mcp import Client
from mcp.server import Server
import mcp_types as types
from openai import AsyncOpenAI
import pytest
from pytest_check import check

from mcp_flightrecorder import Recorder
from mcp_flightrecorder.integrations.mcp_client import OperationEvidence, wrap_mcp
from mcp_flightrecorder.integrations.openai_chat import ModelEvidence, wrap_model


ROOT = Path(__file__).resolve().parents[1]
COVERAGE = {"producers": {"model": "history.model", "host": "history.host", "mcp": "history.mcp"},
            "model_complete": True}


def fixture_json(name):
    return json.loads((ROOT / "tests/fixtures/integration" / name).read_text())


def provider(responses, requests):
    """Only the HTTP backend is substituted; the actual SDK parses every reply."""
    async def handler(request):
        requests.append(json.loads(request.content))
        return httpx2.Response(200, json=responses[len(requests) - 1])

    return AsyncOpenAI(api_key="fixture-only-not-a-secret", base_url="https://fixture.invalid/v1",
                       max_retries=0, http_client=httpx2.AsyncClient(
                           transport=httpx2.MockTransport(handler), trust_env=False))


def echo_server(calls):
    async def list_tools(ctx, params):
        # The SDK may perform its own schema lookup while validating a result.
        # No additional high-level wrapper invocation is manufactured for it.
        return types.ListToolsResult(tools=[types.Tool.model_validate(fixture_json("mcp-tool.json"))])

    async def call_tool(ctx, params):
        calls.append({"name": params.name, "arguments": deepcopy(params.arguments)})
        return types.CallToolResult(content=[types.TextContent(type="text", text=params.arguments["text"])],
                                    is_error=False)

    return Server("history-server", on_list_tools=list_tools, on_call_tool=call_tool)


def binding(decision, result=None, pointer="/content", text_paths=("",)):
    return {"decision_event_id": decision.event_id, "pointer": pointer,
            "result_event_id": None if result is None else result.event_id,
            "text_paths": list(text_paths)}


def artifact(db, event_id):
    row = db.execute("SELECT a.body_text FROM events e JOIN artifacts a ON a.artifact_id=e.payload_ref "
                     "AND a.run_id=e.run_id WHERE e.event_id=?", (event_id,)).fetchone()
    assert row is not None, "An accepted content observation must have its own retained artifact"
    return None if row[0] is None else json.loads(row[0])


@pytest.mark.parametrize("capture", ["content", "metadata"])
def test_alpha_connected_path_and_original_tokens(tmp_path, run_sdk_async, capture):
    """B1 exact alpha slice; metadata repeats it to test B7's retained tokens."""
    messages = fixture_json("messages.json")
    alpha = deepcopy(messages["A"])
    alpha["tool_calls"] = alpha["tool_calls"][:1]
    normal = fixture_json("provider-normal.json")["W1"]
    choice = deepcopy(normal)
    choice["choices"] = [{"index": 0, "message": alpha, "finish_reason": "tool_calls"}]
    tools = fixture_json("provider-tools.json")
    path = tmp_path / "alpha.sqlite"
    rec = Recorder(path, capture=capture, coverage=COVERAGE)
    requests, calls, returned_models, returned_tools = [], [], [], []
    first, final, operation = ModelEvidence(), ModelEvidence(), OperationEvidence()

    async def host():
        async with provider([choice, normal], requests) as model, Client(echo_server(calls)) as client:
            native_create = model.chat.completions.create
            native_tool = client.call_tool

            @wraps(native_create)
            async def model_spy(**kwargs):
                value = await native_create(**kwargs)
                returned_models.append(value)
                return value

            @wraps(native_tool)
            async def tool_spy(*args, **kwargs):
                value = await native_tool(*args, **kwargs)
                returned_tools.append(value)
                return value

            client.call_tool = tool_spy
            create = wrap_model(rec, model_spy, instance_id="history-openai")
            mcp = wrap_mcp(rec, client, client_id="history-client", server_key="history-server")
            with rec.turn(messages["U"], conversation_id="alpha") as turn:
                answer = await create(model="fixture-model", messages=[messages["SYS"], messages["U"]],
                                      tools=tools, temperature=0, n=1, stream=False, evidence=first)
                check.is_(answer, returned_models[-1], msg="The wrapper returns the exact native normal ChatCompletion")
                assert len(first.decisions) == 1, "The alpha slice exposes exactly one accepted decision before host dispatch"
                decision = first.decisions[0].receipt
                assert decision.status == "accepted", "Host binding requires the admitted recorder decision occurrence"
                check.equal(answer.choices[0].message.tool_calls[0].function.arguments, '{"text":"alpha-selected"}',
                            msg="The native SDK's exposed raw argument string is not reserialized by capture")
                with rec.dispatch(action="execute", name="echo", arguments={"text": "alpha-executed"},
                                  decision_event_ids=[decision.event_id]) as dispatch:
                    result = await mcp.call_tool("echo", {"text": "alpha-executed"},
                                                 decision=decision, evidence=operation)
                check.is_(result, returned_tools[-1], msg="The wrapper returns the exact native MCP result object")
                assert operation.terminal is not None, "Explicit result binding requires the actual terminal event receipt"
                annotations = [{"bindings": []} for _ in range(3)] + [
                    {"bindings": [binding(decision, operation.terminal)]}]
                followup = [messages["SYS"], messages["U"], alpha, messages["B1"]]
                answer = await create(model="fixture-model", messages=followup, tools=tools,
                                      temperature=0, n=1, stream=False, annotations=annotations, evidence=final)
                check.is_(answer, returned_models[-1], msg="Bound follow-up evidence preserves the second native response identity")
                check.equal(answer.choices[0].message.content, "Both results recorded.",
                            msg="The connected alpha slice reaches the assigned normal final answer")
            return turn, dispatch, decision, followup

    try:
        turn, dispatch, decision, followup = run_sdk_async(host())
    finally:
        closed = rec.close()
    assert closed.writer_stopped and closed.portable, "The alpha slice must produce a closed portable SQLite file"
    check.equal(closed.run_close_state, "clean", msg="Complete explicit alpha evidence closes cleanly under either retention policy")
    check.equal(calls, [{"name": "echo", "arguments": {"text": "alpha-executed"}}],
                msg="The actual MCP backend sees exactly the rewritten host argument once")
    expected_requests = [{"model": "fixture-model", "messages": value, "tools": tools,
                          "temperature": 0, "n": 1, "stream": False}
                         for value in ([messages["SYS"], messages["U"]], followup)]
    check.equal(requests, expected_requests, msg="Recorder-only annotations and evidence never enter submitted SDK JSON")
    check.not_equal(decision.event_id, decision.decision_id, msg="Decision event IDs and decision occurrence IDs remain distinct categories")
    check.equal(operation.start.decision_id, decision.decision_id, msg="The operation propagates the explicit occurrence ID, not its event ID")
    check.equal(operation.start.turn_id, turn.receipt.turn_id, msg="The logical operation belongs to the actual host turn")
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        db.row_factory = sqlite3.Row
        trace = [row[0] for row in db.execute("SELECT kind FROM events ORDER BY sequence")]
        check.equal(trace, ["model.request", "model.tool_call", "model.response", "host.dispatch", "mcp.request",
                            "mcp.response", "model.request", "model.response"], msg="The alpha slice has the assigned eight-event order")
        counts = {name: db.execute("SELECT count(*) FROM " + name).fetchone()[0]
                  for name in ("turns", "events", "operations", "links", "request_members", "answer_occurrences", "findings")}
        check.equal(counts, dict(turns=1, events=8, operations=1, links=2, request_members=6, answer_occurrences=1, findings=0),
                    msg="No discovery or prepared-summary events enter the smaller B1 slice")
        check.equal(dict(db.execute("SELECT relation,count(*) FROM links GROUP BY relation").fetchall()),
                    {"answers": 1, "dispatch_source": 1}, msg="Only the explicit dispatch and actual submitted answer create links")
        row = db.execute((ROOT / "sql/q2.sql").read_text(), {"run_id": rec.capture_scope().run_id}).fetchone()
        assert row is not None, "Q2 must contain the connected model-to-operation association"
        check.equal(row["forwarded_changed"], 1, msg="Original HMAC arguments prove the alpha rewrite before retention")
        check.equal(row["dispatch_event_id"], dispatch.receipt.event_id, msg="Q2 names the actual host dispatch witness")
        check.equal(row["resolution"], "unknown", msg="The high-level MCP boundary does not infer remote or cache source")
        answers = json.loads(row["answers_json"])
        assert len(answers) == 1, "The alpha slice has one explicitly bound submitted result occurrence"
        check.equal((answers[0]["pointer"], answers[0]["text_equal"]), ("/messages/3/content", 1),
                    msg="The result is bound at its exact submitted destination and compares equal before retention")
        host_digest, mcp_digest = (json.loads(row[name]) for name in ("host_args_digest_json", "mcp_args_digest_json"))
        check.equal(host_digest, mcp_digest, msg="The original host and MCP argument projections agree under the same run key")
        if capture == "content":
            check.equal(artifact(db, decision.event_id), {"arguments_raw": '{"text":"alpha-selected"}'},
                        msg="Model evidence retains exposed raw arguments without inventing parsed arguments")
            check.equal(json.loads(row["forwarded_arguments"]), {"text": "alpha-executed"},
                        msg="Content retention preserves the independently observed forwarded argument")
            check.equal(row["forwarded_arguments"], row["mcp_arguments"], msg="Host and MCP retained argument objects agree")
        else:
            bodies = [row[0] for row in db.execute("SELECT body_text FROM artifacts WHERE representation<>'request_refs/2'")]
            check.is_true(all(value is None for value in bodies), msg="Metadata policy omits all nonstructural artifact bodies")
            control = json.dumps([tuple(row) for row in db.execute("SELECT details_json FROM events")])
            check.is_not_in("alpha-selected", control, msg="Omitted model argument text never leaks into event control metadata")
            check.is_not_in("alpha-executed", control, msg="Omitted result and forwarded body text never leaks into control metadata")


def test_reused_provider_ids_unknown_history_duplicates_blocks_and_compaction(tmp_path, run_sdk_async):
    normal = fixture_json("provider-normal.json")["W1"]
    selected = deepcopy(normal)
    selected["choices"] = [{"index": 0, "message": {"role": "assistant", "content": None,
                            "tool_calls": [{"id": "reused-id", "type": "function", "function": {
                                "name": "echo", "arguments": '{"text":"same"}'}}]}, "finish_reason": "tool_calls"}]
    requests, calls = [], {}
    path = tmp_path / "history.sqlite"
    rec = Recorder(path, capture="content", coverage=COVERAGE)
    system = {"role": "system", "content": "Keep explicit evidence."}
    tool = {"role": "tool", "tool_call_id": "reused-id", "content": "same"}

    async def host():
        async with provider([selected, selected] + [normal] * 6, requests) as client:
            create = wrap_model(rec, client.chat.completions.create, instance_id="history-openai")
            async def submit(label, messages, annotations=None):
                evidence = ModelEvidence()
                await create(model="fixture-model", messages=messages, annotations=annotations, evidence=evidence)
                calls[label] = evidence
                return evidence

            with rec.turn("history"):
                one = await submit("first", [{"role": "user", "content": "select"}])
                two = await submit("second", [{"role": "user", "content": "select again"}])
                assert len(one.decisions) == len(two.decisions) == 1, "Both actual SDK calls expose one call-local decision receipt"
                d1, d2 = one.decisions[0].receipt, two.decisions[0].receipt
                await submit("unknown", [system, tool])
                annotation = {"bindings": [binding(d1)]}
                await submit("duplicates", [system, tool, tool], [{"bindings": []}, annotation, annotation])
                await submit("shifted", [{"role": "user", "content": "prefix"}, system, tool],
                             [{"bindings": []}, {"bindings": []}, annotation])
                await submit("other_decision", [tool], [{"bindings": [binding(d2)]}])
                await submit("compacted", [{"role": "user", "content": "earlier tools removed"}])
                blocks = {"role": "tool", "tool_call_id": "reused-id", "content": [
                    {"type": "text", "text": "left"}, {"type": "text", "text": "right"}]}
                await submit("blocks", [blocks], [{"bindings": [
                    binding(d1, pointer="/content/0", text_paths=("/text",)),
                    binding(d1, pointer="/content/1", text_paths=("/text",))]}])
                return d1, d2

    try:
        d1, d2 = run_sdk_async(host())
    finally:
        closed = rec.close()
    assert closed.portable, "The explicit-history case must close before independent SQLite inspection"
    check.not_equal(d1.decision_id, d2.decision_id, msg="Reused provider IDs cannot merge two recorder decision occurrences")
    check.equal([calls[name].decisions[0].provider_ref for name in ("first", "second")], ["reused-id", "reused-id"],
                msg="Both reused provider references remain separately observed evidence")
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        db.row_factory = sqlite3.Row
        members = {label: [dict(row) for row in db.execute("SELECT * FROM request_members WHERE request_event_id=? ORDER BY message_index",
                                                          (value.request.event_id,))] for label, value in calls.items()}
        known = members["duplicates"][1]["message_artifact_id"]
        check.equal(members["duplicates"][2]["message_artifact_id"], known, msg="Duplicate explicitly bound messages retain both ordered memberships")
        check.equal(members["shifted"][2]["message_artifact_id"], known, msg="Shifting a bound message reuses identity without fixing its old index")
        check.not_equal(members["unknown"][1]["message_artifact_id"], known, msg="An earlier unannotated result never becomes retroactively resolved")
        check.not_equal(members["other_decision"][0]["message_artifact_id"], known, msg="Same provider ID and body with a different explicit decision has distinct identity")
        expected = {"unknown": [], "duplicates": ["/messages/1/content", "/messages/2/content"],
                    "shifted": ["/messages/2/content"], "other_decision": ["/messages/0/content"],
                    "compacted": [], "blocks": ["/messages/0/content/0", "/messages/0/content/1"]}
        for label, pointers in expected.items():
            rows = db.execute("SELECT * FROM answer_occurrences WHERE request_event_id=? ORDER BY target_pointer",
                              (calls[label].request.event_id,)).fetchall()
            check.equal([row["target_pointer"] for row in rows], pointers,
                        msg=label + " preserves the exact submitted occurrence locations without inferred answers")
            expected_source = d2.event_id if label == "other_decision" else d1.event_id
            check.is_true(all(row["source_event_id"] == expected_source for row in rows),
                          msg=label + " associates only the recorder receipt explicitly named by the host")
            if label == "blocks":
                details = [json.loads(row["details_json"]) for row in rows]
                check.equal([value.get("result_event_id") for value in details], [None, None],
                            msg="Two bound text blocks do not fabricate an MCP result source")
                check.is_true(all(value.get("submitted_text_digest", {}).get("basis") == "observed_original" for value in details),
                              msg="Each known text block names the original submitted observation as its comparison basis")
                check.equal([value.get("submitted_text_digest", {}).get("projection") for value in details],
                            ["text_blocks_json/1", "text_blocks_json/1"],
                            msg="Both explicit /text paths use the ordered text-block projection rather than an unrelated comparison token")
                check.is_true(all(value.get("submitted_text_digest", {}).get("value") is not None for value in details),
                              msg="Each explicit text block has an available original token; a named basis alone would also permit unavailable evidence")
        unknown = db.execute("SELECT details_json FROM events WHERE event_id=?", (calls["unknown"].request.event_id,)).fetchone()
        check.is_in("binding_not_supplied", unknown[0], msg="The unannotated provider reference remains an explicit unresolved diagnostic")
        q2 = [dict(row) for row in db.execute((ROOT / "sql/q2.sql").read_text(), {"run_id": rec.capture_scope().run_id})]
        check.equal(len(q2), 2, msg="Reused provider references still produce two independent decision rows")
        check.is_true(all(item["text_equal"] is None for row in q2 for item in json.loads(row["answers_json"])),
                      msg="Binding a submitted block without an observed result leaves result comparison unavailable")


def test_reused_provider_id_keeps_two_same_name_concurrent_operations_distinct(tmp_path, run_sdk_async):
    """B7 beyond the canonical fixture: equal names/arguments and reused provider IDs."""
    selected = deepcopy(fixture_json("provider-normal.json")["W1"])
    selected["choices"] = [{"index": 0, "message": {"role": "assistant", "content": None,
        "tool_calls": [{"id": "reused-id", "type": "function", "function": {
            "name": "echo", "arguments": '{"text":"same"}'}}]}, "finish_reason": "tool_calls"}]
    path = tmp_path / "concurrent-history.sqlite"
    rec = Recorder(path, capture="content", coverage=COVERAGE)
    requests, backend_calls, transitions, native_results = [], [], [], []
    models = [ModelEvidence(), ModelEvidence()]
    operations = [OperationEvidence(), OperationEvidence()]

    async def host():
        entered = [asyncio.Event(), asyncio.Event()]
        release = [asyncio.Event(), asyncio.Event()]

        async def list_tools(context, params):
            return types.ListToolsResult(tools=[types.Tool.model_validate(fixture_json("mcp-tool.json"))])

        async def call_tool(context, params):
            index = len(backend_calls)
            assert index < 2, "Only the two explicit host calls may reach this controlled SDK peer."
            backend_calls.append({"name": params.name, "arguments": deepcopy(params.arguments)})
            transitions.append("enter-" + str(index))
            entered[index].set()
            await asyncio.wait_for(release[index].wait(), timeout=3)
            transitions.append("return-" + str(index))
            return types.CallToolResult(content=[types.TextContent(type="text", text="result-" + str(index))],
                                        is_error=False)

        server = Server("concurrent-history-server", on_list_tools=list_tools, on_call_tool=call_tool)
        async with provider([selected, selected], requests) as model, Client(server) as client:
            native_call = client.call_tool

            @wraps(native_call)
            async def native_spy(*args, **kwargs):
                value = await native_call(*args, **kwargs)
                native_results.append(value)
                return value

            client.call_tool = native_spy
            create = wrap_model(rec, model.chat.completions.create, instance_id="concurrent-model")
            mcp = wrap_mcp(rec, client, client_id="concurrent-client", server_key="concurrent-server")
            tasks, dispatches = [], []
            with rec.turn("same-name concurrent work") as turn:
                for index, evidence in enumerate(models):
                    await create(model="fixture-model", messages=[{"role": "user", "content": "select-" + str(index)}],
                                 evidence=evidence)
                    assert evidence.request is not None and evidence.request.status == "accepted", "Exact concurrent attribution requires each admitted model request occurrence."
                    assert evidence.response is not None and evidence.response.status == "accepted", "Each complete real model response must be admitted before its explicit decision is dispatched."
                    assert len(evidence.decisions) == 1, "Each real model call must expose exactly one call-local decision before dispatch."
                    assert evidence.decisions[0].receipt.status == "accepted", "Concurrent attribution requires both original decision occurrences to be admitted."
                decisions = [evidence.decisions[0].receipt for evidence in models]
                try:
                    for index, decision in enumerate(decisions):
                        with rec.dispatch(action="execute", name="echo", arguments={"text": "same"},
                                          decision_event_ids=[decision.event_id]) as dispatch:
                            assert dispatch.receipt.status == "accepted", "Each independent host action needs its own accepted dispatch event."
                            dispatches.append(dispatch.receipt)
                            tasks.append(asyncio.create_task(mcp.call_tool("echo", {"text": "same"},
                                                        decision=decision, evidence=operations[index])))
                            await asyncio.wait_for(entered[index].wait(), timeout=3)
                            assert operations[index].start is not None and operations[index].start.status == "accepted", (
                                "The actual peer entry must be preceded by the matching admitted operation start."
                            )
                    check.equal(transitions, ["enter-0", "enter-1"],
                                msg="Both same-name native operations enter independently before either result is released.")
                    check.is_true(all(not gate.is_set() for gate in release) and all(not task.done() for task in tasks),
                                  msg="The two real peer gates remain held during the disputed concurrent-attribution check.")
                    check.equal(rec.health().open_operations, 2,
                                msg="Both accepted operations own distinct live completion reservations at the overlap boundary.")
                    check.is_none(rec.capture_scope().dispatch_event_id,
                                  msg="Leaving both lexical dispatch scopes must not finish or cancel either already submitted operation.")
                    release[1].set()
                    second = await asyncio.wait_for(tasks[1], timeout=3)
                    check.is_(second, native_results[0], msg="The second completion returns the exact native SDK result observed by the pass-through spy.")
                    check.is_false(tasks[0].done() or release[0].is_set(),
                                   msg="The first operation stays in flight while the second one completes; completion order cannot select identity.")
                    release[0].set()
                    first = await asyncio.wait_for(tasks[0], timeout=3)
                    check.is_(first, native_results[1], msg="The later first-call completion retains its own native result object.")
                    check.equal((first.content[0].text, second.content[0].text), ("result-0", "result-1"),
                                msg="Equal names and arguments cannot swap the host's two actual result destinations.")
                finally:
                    for gate in release:
                        gate.set()
                    for task in tasks:
                        if not task.done():
                            task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
            return turn.receipt, decisions, dispatches

    try:
        turn, decisions, dispatches = run_sdk_async(host())
    finally:
        closed = rec.close()
    assert closed.writer_stopped and closed.portable, "Concurrent history evidence must be committed and portable before independent read-only checks."
    check.equal(closed.run_close_state, "clean", msg="Complete independently attributed native operations leave no unfinished capture or reservation.")
    check.equal([item.decisions[0].provider_ref for item in models], ["reused-id", "reused-id"],
                msg="The test exposes the same provider reference from two actual model calls, requiring explicit recorder occurrences.")
    check.not_equal(decisions[0].decision_id, decisions[1].decision_id,
                    msg="Reusing the provider ID cannot merge decision occurrence IDs.")
    check.not_equal(models[0].request.model_call_id, models[1].request.model_call_id,
                    msg="Each native model request retains its separate recorder model-call identity.")
    check.equal(backend_calls, [{"name": "echo", "arguments": {"text": "same"}}] * 2,
                msg="Both actual native invocations have identical names and arguments, so equality cannot explain correct attribution.")
    check.equal(transitions, ["enter-0", "enter-1", "return-1", "return-0"],
                msg="Real peer Events prove overlap and reverse completion without sleeps or timing thresholds.")
    check.equal(len(requests), 2, msg="Capture cannot add a provider call while correlating the two explicit decisions.")
    for index, operation in enumerate(operations):
        assert operation.terminal is not None and operation.terminal.status == "accepted", "Each gated native completion must supply its own accepted terminal receipt."
        check.equal((operation.start.decision_id, operation.terminal.decision_id),
                    (decisions[index].decision_id, decisions[index].decision_id),
                    msg="Both lifecycle receipts retain the host-supplied occurrence instead of resolving the reused provider ID.")
        check.equal(operation.terminal.operation_id, operation.start.operation_id,
                    msg="Reverse completion cannot finish the other same-name operation's ticket.")
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        db.row_factory = sqlite3.Row
        rows = [dict(row) for row in db.execute("""SELECT o.operation_id,o.turn_id,o.model_call_id,o.decision_id,
            o.dispatch_event_id,o.link_basis,o.name,o.status,o.resolution,o.input_rounds,o.start_event_id,o.end_event_id
            FROM operations o JOIN events e ON e.event_id=o.start_event_id ORDER BY e.sequence""")]
        expected = [{"operation_id": operations[index].start.operation_id, "turn_id": turn.turn_id,
                     "model_call_id": models[index].request.model_call_id, "decision_id": decisions[index].decision_id,
                     "dispatch_event_id": dispatches[index].event_id, "link_basis": "host_hook", "name": "echo",
                     "status": "ok", "resolution": "unknown", "input_rounds": None,
                     "start_event_id": operations[index].start.event_id, "end_event_id": operations[index].terminal.event_id}
                    for index in range(2)]
        check.equal(rows, expected, msg="Persisted operation rows retain exact model, decision, dispatch, ticket and lifecycle IDs despite equal arguments and reversed completion.")
        expected_trace = []
        for index in range(2):
            expected_trace.extend([models[index].request.event_id, decisions[index].event_id, models[index].response.event_id])
        expected_trace.extend([dispatches[0].event_id, operations[0].start.event_id,
                               dispatches[1].event_id, operations[1].start.event_id,
                               operations[1].terminal.event_id, operations[0].terminal.event_id])
        check.equal([row[0] for row in db.execute("SELECT event_id FROM events ORDER BY sequence")], expected_trace,
                    msg="The independent receipt trace proves S1 before S2 before E2 before E1 with no synthesized discovery or duplicate terminal.")
        links = [tuple(row) for row in db.execute("SELECT relation,source_event_id,target_event_id,witness_event_id FROM links")]
        check.equal(set(links), {("dispatch_source", decisions[index].event_id,
                                 dispatches[index].event_id, dispatches[index].event_id) for index in range(2)},
                    msg="Each host action is witnessed by its explicit source receipt; reused provider IDs create no extra inferred relation.")
        check.equal([artifact(db, operation.terminal.event_id)["content"][0]["text"] for operation in operations],
                    ["result-0", "result-1"], msg="Persisted result bodies remain attached to the same explicit operation receipts the host received.")
        associations = [tuple(row) for row in db.execute("""SELECT decision_id,dispatch_event_id,operation_id FROM operations
            ORDER BY started_at,operation_id""")]
        check.equal(set(associations), {(decisions[index].decision_id, dispatches[index].event_id,
                                       operations[index].start.operation_id) for index in range(2)},
                    msg="The database's explicit joins preserve both independent decision-to-operation associations.")


def test_prepared_context_requires_matching_explicit_destination(tmp_path, run_sdk_async):
    path = tmp_path / "prepared.sqlite"
    rec = Recorder(path, capture="content", coverage=COVERAGE)
    requests, native_calls, evidence = [], [], {}
    normal = fixture_json("provider-normal.json")["W1"]
    async def host():
        async with provider([normal] * 4, requests) as model, Client(echo_server(native_calls)) as client:
            create = wrap_model(rec, model.chat.completions.create, instance_id="context-openai")
            mcp = wrap_mcp(rec, client, client_id="context-client")
            with rec.turn("context"):
                source_ids = []
                for text in ("left", "right"):
                    item = OperationEvidence()
                    await mcp.call_tool("echo", {"text": text}, evidence=item)
                    assert item.terminal is not None, "A prepared source must name the actual completed SDK observation"
                    source_ids.append(item.terminal.event_id)
                prepared = rec.observe("host.tool_result", scope=rec.capture_scope(), payload=True,
                                       facts={"sources": [{"event_id": value, "pointer": "/content/0/text"} for value in source_ids]})
                assert prepared.status == "accepted", "The explicit two-source preparation must be admitted before insertion controls"
                for label, content, insertion in (
                    ("prepared_only", True, []),
                    ("wrong_pointer", True, [{"source_event_id": prepared.event_id, "target_pointer": "/messages/4/content", "content": True}]),
                    ("typed_mismatch", 1, [{"source_event_id": prepared.event_id, "target_pointer": "/messages/0/content", "content": True}]),
                    ("matching", True, [{"source_event_id": prepared.event_id, "target_pointer": "/messages/0/content", "content": True}]),
                ):
                    item = ModelEvidence()
                    await create(model="fixture-model", messages=[{"role": "system", "content": content}],
                                 insertions=insertion, evidence=item)
                    evidence[label] = item
                return prepared, source_ids
    try:
        prepared, source_ids = run_sdk_async(host())
    finally:
        closed = rec.close()
    assert closed.portable, "Prepared-context evidence must be independently readable after closure"
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        rows = db.execute("SELECT source_event_id,target_event_id,witness_event_id FROM links WHERE relation='transformed_from'").fetchall()
        check.equal(set(rows), {(value, prepared.event_id, prepared.event_id) for value in source_ids},
                    msg="The host's two explicit preparation sources remain two witnessed relations")
        inserted = db.execute("SELECT source_event_id,target_event_id,target_pointer,witness_event_id FROM links WHERE relation='inserted_into'").fetchall()
        check.equal(inserted, [(prepared.event_id, evidence["matching"].request.event_id, "/messages/0/content", evidence["matching"].request.event_id)],
                    msg="Only the explicitly supplied matching destination inserts prepared material; bool and int do not compare equal")
        for label in ("wrong_pointer", "typed_mismatch"):
            details = db.execute("SELECT details_json FROM events WHERE event_id=?", (evidence[label].request.event_id,)).fetchone()[0]
            check.is_in("insertion", details, msg=label + " remains visible as a capture diagnostic without an invented insertion link")
