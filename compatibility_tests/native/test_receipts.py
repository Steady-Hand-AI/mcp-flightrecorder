"""C6/C7: explicit host-owned public receipts, separate from stock native host.

These deliberately labelled controls use core hooks for selection/operation
fixtures and the actual AnyLLM/Ollama facade for their submitted follow-up.
The host owns receipt variables; equal names, arguments, provider references
or Python object identities never choose an association.
"""
import asyncio
from dataclasses import asdict
import json

from any_llm import AnyLLM
import httpx
import pytest
from pytest_check import check

from mcp_flightrecorder import Recorder
from mcp_flightrecorder.integrations.anyllm_ollama import ModelEvidence, wrap_model

from .host_evidence import artifact_body, events, inspect_recording, operations, relations


COVERAGE = {"producers": {"model": "explicit-receipt-model-fixture",
                           "host": "explicit-receipt-host-fixture",
                           "mcp": "explicit-receipt-operation-fixture"}, "model_complete": True}


def admitted(receipt):
    assert receipt.status == "accepted", "The explicit-receipt control needs an admitted public receipt before using its occurrence identity."
    return receipt


def model_request(recorder, messages, annotations, *, insertions=()):
    payload = {"model": "explicit-host-control", "messages": messages}
    receipt = admitted(recorder.observe("model.request", scope=recorder.capture_scope(), payload=payload,
        facts={"message_path": "/messages", "messages": annotations, "insertions": list(insertions)}))
    return receipt, payload


def model_response(recorder, request, payload):
    return admitted(recorder.observe("model.response", scope=recorder.capture_scope(), payload=payload,
                                    facts={"model_call_id": request.model_call_id, "state": "complete"}))


def two_known_selections(recorder):
    request, submitted = model_request(recorder, [{"role": "user", "content": "select two equal calls"}],
                                       [{"bindings": [], "complete": True}])
    shared_arguments = {"x": 7}
    decisions = [admitted(recorder.observe("model.tool_call", scope=recorder.capture_scope(),
        payload={"arguments": shared_arguments}, facts={"name": "echo", "provider_ref": "reused-reference",
                                                       "model_call_id": request.model_call_id})) for _ in range(2)]
    response = model_response(recorder, request, {"fixture": "explicit two-selection control"})
    return request, submitted, decisions, response, shared_arguments


def binding(decision, result=None):
    return {"decision_event_id": decision.event_id, "pointer": "/content",
            "result_event_id": None if result is None else result.event_id, "text_paths": [""]}


async def native_followup(recorder, messages, annotations, *, insertions=()):
    """Submit explicit receipts through the new facade and real SDK conversion."""
    wire = []

    def transport(request):
        wire.append(json.loads(request.content))
        return httpx.Response(200, json={"model": "explicit-host-control",
            "created_at": "2026-10-22T00:00:00.000000Z", "done": True, "done_reason": "stop",
            "message": {"role": "assistant", "content": "Explicit answers received ✓"},
            "prompt_eval_count": 5, "eval_count": 3})

    provider = AnyLLM.create("ollama", api_base="http://fixture.invalid",
                             transport=httpx.MockTransport(transport))
    evidence = ModelEvidence()
    submitted = {"model": "explicit-host-control", "messages": messages, "stream": False}
    try:
        create = wrap_model(recorder, provider.acompletion, instance_id="explicit-receipt-followup")
        result = await create(**submitted, annotations=annotations, insertions=insertions, evidence=evidence)
        check.equal(result.choices[0].message.content, "Explicit answers received ✓",
                    msg="Explicit annotation/insertion composition preserves the actual native AnyLLM completion output.")
    finally:
        await provider.client._client.aclose()
    assert evidence.request is not None and evidence.response is not None, "The native follow-up must expose both recorder receipts before inspecting explicit answer associations."
    admitted(evidence.request)
    admitted(evidence.response)
    check.equal(evidence.losses, (),
                msg="Supported ordinary JSON plus explicit public receipts passes the AnyLLM facade without capture rejection.")
    check.equal(len(wire), 1,
                msg="The explicit-receipt follow-up invokes the real SDK transport exactly once.")
    check.is_true(all(key not in wire[0] for key in ("annotations", "insertions", "evidence")),
                  msg="Recorder-only receipts never enter native Ollama provider JSON.")
    return evidence.request, submitted


def closed_report(path, capture, close, submitted_requests):
    return {
        "capture": capture,
        "close": asdict(close),
        "submitted_requests": submitted_requests,
        **inspect_recording(path, capture=capture),
    }


def valid_explicit_recording(report, expected_state):
    check.equal(report["close"], {"writer_stopped": True, "run_close_state": expected_state,
                                  "portable": True, "reason": None},
                msg="Explicit receipts close with the truthful capture state and a portable database.")
    check.equal(report["database"]["integrity_check"], ["ok"],
                msg="The receipt-control recording passes SQLite integrity.")
    check.equal(report["database"]["foreign_key_check"], [],
                msg="All receipt-bound endpoints retain valid foreign keys.")
    check.equal(report["source_sha256_before"], report["source_sha256_after"],
                msg="Public inspection and snapshotting leave the explicit-receipt source bytes unchanged.")
    check.is_true(report["snapshot"]["rows_equal"],
                  msg="The installed snapshot command preserves every explicit receipt association on reopen.")
    check.equal(report["snapshot"]["database"]["integrity_check"], ["ok"],
                msg="The portable receipt-control copy also passes integrity.")
    assert report["check"]["ok"], "The public reader must successfully inspect explicit receipt evidence before evaluating its result."
    check.equal(report["check"]["data"]["required_failures"], [],
                msg="Known and explicitly unknown bindings produce no contradictory reader findings.")
    check.equal(report["tables"]["runs"][0]["dropped_events"], 0,
                msg="Explicit uncertainty is not disguised capture rejection or capacity loss.")
    if report["capture"] == "content":
        check.equal(report["reconstructed_requests"], report["submitted_requests"],
                    msg="Public payload reconstruction preserves both original submitted control requests.")
    else:
        check.is_true(all(row["body_text"] is None for row in report["tables"]["artifacts"]
                          if row["representation"] != "request_refs/2"),
                      msg="Metadata retention keeps identities while omitting all nonstructural control bodies.")


def submitted_answers(report, request_event_id):
    envelope = next(item for item in report["request_slices"]
                    if item["scope"].get("request_event_id") == request_event_id)
    assert envelope["ok"], "The public slice reader must resolve the submitted request before comparing its projected answer identities and text equality."
    return sorted([row for row in envelope["data"]["answers"] if row["request_event_id"] == request_event_id],
                  key=lambda row: row["message_index"])


@pytest.mark.parametrize("capture", ["content", "metadata"])
def test_explicit_receipts_preserve_overlapping_equal_calls_in_reverse_completion(evidence_path, run_sdk_async, capture):
    path = evidence_path / "recording.sqlite"
    recorder = Recorder(path, capture=capture, coverage=COVERAGE,
                        environment={"fixture_kind": "explicit_host_receipt_control",
                                     "selection_and_operation": "public-core-hook fixture",
                                     "followup": "installed AnyLLM/Ollama conversion"})
    transitions, calls, starts, terminals, dispatches, scopes = [], [], [None, None], [None, None], [], []

    async def host():
        entered = [asyncio.Event(), asyncio.Event()]
        release = [asyncio.Event(), asyncio.Event()]
        tasks = []
        with recorder.turn("Equal values require explicit occurrence receipts") as turn:
            first, submitted, decisions, response, arguments = two_known_selections(recorder)

            async def execute(index):
                starts[index] = admitted(recorder.start_operation(scope=scopes[index],
                    client_id="receipt-host", server_key="controlled", method="tools/call", name="echo",
                    arguments=arguments, decision_id=decisions[index].decision_id,
                    dispatch_event_id=dispatches[index].event_id, link_basis="host_hook"))
                calls.append({"name": "echo", "arguments": dict(arguments)})
                transitions.append("enter-" + str(index))
                entered[index].set()
                await asyncio.wait_for(release[index].wait(), timeout=5)
                result = {"content": [{"type": "text", "text": "result-" + str(index)}]}
                terminals[index] = admitted(recorder.finish_operation(starts[index].ticket, status="ok",
                    result=result, text_paths=["/content/0/text"], resolution="unknown"))
                transitions.append("return-" + str(index))
                return result

            try:
                for index, decision in enumerate(decisions):
                    with recorder.dispatch(action="execute", name="echo", arguments=arguments,
                                           decision_event_ids=[decision.event_id]) as dispatch:
                        dispatches.append(admitted(dispatch.receipt))
                        scopes.append(recorder.capture_scope())
                        tasks.append(asyncio.create_task(execute(index)))
                        await asyncio.wait_for(entered[index].wait(), timeout=5)
                check.equal(transitions, ["enter-0", "enter-1"],
                            msg="Both controlled operations actually enter before either completion gate is released.")
                check.is_true(all(not task.done() for task in tasks),
                              msg="Both operation coroutines remain live together; two calls alone would not prove overlap.")
                check.equal(recorder.health().open_operations, 2,
                            msg="Each overlapped explicit operation owns its distinct live completion reservation.")
                release[1].set()
                second = await asyncio.wait_for(tasks[1], timeout=5)
                check.is_false(tasks[0].done(),
                               msg="The first operation remains in flight after the second completes, proving reversed completion.")
                release[0].set()
                first_result = await asyncio.wait_for(tasks[0], timeout=5)
            finally:
                for gate in release:
                    gate.set()
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
            messages = [{"role": "tool", "tool_call_id": "reused-reference", "content": result["content"][0]["text"]}
                        for result in (first_result, second)]
            annotations = [{"bindings": [binding(decisions[index], terminals[index])], "complete": True}
                           for index in range(2)]
            followup, submitted_followup = await native_followup(recorder, messages, annotations)
        return turn.receipt, first, decisions, [submitted, submitted_followup], followup

    try:
        turn, first, decisions, submitted, followup = run_sdk_async(host())
    finally:
        close = recorder.close()
    report = closed_report(path, capture, close, submitted)
    valid_explicit_recording(report, "clean")
    check.equal(calls, [{"name": "echo", "arguments": {"x": 7}}] * 2,
                msg="Both actual control invocations expose equal names and arguments, so values cannot identify the receipt association.")
    check.equal(transitions, ["enter-0", "enter-1", "return-1", "return-0"],
                msg="Independent Event gates establish overlap and reverse completion without sleeps or timing thresholds.")
    check.equal([json.loads(row["details_json"])["provider_ref"] for row in events(report, "model.tool_call")],
                ["reused-reference"] * 2,
                msg="Reusing a provider reference leaves two distinct observed selections.")
    check.not_equal(decisions[0].decision_id, decisions[1].decision_id,
                    msg="The core allocates separate occurrence IDs even when every model-facing selection value is equal.")
    expected = {(starts[index].operation_id, decisions[index].decision_id, dispatches[index].event_id,
                 first.model_call_id, turn.turn_id, starts[index].event_id, terminals[index].event_id)
                for index in range(2)}
    actual = {(row["operation_id"], row["decision_id"], row["dispatch_event_id"], row["model_call_id"],
               row["turn_id"], row["start_event_id"], row["end_event_id"]) for row in operations(report)}
    check.equal(actual, expected,
                msg="Each persisted lifecycle retains exactly the host-owned selection, dispatch and terminal receipt despite reverse completion.")
    answers = submitted_answers(report, followup.event_id)
    check.equal([(row["source_event_id"], row["result_event_id"], row["text_equal"]) for row in answers],
                [(decisions[index].event_id, terminals[index].event_id, 1) for index in range(2)],
                msg="Both submitted tool blocks retain their explicitly supplied decision/result receipts and equal-text witnesses.")
    check.equal(len(relations(report, "dispatch_source")), 2,
                msg="Only the two supplied decision-event receipts produce dispatch-source links.")
    if capture == "content":
        terminal_events = {row["event_id"]: row for row in events(report, "mcp.response")}
        check.equal([artifact_body(report, terminal_events[item.event_id]["payload_ref"])["content"][0]["text"]
                     for item in terminals], ["result-0", "result-1"],
                    msg="Stored result bodies stay attached to their own operation tickets rather than completion order.")


@pytest.mark.parametrize("capture", ["content", "metadata"])
@pytest.mark.parametrize("witness", ["ambiguous", "exact_first"])
def test_explicit_empty_alias_bindings_preserve_known_answers_and_unknown_origins(evidence_path, run_sdk_async, capture, witness):
    path = evidence_path / "recording.sqlite"
    recorder = Recorder(path, capture=capture, coverage=COVERAGE,
                        environment={"fixture_kind": "explicit_host_receipt_control", "result_witness": witness,
                                     "selection_and_operation": "public-core-hook fixture",
                                     "followup": "installed AnyLLM/Ollama conversion"})
    terminals, prepared = [], []
    empty = ""
    aliased_results = [empty, empty]
    try:
        with recorder.turn("Two known selections, aliased empty result objects"):
            initial, submitted, decisions, response, arguments = two_known_selections(recorder)
            for index, decision in enumerate(decisions):
                with recorder.dispatch(action="execute", name="echo", arguments=arguments,
                                       decision_event_ids=[decision.event_id]):
                    start = admitted(recorder.start_operation(scope=recorder.capture_scope(), client_id="receipt-host",
                        server_key="controlled", method="tools/call", name="echo", arguments=arguments,
                        decision_id=decision.decision_id, link_basis="host_hook"))
                    terminal = admitted(recorder.finish_operation(start.ticket, status="ok",
                        result={"content": [{"type": "text", "text": aliased_results[index]}]},
                        text_paths=["/content/0/text"], resolution="unknown"))
                    terminals.append(terminal)
                prepared.append(admitted(recorder.observe("host.tool_result", scope=recorder.capture_scope(),
                    payload=aliased_results[index], facts={"sources": [{"event_id": terminal.event_id}],
                                                          "text_paths": [""]})))
            messages = [{"role": "tool", "tool_call_id": "reused-reference", "content": value}
                        for value in aliased_results]
            annotations = []
            for index, decision in enumerate(decisions):
                exact = witness == "exact_first" and index == 0
                annotations.append({"bindings": [binding(decision, terminals[index] if exact else None)],
                    "unresolved": [] if exact else [{"pointer": "/content",
                                                     "reason": "aliased_result_receipt_not_supplied"}],
                    "complete": True})
            insertions = ([{"source_event_id": prepared[0].event_id, "target_pointer": "/messages/0/content",
                            "content": aliased_results[0]}] if witness == "exact_first" else [])
            followup, submitted_followup = run_sdk_async(native_followup(recorder, messages, annotations,
                                                                        insertions=insertions))
    finally:
        close = recorder.close()
    report = closed_report(path, capture, close, [submitted, submitted_followup])
    valid_explicit_recording(report, "incomplete")
    check.is_true(aliased_results[0] is aliased_results[1],
                  msg="The control really reuses the same empty-string object, so object identity cannot choose its source operation.")
    answers = submitted_answers(report, followup.event_id)
    check.equal(len(answers), 2,
                msg="Both host-owned decision-to-answer receipts survive even when the result origin is ambiguous.")
    check.equal([row["source_event_id"] for row in answers], [value.event_id for value in decisions],
                msg="The two known answer bindings use exact recorder selection receipts, never the reused provider reference.")
    expected_origins = [terminals[0].event_id, None] if witness == "exact_first" else [None, None]
    check.equal([row["result_event_id"] for row in answers], expected_origins,
                msg="Only the explicit positive result receipt supplies an origin; aliased empty contents cannot fill another origin.")
    check.equal([row["text_equal"] for row in answers], [1, None] if witness == "exact_first" else [None, None],
                msg="Empty-text equality is conclusive only for the supplied result witness; matching bytes cannot infer one.")
    insertions = relations(report, "inserted_into")
    check.equal(len(insertions), 1 if witness == "exact_first" else 0,
                msg="The ambiguous arm produces no insertion; its positive neighbor produces exactly the one explicitly witnessed destination.")
    if witness == "exact_first":
        check.equal(insertions[0]["source_event_id"], prepared[0].event_id,
                    msg="The positive insertion names only the prepared receipt supplied by the host, despite the other equal empty value.")
    check.equal(len(relations(report, "transformed_from")), 2,
                msg="Two original result transformations remain known independently of later aliased submission origins.")
    check.is_in("answer_bindings", report["check"]["data"]["required_unknowns"],
                msg="The public reader still reports the remaining source ambiguity rather than treating two known decisions as full provenance.")
    if capture == "metadata":
        details = json.dumps([row["details_json"] for row in report["tables"]["events"]])
        check.is_not_in("select two equal calls", details,
                        msg="Metadata mode does not hide omitted model input in the explicit binding diagnostic channel.")
