"""AnyLLM/Ollama conversion contracts and labelled boundary controls."""
import asyncio
import hashlib
import inspect as python_inspect
import json

from any_llm.exceptions import AnyLLMError
from any_llm.types.completion import ChatCompletionMessage
from pydantic import BaseModel
import pytest
import pytest_check as check

from mcp_flightrecorder import Recorder
from mcp_flightrecorder.integrations import anyllm_ollama, openai_chat
from mcp_flightrecorder.integrations.anyllm_ollama import ModelEvidence, wrap_model

from .model_support import (
    ANSWER, COVERAGE, MODEL, PROMPT, TOOLS, BoundaryIterator, WireBytes, boundary_method,
    native_provider, native_spy, ollama_frame, persisted, projection, request_body,
    stream_chunk, tool_delta,
)


@pytest.fixture
def recording_factory(evidence_path):
    owned = []

    def create(name="model", capture="content"):
        path = evidence_path / (name + ".sqlite")
        recorder = Recorder(path, capture=capture, coverage=COVERAGE)
        owned.append(recorder)
        return recorder, path

    yield create
    for recorder in owned:
        closed = recorder.close(timeout=5)
        assert closed.writer_stopped, "Each case must join its own recording writer before fixture cleanup."


def test_native_signature_shared_receipts_and_invocation_ownership(recording_factory, run_sdk_async):
    recorder, path = recording_factory()
    requests, policy_calls = [], []
    provider = native_provider(requests)

    def policy(kind, payload, context):
        policy_calls.append(kind)
        return {"policy_id": "never-awaited", "retained": payload}

    signature = python_inspect.signature(provider.acompletion)
    expected = ["model", "messages", "tools", "tool_choice", "temperature", "top_p", "max_tokens",
                "response_format", "stream", "n", "stop", "presence_penalty", "frequency_penalty",
                "seed", "user", "session_label", "parallel_tool_calls", "logprobs", "top_logprobs",
                "logit_bias", "stream_options", "max_completion_tokens", "reasoning_effort",
                "prompt_cache_key", "service_tier", "timeout", "kwargs"]
    check.equal(list(signature.parameters), expected,
                msg="The installed native signature must match the exercised contract; production derives names from that signature.")
    check.equal([parameter.kind.name for parameter in signature.parameters.values()],
                ["POSITIONAL_OR_KEYWORD"] * 2 + ["KEYWORD_ONLY"] * 24 + ["VAR_KEYWORD"],
                msg="Only model/messages are positional; provider extras arrive through the one native kwargs slot.")
    check.equal({name: parameter.default for name, parameter in signature.parameters.items()
                 if parameter.kind is python_inspect.Parameter.KEYWORD_ONLY},
                {name: "auto" if name == "reasoning_effort" else None for name in expected[2:-1]},
                msg="The installed native defaults must match the exercised contract; omitted defaults must not become submitted body evidence.")
    check.is_true(ModelEvidence is openai_chat.ModelEvidence,
                  msg="Both public modules must export the same exact evidence type and ownership rule.")
    check.is_true(anyllm_ollama.ModelDecision is openai_chat.ModelDecision,
                  msg="Decision receipt values are shared, not copied into an incompatible side channel.")

    async def scenario():
        try:
            for label in (None, "", "x" * 257, "\ud800"):
                with pytest.raises(ValueError):
                    wrap_model(recorder, provider.acompletion, instance_id=label)
            create = wrap_model(recorder, provider.acompletion, instance_id="ollama", retention_policy=policy)
            check.equal(python_inspect.signature(create), signature,
                        msg="The wrapper exposes the actual cached native signature through wraps.")
            with pytest.raises(TypeError):
                create(MODEL)
            with pytest.raises(TypeError):
                create(MODEL, [], model=MODEL)
            with pytest.raises(TypeError):
                create(MODEL, [], evidence=object())
            evidence = ModelEvidence()
            pending = create(MODEL, [], evidence=evidence)
            check.is_true(python_inspect.iscoroutine(pending), msg="Invocation creates only an unscheduled coroutine.")
            check.is_none(evidence.request, msg="Never-awaited model work must not allocate a request receipt.")
            check.equal(policy_calls, [], msg="A configured policy must remain lazy with the native AnyLLM invocation.")
            check.equal(evidence.decisions, (), msg="Never-awaited work has no fabricated decision occurrence.")
            check.equal(recorder.health().ordinary.used_slots, 0,
                        msg="Creating a coroutine must not reserve recorder admission capacity.")
            with pytest.raises(ValueError):
                create(MODEL, [], evidence=evidence)
            pending.close()
        finally:
            await provider.client._client.aclose()

    run_sdk_async(scenario())
    events, _, _, _ = persisted(recorder, path)
    check.equal(events, [], msg="Rejected configuration and never-awaited calls leave the recording empty.")
    check.equal(requests, [], msg="Invocation validation must not initiate provider work.")
    check.equal(policy_calls, [], msg="Closing never-awaited work must not execute its configured policy.")


@pytest.mark.parametrize("tools", [False, True], ids=["text", "tools"])
def test_native_normal_response_identity_and_projection(recording_factory, run_sdk_async, tools):
    recorder, path = recording_factory()
    requests, evidence = [], ModelEvidence()
    frame = ollama_frame(content=ANSWER, tools=[("echo", {"x": 7})] if tools else None)
    provider = native_provider(requests, body=frame)
    spy, seen = native_spy(provider.acompletion)
    options = {"model": MODEL, "messages": [{"role": "user", "content": PROMPT}], "tools": TOOLS,
               "stream": False, "temperature": 0}

    async def scenario():
        try:
            control = await provider.acompletion(**options)
            with recorder.turn(PROMPT):
                result = await wrap_model(recorder, spy, instance_id="ollama")(**options, evidence=evidence)
            check.is_true(result is seen["results"][0],
                          msg="The nonstreaming facade returns the exact native completion object.")
            check.equal(result.choices[0].message.content, control.choices[0].message.content,
                        msg="Observed output preserves the native caller-visible text.")
            check.equal(result.choices[0].message.reasoning.content, "reasoning λ",
                        msg="AnyLLM's exposed reasoning extension remains available to its caller.")
            return projection(result)
        finally:
            await provider.client._client.aclose()

    returned = run_sdk_async(scenario())
    events, artifacts, links, _ = persisted(recorder, path)
    check.equal(requests, [requests[0], requests[0]],
                msg="Native control and observed invocation each submit one identical provider body.")
    check.equal(request_body(events[0], artifacts), options,
                msg="The recorded request preserves exactly supplied JSON, without synthesizing native defaults.")
    check.equal(events[-1]["body"], returned,
                msg="The shared trusted projection retains every exposed subclass field, explicit null, timing and usage value.")
    check.equal(events[-1]["body"]["usage"]["total_duration"], 19,
                msg="AnyLLM-only provider timing data survives the existing OpenAI-parent projector.")
    check.equal(events[-1]["state"], "complete", msg="Fully projected normal native output has a complete response observation.")
    check.equal(len(evidence.decisions), int(tools), msg="Only actual function selections receive decision receipts.")
    if tools:
        decision = evidence.decisions[0]
        check.equal((decision.choice_index, decision.tool_index), (0, 0),
                    msg="Normal response identity uses the exposed choice index and tool-list position.")
        check.equal(decision.provider_ref, returned["choices"][0]["message"]["tool_calls"][0]["id"],
                    msg="The AnyLLM-generated UUID remains unchanged; it is labelled normalized output, not a wire identity.")
        check.equal(events[1]["body"], {"arguments_raw": '{"x": 7}'},
                    msg="Stored arguments retain the SDK's exposed serialization without pretending it is original wire JSON.")
        check.is_true(events[1]["details"]["complete"], msg="A native normal tool_calls finish completes the known selection.")
    check.equal(links, [], msg="A normalized provider reference does not invent host provenance links.")
    check.equal(evidence.losses, (), msg="The supported normal route has no adapter projection or request loss.")


def _assert_policy_artifact(artifact, retained, original, *, policy_id, redacted):
    expected = json.dumps(retained, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
    observed = json.dumps(original, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
    assert artifact["body_text"] is not None, "Selected retained content must have a durable body before byte checks."
    check.equal(artifact["body_text"].encode("utf-8"), expected,
                msg="Durable bytes equal the independently specified retained JSON, including UTF-8 encoding.")
    check.equal(artifact["stored_bytes"], len(expected), msg="Stored length describes retained content.")
    check.equal(artifact["stored_sha256"], hashlib.sha256(expected).hexdigest(),
                msg="Stored hash describes retained bytes rather than the original body.")
    check.equal(artifact["observed_bytes"], len(observed), msg="Observed length still describes the corresponding original part.")
    check.equal((artifact["policy_id"], artifact["redacted"], artifact["truncated"], artifact["omitted_reason"]),
                (policy_id, int(redacted), 0, None), msg="The selected policy and flags survive artifact preparation.")


@pytest.mark.parametrize("placement", ["positional", "keyword"])
def test_native_selective_retention_preserves_conversion_and_originals(recording_factory, run_sdk_async, placement):
    recorder, path = recording_factory()
    marker = "anyllm-retention-private-Ω"
    messages = [{"role": "system", "content": "instruction " + marker},
                {"role": "user", "content": "question " + marker}]
    tools = json.loads(json.dumps(TOOLS))
    tools[0]["function"]["description"] = marker
    original_messages = json.loads(json.dumps(messages))
    original_tools = json.loads(json.dumps(tools))
    requests, calls, outputs, evidence = [], [], [], ModelEvidence()
    provider = native_provider(requests, body=ollama_frame(content=marker, tools=[("echo", {"x": 7, "secret": marker})]))
    spy, seen = native_spy(provider.acompletion)
    options = {"tools": tools, "stream": False, "temperature": 0, "seed": 0,
               "num_ctx": 2048, "top_k": 12, "num_predict": 9, "max_tokens": 19,
               "session_label": "native-only-label", "timeout": None}

    def policy(kind, payload, context):
        calls.append((kind, json.loads(json.dumps(payload)), dict(context)))
        context["instance_id"] = "policy-owned-context"
        if kind == "model.request":
            payload["messages"][0]["content"] = "retained instruction Ω"
            payload["messages"][1]["content"] = "retained question Ω"
            payload["tools"] = []
        elif kind == "model.tool_call":
            payload.clear()
            payload["arguments_raw"] = '{"x":0}'
        else:
            payload.clear()
            payload["summary"] = "retained answer Ω"
        mapping = {"policy_id": "anyllm-selective", "retained": payload, "redacted": True}
        outputs.append(mapping)
        return mapping

    async def scenario():
        try:
            create = wrap_model(recorder, spy, instance_id="ollama-policy", retention_policy=policy)
            check.equal(python_inspect.signature(create), python_inspect.signature(provider.acompletion),
                        msg="Adding policy configuration leaves the actual native callable signature intact.")
            with recorder.turn("invoked") as turn_a:
                pending = (create(MODEL, messages, evidence=evidence, **options) if placement == "positional"
                           else create(model=MODEL, messages=messages, evidence=evidence, **options))
            check.equal((calls, seen["calls"], requests), ([], [], []),
                        msg="Policy selection and native conversion wait for execution, not public invocation.")
            with recorder.turn("executed") as turn_b:
                result = await pending
                check.is_true(result is seen["results"][0], msg="Selected retention never replaces the genuine normalized result.")
                check.equal(recorder.capture_scope().turn_id, turn_b.receipt.turn_id,
                            msg="Policy capture cannot replace the caller's later ambient turn.")
            original_result = projection(result)
            check.equal(result.choices[0].message.content, marker, msg="The caller still receives unredacted native text.")
            check.equal(json.loads(result.choices[0].message.tool_calls[0].function.arguments), {"x": 7, "secret": marker},
                        msg="Decision masking cannot mutate the native normalized argument string.")
            args, kwargs = seen["calls"][0]
            check.is_true((args[1] if args else kwargs["messages"]) is messages,
                          msg="Native conversion receives the caller's original messages by identity.")
            check.is_true(kwargs["tools"] is tools, msg="The native catalog remains the original supplied object.")
            check.equal(args, (MODEL, messages) if placement == "positional" else (),
                        msg="The facade preserves positional versus keyword invocation without rebinding the native call.")
            check.equal(kwargs, options if placement == "positional" else {"model": MODEL, "messages": messages, **options},
                        msg="All native options pass through exactly; recorder and policy arguments never reach the SDK.")
            for mapping in outputs:
                mapping["retained"].clear()
                mapping.clear()
            return turn_a.receipt.turn_id, original_result
        finally:
            await provider.client._client.aclose()

    turn_id, original_result = run_sdk_async(scenario())
    events, artifacts, links, runs = persisted(recorder, path)
    check.equal((messages, tools), (original_messages, original_tools),
                msg="Mutating callback payload/context/output cannot alter submitted native inputs.")
    check.equal((len(seen["calls"]), len(requests)), (1, 1), msg="All three policy kinds share one genuine provider invocation.")
    check.equal(requests[0]["messages"], original_messages, msg="Actual Ollama transport receives the original ordered messages.")
    check.equal(requests[0]["tools"], original_tools, msg="Retained catalog replacement does not change native tool conversion.")
    check.equal({key: requests[0]["options"][key] for key in ("num_ctx", "top_k", "num_predict")},
                {"num_ctx": 2048, "top_k": 12, "num_predict": 9},
                msg="Native hidden-option conversion still applies num_predict over the separately supplied max_tokens.")
    check.equal([event["kind"] for event in events], ["model.request", "model.tool_call", "model.response"],
                msg="One request, one exposed decision, and one response are admitted without retries.")
    check.equal([item[0] for item in calls], [event["kind"] for event in events],
                msg="Every stored body kind has its independent policy selection.")
    empty_context = {"instance_id": "ollama-policy", "choice_index": None, "tool_index": None,
                     "name": None, "provider_ref": None}
    check.equal(calls[0][2], empty_context, msg="Request policy context contains exactly the documented five keys.")
    check.equal(calls[2][2], empty_context, msg="Response context does not inherit decision-local fields.")
    reference = original_result["choices"][0]["message"]["tool_calls"][0]["id"]
    check.equal(calls[1][2], {**empty_context, "choice_index": 0, "tool_index": 0,
                             "name": "echo", "provider_ref": reference},
                msg="The genuine AnyLLM normalized reference and exposed positions reach the decision policy unchanged.")
    expected_request = {"model": MODEL, "messages": [{"role": "system", "content": "retained instruction Ω"},
                                                     {"role": "user", "content": "retained question Ω"}],
                        **{key: value for key, value in options.items() if key not in ("timeout", "session_label")}}
    expected_request["tools"] = []
    check.equal(request_body(events[0], artifacts), expected_request,
                msg="Retained request preserves supplied named/flattened options and ordered positions with independent content redaction.")
    check.equal(calls[0][1], {"model": MODEL, "messages": original_messages,
                             **{key: value for key, value in options.items() if key not in ("timeout", "session_label")}},
                msg="The request callback sees actual supported submitted arguments, excluding native-only transport options.")
    check.equal(calls[2][1], original_result, msg="The response policy sees the genuine complete SDK projection before masking.")
    manifest = events[0]["body"]
    check.equal(len(manifest["message_refs"]), 2, msg="Per-position replacement preserves both original request members.")
    check.equal(events[0]["catalog_ref"], manifest["field_refs"][0]["artifact_id"],
                msg="The submitted catalog relationship survives an empty retained catalog.")
    for index, key in enumerate(manifest["message_refs"]):
        _assert_policy_artifact(artifacts[key], expected_request["messages"][index], original_messages[index],
                                policy_id="anyllm-selective", redacted=True)
    _assert_policy_artifact(artifacts[events[0]["catalog_ref"]], [], original_tools,
                            policy_id="anyllm-selective", redacted=True)
    _assert_policy_artifact(artifacts[events[1]["payload_ref"]], {"arguments_raw": '{"x":0}'}, calls[1][1],
                            policy_id="anyllm-selective", redacted=True)
    _assert_policy_artifact(artifacts[events[2]["payload_ref"]], {"summary": "retained answer Ω"}, original_result,
                            policy_id="anyllm-selective", redacted=True)
    check.is_true(events[1]["details"]["args_digest"]["value"], msg="Supported original decision arguments retain an available comparison.")
    check.equal(events[1]["details"]["args_digest"]["basis"], "observed_original", msg="Retention cannot become comparison provenance.")
    check.is_not_in(marker, json.dumps(artifacts, ensure_ascii=False),
                    msg="Each body representation containing the private marker is independently masked in durable artifacts.")
    check.equal([event["turn_id"] for event in events], [turn_id] * 3, msg="Policy work retains invocation attribution across turns.")
    check.equal({event["model_call_id"] for event in events}, {evidence.request.model_call_id},
                msg="One model occurrence associates its request, decision and response without creating MCP tickets.")
    check.is_true(all(receipt.status == "accepted" for receipt in
                      (evidence.request, evidence.decisions[0].receipt, evidence.response)),
                  msg="The bounded native scenario admits every intended event rather than merely avoiding an exception.")
    check.equal([event["details"]["instance_id"] for event in events], ["ollama-policy"] * 3,
                msg="Policy-owned context mutation cannot rewrite durable attribution.")
    check.equal((events[-1]["state"], runs[0]["close_state"], evidence.losses, links), ("complete", "clean", (), []),
                msg="Intentional body redaction preserves complete source evidence and ordinary joined close.")
    health = recorder.health()
    check.equal((health.dropped_events, health.ordinary.used_slots, health.completion.used_slots, health.open_operations),
                (0, 0, 0, 0), msg="The completed model route drops no event and leaves no ordinary or MCP completion capacity.")


@pytest.mark.parametrize("ending", ["eof", "early-close"])
def test_native_selective_retention_consumes_only_requested_stream(recording_factory, run_sdk_async, ending):
    recorder, path = recording_factory()
    marker = "anyllm-stream-private-Ω"
    frames = [ollama_frame(content=marker, tools=[("echo", {"x": 7})]),
              ollama_frame(content="suffix " + marker)]
    wire, requests, evidence, calls = WireBytes(frames), [], ModelEvidence(), []
    provider = native_provider(requests, wire=wire)
    spy, seen = native_spy(provider.acompletion)
    messages = [{"role": "user", "content": "stream request"}]

    def policy(kind, payload, context):
        calls.append((kind, json.loads(json.dumps(payload)), dict(context)))
        if kind == "model.request":
            return {"policy_id": "anyllm-stream", "retained": payload}
        if kind == "model.tool_call":
            return {"policy_id": "anyllm-stream", "omitted_reason": "chosen-decision-omission"}
        return {"policy_id": "anyllm-stream", "retained": {"consumed": len(payload["chunks"]), "summary": "retained stream Ω"},
                "redacted": True}

    async def scenario():
        try:
            with recorder.turn("stream invocation") as turn_a:
                pending = wrap_model(recorder, spy, instance_id="ollama-stream", retention_policy=policy)(
                    MODEL, messages, tools=TOOLS, stream=True, num_ctx=2048, evidence=evidence)
            check.equal((calls, seen["calls"], wire.reads), ([], [], []), msg="Configured streams remain unscheduled until awaited.")
            with recorder.turn("stream consumer"):
                stream = await pending
                ledger = seen["iterators"][0]
                check.equal([item[0] for item in calls], ["model.request"], msg="Awaiting the factory selects only its submitted request.")
                check.equal((requests, wire.reads), ([], []), msg="The native AnyLLM HTTP iterator is still lazy after create returns.")
                consumed = [await stream.__anext__()]
                check.is_true(consumed[0] is ledger.chunks[0], msg="Retention forwards the exact first native converted chunk.")
                check.equal(wire.reads, [0], msg="One explicit read consumes one NDJSON frame.")
                check.equal([item[0] for item in calls], ["model.request", "model.tool_call"],
                            msg="A complete function decision is independently selected before its terminal native chunk is forwarded.")
                check.is_none(evidence.response, msg="A terminal chunk does not fabricate native EOF or prematurely run aggregate policy.")
                if ending == "eof":
                    consumed.append(await stream.__anext__())
                    check.is_true(consumed[1] is ledger.chunks[1], msg="The second requested chunk keeps exact identity and order.")
                    check.equal(len(calls), 2, msg="A further text chunk is not another policy body invocation.")
                    with pytest.raises(StopAsyncIteration):
                        await stream.__anext__()
                    check.is_true(ledger.eof, msg="Only genuine native exhaustion completes the aggregate.")
                else:
                    check.is_false(ledger.eof, msg="Closing the consumed prefix must remain distinct from observed EOF.")
                await stream.aclose()
                before = len(calls)
                await stream.aclose()
                check.equal((len(calls), ledger.closes), (before, 2),
                            msg="Repeated explicit cleanup remains native work without duplicate policy or terminal observations.")
                check.equal((stream._capture.chunks, stream._capture.tools, stream._capture.choices), ([], {}, {}),
                            msg="The existing observer releases its bounded aggregate and tool state after cleanup.")
            return turn_a.receipt.turn_id, [projection(chunk) for chunk in consumed]
        finally:
            await provider.client._client.aclose()

    turn_id, consumed = run_sdk_async(scenario())
    events, artifacts, links, runs = persisted(recorder, path)
    count = 2 if ending == "eof" else 1
    check.equal((len(seen["calls"]), len(requests), wire.reads), (1, 1, list(range(count))),
                msg="The policy neither repeats native creation nor drains an unread stream suffix.")
    check.equal([item[0] for item in calls], ["model.request", "model.tool_call", "model.response"],
                msg="Three body kinds execute once; chunks themselves are not policy calls.")
    original_aggregate = {"format": "anyllm_ollama_stream/1", "chunks": consumed}
    check.equal(calls[-1][1], original_aggregate, msg="The aggregate policy sees only the exact native chunks consumed by the host.")
    check.equal([event["kind"] for event in events], ["model.request", "model.tool_call", "model.response"],
                msg="Consumed native selection and terminal each remain one admitted occurrence.")
    expected_request = {"model": MODEL, "messages": messages, "tools": TOOLS, "stream": True, "num_ctx": 2048}
    check.equal(request_body(events[0], artifacts), expected_request, msg="Explicit request retention keeps supplied native conversion inputs.")
    check.equal(requests[0]["messages"], messages, msg="Original messages still reach the genuine Ollama transport.")
    check.equal(requests[0]["options"]["num_ctx"], 2048, msg="The retained stream still forwards supported hidden native options.")
    context = {"instance_id": "ollama-stream", "choice_index": None, "tool_index": None, "name": None, "provider_ref": None}
    reference = consumed[0]["choices"][0]["delta"]["tool_calls"][0]["id"]
    check.equal([item[2] for item in calls], [context, {**context, "choice_index": 0, "tool_index": 0,
                                                       "name": "echo", "provider_ref": reference}, context],
                msg="Request/decision/aggregate contexts retain the exact call-local normalized tool facts.")
    decision = artifacts[events[1]["payload_ref"]]
    check.equal((decision["body_text"], decision["stored_bytes"], decision["stored_sha256"], decision["omitted_reason"], decision["policy_id"]),
                (None, 0, None, "chosen-decision-omission", "anyllm-stream"),
                msg="Explicit decision omission is separate from the selected aggregate's retained content.")
    check.is_true(events[1]["details"]["complete"], msg="A complete native tool selection stays complete at an early response close.")
    check.is_true(events[1]["details"]["args_digest"]["value"], msg="Omitted decision content retains its available original comparison.")
    _assert_policy_artifact(artifacts[events[-1]["payload_ref"]], {"consumed": count, "summary": "retained stream Ω"},
                            original_aggregate, policy_id="anyllm-stream", redacted=True)
    check.is_not_in(marker, json.dumps(artifacts, ensure_ascii=False), msg="Separately retained aggregate content omits original private chunk text.")
    check.equal(events[-1]["state"], "complete" if ending == "eof" else "partial",
                msg="Valid retention cannot promote an early-close source aggregate into a complete response.")
    check.equal(runs[0]["close_state"], "clean" if ending == "eof" else "incomplete",
                msg="Joined close preserves the original full-versus-partial consumption truth.")
    check.equal([event["turn_id"] for event in events], [turn_id] * 3, msg="Stream policy observations keep invocation scope across later consumption.")
    check.equal({event["model_call_id"] for event in events}, {evidence.request.model_call_id},
                msg="Request, selection and aggregate retain one model-call association.")
    check.is_true(all(receipt.status == "accepted" for receipt in
                      (evidence.request, evidence.decisions[0].receipt, evidence.response)),
                  msg="Selected stream bodies are admitted once under the bounded fixture.")
    check.equal((evidence.losses, links), ((), []), msg="Intentional omission and early close do not fabricate projection failure or host links.")
    check.is_true(provider.client._client.is_closed, msg="The test closes its genuine native provider transport.")
    health = recorder.health()
    check.equal((health.dropped_events, health.ordinary.used_slots, health.completion.used_slots, health.open_operations),
                (0, 0, 0, 0), msg="Both stream outcomes release recording resources without inventing MCP operation reservations.")


@pytest.mark.parametrize("topology,finish", [("one-chunk", "stop"), ("separate-chunks", "stop"),
                                               ("separate-chunks", "tool_calls")],
                         ids=["two-tools-stop", "separate-equal-stop", "separate-equal-tool-calls"])
def test_native_stream_lazy_identity_positions_and_eof(recording_factory, run_sdk_async, topology, finish):
    recorder, path = recording_factory()
    frames = ([ollama_frame(content="", tools=[("echo", {"x": 7}), ("echo", {"x": 7})], finish=finish)]
              if topology == "one-chunk" else
              [ollama_frame(content="", tools=[("echo", {"x": 7})], done=False, finish=None),
               ollama_frame(content="", tools=[("echo", {"x": 7})], finish=finish)])
    wire, requests, evidence = WireBytes(frames), [], ModelEvidence()
    provider = native_provider(requests, wire=wire)
    spy, seen = native_spy(provider.acompletion)

    async def scenario():
        try:
            with recorder.turn(PROMPT):
                stream = await wrap_model(recorder, spy, instance_id="ollama")(
                    MODEL, [{"role": "user", "content": PROMPT}], stream=True, tools=TOOLS, evidence=evidence)
                check.equal(requests, [], msg="Awaiting the native create preserves its lazy HTTP boundary.")
                check.equal(wire.reads, [], msg="Returning a stream must not prefetch native chunks.")
                for method in ("response", "close", "__aenter__", "__aexit__"):
                    check.is_false(hasattr(stream, method),
                                   msg=f"The AnyLLM facade must not inherit unsupported OpenAI {method} behavior.")
                ledger = seen["iterators"][0]
                consumed = []
                for index in range(len(frames)):
                    chunk = await stream.__anext__()
                    consumed.append(chunk)
                    check.is_true(chunk is ledger.chunks[index],
                                  msg="Each caller-requested chunk retains the exact object produced by native conversion.")
                    check.equal(wire.reads, list(range(index + 1)),
                                msg="Native NDJSON reads advance only with explicit consumer requests.")
                check.equal(len(evidence.decisions), 2,
                            msg="The consumed accepted terminal reason emits both complete decisions before yielding its chunk.")
                check.is_none(evidence.response, msg="A terminal chunk alone is not actual native exhaustion.")
                with pytest.raises(StopAsyncIteration):
                    await stream.__anext__()
                check.is_true(ledger.eof, msg="Only the observed native StopAsyncIteration completes the response.")
                await stream.aclose()
                return [projection(item) for item in consumed]
        finally:
            await provider.client._client.aclose()

    normalized = run_sdk_async(scenario())
    events, _, links, _ = persisted(recorder, path)
    decisions = [event for event in events if event["kind"] == "model.tool_call"]
    check.equal([(item.choice_index, item.tool_index) for item in evidence.decisions], [(0, 0), (0, 1)],
                msg="Native outer-generator indices keep equal tool names and arguments as distinct occurrences.")
    exposed = [tool for item in normalized for tool in item["choices"][0]["delta"].get("tool_calls", [])]
    check.equal([item.provider_ref for item in evidence.decisions], [tool["id"] for tool in exposed],
                msg="Neither position nor normalized UUIDs are rewritten by observation.")
    check.equal(len({item.receipt.decision_id for item in evidence.decisions}), 2,
                msg="Equal-argument selections retain two separate admitted decision identities.")
    check.is_true(all(event["details"]["complete"] for event in decisions),
                  msg="Both complete exposed tool fields are finalized by the native accepted terminal reason.")
    check.equal(events[-1]["body"], {"format": "anyllm_ollama_stream/1", "chunks": normalized},
                msg="The named observation stores exactly consumed normalized chunks, preserving reason and order.")
    check.equal(events[-1]["state"], "complete", msg="Actual EOF after all terminal choices permits complete stream capture.")
    check.equal(len(requests), 1, msg="Observation does not add provider requests.")
    check.equal(links, [], msg="Native model selections alone do not manufacture host result associations.")


def test_native_late_tool_survives_body_overflow(recording_factory, run_sdk_async):
    recorder, path = recording_factory()
    cap = 1_048_576
    wire = WireBytes([
        ollama_frame(content="prefix", done=False, finish=None, thinking=None),
        ollama_frame(content="x" * cap, done=False, finish=None, thinking=None),
        ollama_frame(content="", tools=[("echo", {"x": 7})], done=True,
                     finish="stop", thinking=None),
    ])
    requests, evidence = [], ModelEvidence()
    provider = native_provider(requests, wire=wire)
    spy, seen = native_spy(provider.acompletion)

    async def scenario():
        try:
            with recorder.turn(PROMPT):
                stream = await wrap_model(recorder, spy, instance_id="ollama")(
                    MODEL, [{"role": "user", "content": PROMPT}], stream=True, tools=TOOLS, evidence=evidence)
                assert requests == [] and wire.reads == []
                ledger = seen["iterators"][0]
                consumed = []
                for index in range(3):
                    item = await stream.__anext__()
                    consumed.append(item)
                    assert item is ledger.chunks[index]
                    assert wire.reads == list(range(index + 1))
                assert len(evidence.decisions) == 1
                assert evidence.decisions[0].receipt is not None and evidence.decisions[0].receipt.status == "accepted"
                assert evidence.response is None
                assert (stream._capture.body_stopped, stream._capture.stopped, stream._capture.selection_gap) == (True, False, False)
                with pytest.raises(StopAsyncIteration):
                    await stream.__anext__()
                assert ledger.eof
                response = evidence.response
                assert response is not None and response.status == "accepted"
                await stream.aclose()
                await stream.aclose()
                assert evidence.response is response and len(evidence.decisions) == 1
                assert wire.reads == [0, 1, 2] and len(ledger.chunks) == 3
                assert (stream._capture.chunks, stream._capture.tools, stream._capture.choices) == ([], {}, {})
                assert (stream._capture.chunk_bytes, stream._capture.fragment_bytes) == (0, 0)
                return [projection(item) for item in consumed]
        finally:
            await provider.client._client.aclose()

    normalized = run_sdk_async(scenario())
    events, _, _, _ = persisted(recorder, path)
    assert [event["kind"] for event in events] == ["model.request", "model.tool_call", "model.response"]
    exposed = normalized[2]["choices"][0]["delta"]["tool_calls"][0]
    decision = evidence.decisions[0]
    assert (decision.choice_index, decision.tool_index, decision.provider_ref) == (0, 0, exposed["id"])
    assert events[1]["name"] == exposed["function"]["name"] == "echo"
    assert events[1]["body"] == {"arguments_raw": exposed["function"]["arguments"]}
    assert events[1]["details"]["complete"] is True and events[1]["details"]["args_digest"]["value"] is not None
    assert events[-1]["body"] == {"format": "anyllm_ollama_stream/1", "chunks": normalized[:1]}
    assert events[-1]["state"] == "partial" and evidence.losses == ("stream_capture_limit",)
    assert len(requests) == len(seen["calls"]) == 1
    assert provider.client._client.is_closed


def test_native_terminal_chunk_then_close_is_partial(recording_factory, run_sdk_async):
    recorder, path = recording_factory()
    frames = [ollama_frame(content="", tools=[("echo", {"x": 7})]), ollama_frame(content="unconsumed")]
    wire, requests, evidence = WireBytes(frames), [], ModelEvidence()
    provider = native_provider(requests, wire=wire)

    async def scenario():
        try:
            stream = await wrap_model(recorder, provider.acompletion, instance_id="ollama")(
                MODEL, [{"role": "user", "content": PROMPT}], stream=True, evidence=evidence)
            first = await stream.__anext__()
            check.equal(len(evidence.decisions), 1, msg="A stop-with-tools chunk exposes its complete selection immediately.")
            check.is_none(evidence.response, msg="Selection completion cannot fabricate unobserved stream exhaustion.")
            await stream.aclose()
            await stream.aclose()
            return projection(first)
        finally:
            await provider.client._client.aclose()

    prefix = run_sdk_async(scenario())
    events, _, _, _ = persisted(recorder, path)
    check.equal(wire.reads, [0], msg="Explicit close after a terminal chunk must not drain an unrequested suffix.")
    check.equal([event["kind"] for event in events], ["model.request", "model.tool_call", "model.response"],
                msg="Repeated native cleanup produces one recorder terminal and no duplicate selection.")
    check.equal(events[-1]["body"]["chunks"], [prefix], msg="Early-close capture retains only the consumed prefix.")
    check.equal(events[-1]["state"], "partial", msg="Unobserved EOF remains partial despite a completed tool selection.")
    check.is_true(events[1]["details"]["complete"], msg="An independently complete decision survives a partial response boundary.")


@pytest.mark.parametrize("placement", ["positional", "keyword"])
def test_supplied_arguments_freeze_at_execution_with_native_options(recording_factory, run_sdk_async, placement):
    recorder, path = recording_factory()
    requests, evidence = [], ModelEvidence()
    wire = WireBytes([ollama_frame()])
    provider = native_provider(requests, wire=wire)
    spy, seen = native_spy(provider.acompletion)
    messages = [{"role": "user", "content": "factory"}]
    annotations = [{"bindings": [], "unresolved": [], "complete": True}]
    options = {"stream": True, "tools": TOOLS, "temperature": 0, "top_p": 0.6, "seed": 0,
               "num_ctx": 2048, "top_k": 12, "num_predict": 9, "max_tokens": 19,
               "num_keep": 1, "min_p": 0.1, "typical_p": 0.9, "repeat_last_n": 8,
               "repeat_penalty": 1.1, "presence_penalty": 0, "frequency_penalty": 0,
               "stop": ["END"], "num_batch": 4, "session_label": "not-a-body-field", "timeout": None}

    async def scenario():
        try:
            create = wrap_model(recorder, spy, instance_id="ollama")
            with recorder.turn("original") as turn_a:
                if placement == "positional":
                    pending = create(MODEL, messages, evidence=evidence, annotations=annotations, **options)
                else:
                    pending = create(model=MODEL, messages=messages, evidence=evidence, annotations=annotations, **options)
            messages[0]["content"] = "execution"
            with recorder.turn("consumer") as turn_b:
                stream = await pending
                messages[0]["content"] = "after-execution"
                annotations[0]["complete"] = False
                result = [projection(chunk) async for chunk in stream]
                await stream.aclose()
                check.equal(recorder.capture_scope().turn_id, turn_b.receipt.turn_id,
                            msg="Completing work under its original scope cannot overwrite the host's current turn.")
            args, kwargs = seen["calls"][0]
            check.is_true((args[1] if args else kwargs["messages"]) is messages,
                          msg="The original message object is forwarded, not replaced by its capture copy.")
            check.is_true(kwargs["tools"] is TOOLS, msg="Tool/callback identities remain owned by the native caller.")
            check.equal(kwargs["session_label"], "not-a-body-field",
                        msg="The documented non-body exclusion remains forwarded to the SDK.")
            check.is_none(kwargs["timeout"], msg="An explicitly supplied timeout None remains a native argument.")
            check.is_false(any(key in kwargs for key in ("evidence", "annotations", "insertions")),
                           msg="Recorder-only arguments must not reach the native method.")
            return turn_a.receipt.turn_id, result
        finally:
            await provider.client._client.aclose()

    turn_id, result = run_sdk_async(scenario())
    events, artifacts, _, _ = persisted(recorder, path)
    expected = {"model": MODEL, "messages": [{"role": "user", "content": "execution"}],
                **{key: value for key, value in options.items() if key not in ("timeout", "session_label")}}
    check.equal(request_body(events[0], artifacts), expected,
                msg="Capture binds supplied names only, flattens supported hidden options once, and freezes at execution.")
    check.equal([event["turn_id"] for event in events], [turn_id] * len(events),
                msg="Request, chunks and terminal retain invocation turn A despite later execution and cleanup.")
    check.equal(requests[0]["messages"][0]["content"], "execution",
                msg="The native SDK's own request preparation retains its original execution-time value.")
    check.equal({key: requests[0]["options"][key] for key in ("num_ctx", "top_k", "num_predict")},
                {"num_ctx": 2048, "top_k": 12, "num_predict": 9},
                msg="Actual native conversion receives the configured Ollama option control unchanged.")
    check.equal(request_body(events[0], artifacts)["max_tokens"], 19,
                msg="Capture preserves conflicting supplied limits rather than emulating the SDK's num_predict precedence.")
    check.is_not_in("reasoning_effort", request_body(events[0], artifacts),
                 msg="The native default reasoning_effort is not falsely reported as a supplied argument.")
    check.equal(evidence.losses, (), msg="All fifteen supported options and ordinary named arguments are capturable JSON.")


class StructuredAnswer(BaseModel):
    value: str


def echo_callable(x: int) -> int:
    """Echo the provided integer."""
    return x


@pytest.mark.parametrize("unsupported", ["unknown-option", "callable-tool", "message-model", "output-class", "one-shot-messages"])
def test_unavailable_native_request_does_not_erase_complete_selection(recording_factory, run_sdk_async, unsupported):
    recorder, path = recording_factory()
    requests, evidence, iterations = [], ModelEvidence(), []
    provider = native_provider(requests, body=ollama_frame(content='{"value":"ready"}', tools=[("echo", {"x": 7})]))
    spy, seen = native_spy(provider.acompletion)
    options = {"model": MODEL, "messages": [{"role": "user", "content": PROMPT}], "tools": TOOLS}
    if unsupported == "unknown-option":
        options["unknown_private_option"] = "excluded-private-sentinel"
    elif unsupported == "callable-tool":
        options["tools"] = [echo_callable]
    elif unsupported == "message-model":
        options["messages"] = [ChatCompletionMessage(role="assistant", content=PROMPT)]
    elif unsupported == "output-class":
        options["response_format"] = StructuredAnswer
    else:
        def messages():
            iterations.append("native-consumption")
            yield {"role": "user", "content": PROMPT}
        options["messages"] = messages()

    async def scenario():
        try:
            pending = wrap_model(recorder, spy, instance_id="ollama")(**options, evidence=evidence)
            check.equal(iterations, [], msg="Capture at invocation cannot consume one-shot native messages.")
            result = await pending
            check.is_true(result is seen["results"][0],
                          msg="Request unavailability cannot replace the actual native completion.")
            check.is_true(seen["calls"][0][1]["messages"] is options["messages"],
                          msg="Unsupported message objects still reach the native callable by identity.")
            check.is_true(seen["calls"][0][1]["tools"] is options["tools"],
                          msg="Callable or JSON tools remain native-owned input objects.")
            return projection(result)
        finally:
            await provider.client._client.aclose()

    result = run_sdk_async(scenario())
    events, artifacts, _, runs = persisted(recorder, path)
    check.equal(len(requests), 1, msg="Unsupported capture input must not suppress or duplicate an otherwise valid native call.")
    check.equal(iterations, ["native-consumption"] if unsupported == "one-shot-messages" else [],
                msg="Only the native SDK consumes a one-shot input, exactly once.")
    check.is_none(events[0]["body"], msg="An unsupported supplied body is unavailable instead of a silently trimmed complete request.")
    check.equal(artifacts[events[0]["payload_ref"]]["omitted_reason"], "unexposed",
                msg="The core's explicit omitted artifact identifies MISSING input without inventing an absent artifact row.")
    check.is_false(events[0]["details"]["complete"], msg="Request unavailability is persisted, not only reported in the side channel.")
    check.equal(evidence.losses, ("request_unavailable",), msg="Unsupported request projection uses the existing bounded diagnostic.")
    check.equal(len(evidence.decisions), 1, msg="Missing request capture does not discard an independently observed selection.")
    check.is_true(events[1]["details"]["complete"], msg="Complete exposed native tool fields retain their own completeness.")
    check.equal(events[-1]["body"], result, msg="Supported native output remains fully retained despite unavailable input.")
    check.equal(events[-1]["state"], "partial", msg="The enclosing response still discloses the missing request context.")
    check.equal(runs[0]["close_state"], "incomplete", msg="An unavailable request cannot produce a falsely complete recording.")


def test_unavailable_native_request_keeps_complete_stream_selection(recording_factory, run_sdk_async):
    recorder, path = recording_factory()
    requests, evidence = [], ModelEvidence()
    wire = WireBytes([ollama_frame(content="", tools=[("echo", {"x": 7})])])
    provider = native_provider(requests, wire=wire)
    spy, seen = native_spy(provider.acompletion)

    async def scenario():
        try:
            stream = await wrap_model(recorder, spy, instance_id="ollama")(
                MODEL, [{"role": "user", "content": PROMPT}], stream=True,
                unknown_private_option="excluded-native-option", evidence=evidence)
            chunk = await stream.__anext__()
            check.is_true(chunk is seen["iterators"][0].chunks[0],
                          msg="Unavailable request capture does not replace the later native stream chunk.")
            check.equal(len(evidence.decisions), 1,
                        msg="A fully exposed stop-with-tools selection completes before yielding despite its unavailable request body.")
            check.is_none(evidence.response, msg="Request loss cannot fabricate native EOF after one terminal chunk.")
            with pytest.raises(StopAsyncIteration):
                await stream.__anext__()
            await stream.aclose()
            return projection(chunk)
        finally:
            await provider.client._client.aclose()

    result = run_sdk_async(scenario())
    events, artifacts, _, runs = persisted(recorder, path)
    check.equal(artifacts[events[0]["payload_ref"]]["omitted_reason"], "unexposed",
                msg="The unsupported supplied option makes input explicitly unavailable rather than silently omitted.")
    check.is_true(events[1]["details"]["complete"],
                  msg="The shared accumulator does not transfer unrelated request incompleteness onto a fully witnessed selection.")
    check.equal(events[-1]["body"], {"format": "anyllm_ollama_stream/1", "chunks": [result]},
                msg="Actual consumed native output remains retained alongside explicit input loss.")
    check.equal(events[-1]["state"], "partial", msg="The enclosing response remains partial because its submitted request was unavailable.")
    check.equal(runs[0]["close_state"], "incomplete", msg="Known tool selection does not conceal the incomplete surrounding capture.")
    check.equal(evidence.losses, ("request_unavailable",),
                msg="A complete selection does not acquire a spurious partial_tool_call diagnostic from missing request context.")


@pytest.mark.parametrize("ending", ["close-before-first", "empty-eof", "nonterminal-eof", "partial-tool"])
def test_native_partial_stream_boundaries(recording_factory, run_sdk_async, ending):
    recorder, path = recording_factory()
    frames = ({"close-before-first": [ollama_frame()], "empty-eof": [],
               "nonterminal-eof": [ollama_frame(done=False, finish=None)],
               "partial-tool": [ollama_frame(content="", tools=[("echo", {"x": 7})], done=False, finish=None)]})[ending]
    wire, requests, evidence = WireBytes(frames), [], ModelEvidence()
    provider = native_provider(requests, wire=wire)

    async def scenario():
        try:
            stream = await wrap_model(recorder, provider.acompletion, instance_id="ollama")(
                MODEL, [{"role": "user", "content": PROMPT}], stream=True, evidence=evidence)
            consumed = []
            if ending == "partial-tool":
                consumed.append(projection(await stream.__anext__()))
            elif ending != "close-before-first":
                consumed = [projection(chunk) async for chunk in stream]
            await stream.aclose()
            await stream.aclose()
            return consumed
        finally:
            await provider.client._client.aclose()

    consumed = run_sdk_async(scenario())
    events, _, _, _ = persisted(recorder, path)
    check.equal(len(requests), int(ending != "close-before-first"),
                msg="Closing the still-lazy stream cannot start HTTP work; consumed routes make one native request.")
    check.equal(wire.reads, list(range(len(consumed))), msg="Capture adds neither eager reads nor a cleanup drain.")
    terminals = [event for event in events if event["kind"] == "model.response"]
    assert len(terminals) == 1, "Every observed boundary must have one terminal before checking its capture state."
    check.equal(terminals[0]["state"], "partial", msg="Empty or unfinished consumption cannot be declared a complete response.")
    check.equal(terminals[0]["body"], {"format": "anyllm_ollama_stream/1", "chunks": consumed},
                msg="Partial evidence contains only the exact consumed normalized prefix, even when it is empty.")
    check.equal(len(evidence.decisions), int(ending == "partial-tool"),
                msg="Only an actually consumed unfinished tool creates a partial decision occurrence.")
    if ending == "partial-tool":
        check.is_false(events[1]["details"]["complete"], msg="A complete-looking fragment without a terminal witness stays partial.")


@pytest.mark.parametrize("ending", ["create-error", "midstream-error", "cancellation"])
def test_native_failure_and_cancellation_keep_original_scope(recording_factory, run_sdk_async, ending):
    recorder, path = recording_factory()
    requests, evidence = [], ModelEvidence()
    frames = [ollama_frame(content="", tools=[("echo", {"x": 7})])]
    wire = WireBytes(frames, error_after=1 if ending == "midstream-error" else None,
                     block_after=1 if ending == "cancellation" else None)
    provider = (native_provider(requests, body={"error": "owned provider failure"}, status=500)
                if ending == "create-error" else native_provider(requests, wire=wire))
    spy, seen = native_spy(provider.acompletion)

    async def scenario():
        try:
            create = wrap_model(recorder, spy, instance_id="ollama")
            with recorder.turn("invocation") as turn_a:
                pending = create(MODEL, [{"role": "user", "content": PROMPT}],
                                 stream=ending != "create-error", evidence=evidence)
            with recorder.turn("completion") as turn_b:
                if ending == "create-error":
                    with pytest.raises(AnyLLMError) as caught:
                        await pending
                    native_error = seen["errors"][-1]
                else:
                    stream = await pending
                    await stream.__anext__()
                    check.equal(len(evidence.decisions), 1,
                                msg="A complete tool selection is known before the later native failure boundary.")
                    ledger = seen["iterators"][0]
                    if ending == "midstream-error":
                        with pytest.raises(AnyLLMError) as caught:
                            await stream.__anext__()
                    else:
                        task = asyncio.create_task(stream.__anext__())
                        try:
                            await asyncio.wait_for(wire.entered.wait(), 3)
                            task.cancel("owned native cancellation")
                            with pytest.raises(asyncio.CancelledError) as caught:
                                await task
                        finally:
                            wire.release.set()
                            if not task.done():
                                task.cancel()
                                await asyncio.gather(task, return_exceptions=True)
                    native_error = ledger.errors[-1]
                    await stream.aclose()
                    await stream.aclose()
                check.is_true(caught.value is native_error,
                              msg="The facade must surface the exact error/cancellation object after native AnyLLM processing.")
                if ending != "cancellation":
                    check.is_instance(native_error, AnyLLMError,
                                      msg="The fixture exercises AnyLLM's actual enabled exception conversion, not a recorder relabel.")
                check.equal(recorder.capture_scope().turn_id, turn_b.receipt.turn_id,
                            msg="Terminal capture cannot restore the invocation turn over the host's current context.")
            return turn_a.receipt.turn_id
        finally:
            await provider.client._client.aclose()

    turn_id = run_sdk_async(scenario())
    events, _, _, _ = persisted(recorder, path)
    check.equal(len(requests), 1, msg="Native create/iteration failure does not trigger observer retries.")
    check.equal([event["turn_id"] for event in events], [turn_id] * len(events),
                msg="All observations retain the invocation turn through cross-turn failure and cleanup.")
    check.equal([event["state"] for event in events if event["kind"] == "model.response"],
                ["cancelled" if ending == "cancellation" else "error"],
                msg="Exactly one terminal preserves the native error versus cancellation distinction.")
    check.is_false(events[-1]["details"]["complete"], msg="A native failure cannot be reported as a complete response.")
    decisions = [event for event in events if event["kind"] == "model.tool_call"]
    check.equal(len(decisions), int(ending != "create-error"),
                msg="Pre-stream failure invents no decision; a prior known selection remains admitted.")
    check.is_true(all(event["details"]["complete"] for event in decisions),
                  msg="A later error/cancellation never erases an earlier independently complete decision.")


def test_reused_reference_double_keeps_choice_and_tool_occurrences(recording_factory, run_sdk_async):
    recorder, path = recording_factory()
    choices = [{"index": 2, "delta": {"tool_calls": [tool_delta(0), tool_delta(1)]}, "finish_reason": "stop"},
               {"index": 4, "delta": {"tool_calls": [tool_delta(0)]}, "finish_reason": "tool_calls"}]
    frame = stream_chunk(choices=choices)
    native, evidence = BoundaryIterator([frame]), ModelEvidence()

    async def scenario():
        stream = await wrap_model(recorder, boundary_method(native), instance_id="labelled-double")(
            MODEL, [], stream=True, evidence=evidence)
        check.is_true(await stream.__anext__() is frame, msg="The boundary double's native chunk identity must remain unchanged.")
        check.equal(len(evidence.decisions), 3, msg="All three positional occurrences emit before the terminal chunk is returned.")
        with pytest.raises(StopAsyncIteration):
            await stream.__anext__()
        await stream.aclose()

    run_sdk_async(scenario())
    events, _, _, _ = persisted(recorder, path)
    check.equal([(entry.choice_index, entry.tool_index) for entry in evidence.decisions], [(2, 0), (2, 1), (4, 0)],
                msg="Reused provider references and identical arguments never merge separate choice/tool positions.")
    check.equal([entry.provider_ref for entry in evidence.decisions], ["reused-double-reference"] * 3,
                msg="A reused exposed reference is retained literally rather than rewritten to appear unique.")
    check.equal(len({entry.receipt.decision_id for entry in evidence.decisions}), 3,
                msg="Explicit recorder occurrences remain unique even when all provider references alias.")
    check.equal(events[-1]["state"], "complete", msg="All observed choices have accepted terminal witnesses in this labelled double.")


@pytest.mark.parametrize("finish", ["length", "content_filter"])
def test_unfinished_tool_double_is_not_completed_by_text_terminal(recording_factory, run_sdk_async, finish):
    recorder, path = recording_factory()
    frame = stream_chunk(tools=[tool_delta(0)], finish=finish)
    evidence = ModelEvidence()

    async def scenario():
        stream = await wrap_model(recorder, boundary_method(BoundaryIterator([frame])), instance_id="labelled-double")(
            MODEL, [], stream=True, evidence=evidence)
        await stream.__anext__()
        check.equal(evidence.decisions, (), msg="A text-only terminal reason is not a complete tool-selection witness.")
        with pytest.raises(StopAsyncIteration):
            await stream.__anext__()
        await stream.aclose()

    run_sdk_async(scenario())
    events, _, _, _ = persisted(recorder, path)
    check.equal(len(evidence.decisions), 1, msg="The consumed unfinished key remains represented once at actual EOF.")
    check.is_false(events[1]["details"]["complete"], msg="Incomplete tool execution evidence stays partial for length/filter termination.")
    check.equal(events[-1]["state"], "partial", msg="Actual EOF cannot repair an unfinished tool selection.")


@pytest.mark.parametrize("count", [64, 65], ids=["at-tool-cap", "over-tool-cap"])
def test_native_tool_key_limits(recording_factory, run_sdk_async, count):
    recorder, path = recording_factory()
    frames = [ollama_frame(content="", tools=[("echo", {"x": 7})] * count)]
    wire, requests, evidence = WireBytes(frames), [], ModelEvidence()
    provider = native_provider(requests, wire=wire)

    async def scenario():
        try:
            stream = await wrap_model(recorder, provider.acompletion, instance_id="ollama")(
                MODEL, [{"role": "user", "content": PROMPT}], stream=True, evidence=evidence)
            chunk = await stream.__anext__()
            check.equal(len(chunk.choices[0].delta.tool_calls), count,
                        msg="The actual native conversion yields every tool despite recorder key limits.")
            check.equal([tool.index for tool in chunk.choices[0].delta.tool_calls], list(range(count)),
                        msg="The real outer Ollama generator assigns every occurrence a distinct increasing index.")
            check.equal(len(stream._capture.tools), 64,
                        msg="Only the bounded prefix of positional tool states remains live in the shared accumulator.")
            assert 0 <= stream._capture.chunk_bytes <= 1_048_576
            assert 0 <= stream._capture.fragment_bytes <= 1_048_576
            assert stream._capture.chunk_bytes + stream._capture.fragment_bytes <= 2_097_152
            with pytest.raises(StopAsyncIteration):
                await stream.__anext__()
            await stream.aclose()
        finally:
            await provider.client._client.aclose()

    run_sdk_async(scenario())
    events, _, _, _ = persisted(recorder, path)
    decisions = [event for event in events if event["kind"] == "model.tool_call"]
    check.equal(len(decisions), 64, msg="Each admitted positional tool state emits exactly one bounded decision receipt.")
    check.equal(len(evidence.decisions), 64, msg="The evidence side channel cannot grow beyond the same tool limit.")
    check.equal(events[-1]["state"], "complete" if count == 64 else "partial",
                msg="The exact tool cap qualifies; the first excess key preserves a partial prefix.")
    check.equal("decision_limit" in evidence.losses, count > 64,
                msg="Cardinality overflow reports decision_limit without claiming a provider limit.")
    check.is_true(all(event["details"]["complete"] == (count == 64) for event in decisions),
                  msg="Pending keys become partial at overflow before the unprocessed finish marker.")


@pytest.mark.parametrize("capture", ["content", "metadata"])
def test_native_retention_unresolved_messages_and_repeated_cleanup(recording_factory, run_sdk_async, capture):
    recorder, path = recording_factory(capture=capture)
    secret = "private-model-body-λ"
    messages = [{"role": "tool", "tool_call_id": "unresolved-provider-ref", "content": secret}]
    wire, requests, evidence = WireBytes([ollama_frame(content=secret)]), [], ModelEvidence()
    provider = native_provider(requests, wire=wire)
    spy, seen = native_spy(provider.acompletion)

    async def scenario():
        try:
            stream = await wrap_model(recorder, spy, instance_id="ollama")(
                model=MODEL, messages=messages, stream=True, evidence=evidence)
            result = [chunk async for chunk in stream]
            await stream.aclose()
            await stream.aclose()
            check.equal(result[0].choices[0].delta.content, secret,
                        msg="Both retention policies preserve native output for the host.")
            check.is_true(result[0] is seen["iterators"][0].chunks[0],
                          msg="Metadata retention cannot substitute a redacted chunk for the native object.")
            return [projection(chunk) for chunk in result]
        finally:
            await provider.client._client.aclose()

    result = run_sdk_async(scenario())
    events, artifacts, links, _ = persisted(recorder, path)
    check.equal([event["kind"] for event in events], ["model.request", "model.response"],
                msg="Exhaustion plus repeated cleanup produces one terminal and no fabricated selections.")
    check.equal(events[0]["details"]["unresolved_answers"], [
        {"pointer": "/messages/0/content", "reason": "binding_not_supplied", "provider_ref": "unresolved-provider-ref"}],
        msg="Default tool-message annotations remain explicitly unresolved without reconstructing host identity.")
    check.equal(links, [], msg="A provider reference is never promoted into an answer or insertion link.")
    if capture == "content":
        check.equal(request_body(events[0], artifacts)["messages"], messages,
                    msg="Content capture reconstructs the actual submitted tool-message object.")
        check.equal(events[-1]["body"]["chunks"], result, msg="Content capture retains the consumed native output.")
    else:
        check.is_true(all(artifact["body_text"] is None for artifact in artifacts.values()
                          if artifact["representation"] != "request_refs/2"),
                      msg="Metadata policy omits content bodies, including request templates and chunks; structural manifests remain available.")
        check.is_not_in(secret, json.dumps(artifacts, ensure_ascii=False),
                        msg="Structural manifests and retained artifact metadata cannot conceal omitted content.")
        check.is_not_in(secret, json.dumps(events, ensure_ascii=False),
                        msg="Omitted body text must not leak into control metadata or event details.")
        check.is_not_in(secret, repr(evidence), msg="Evidence side channels store receipts rather than payload copies.")
    check.equal(len(requests), 1, msg="Retention selection and repeated close do not add provider work.")


def test_native_cleanup_after_recorder_shutdown_preserves_bytes(recording_factory, run_sdk_async):
    recorder, path = recording_factory()
    wire = WireBytes([ollama_frame(done=False, finish=None), ollama_frame(content="never-consumed")])
    requests, evidence = [], ModelEvidence()
    provider = native_provider(requests, wire=wire)
    spy, seen = native_spy(provider.acompletion)

    async def scenario():
        try:
            stream = await wrap_model(recorder, spy, instance_id="ollama")(
                MODEL, [{"role": "user", "content": PROMPT}], stream=True, evidence=evidence)
            await stream.__anext__()
            closed = recorder.close(timeout=5)
            assert closed.portable and closed.writer_stopped, (
                "The late-cleanup control must finalize its own recording before comparing immutable bytes."
            )
            before = hashlib.sha256(path.read_bytes()).hexdigest()
            await stream.aclose()
            await stream.aclose()
            check.equal(hashlib.sha256(path.read_bytes()).hexdigest(), before,
                        msg="Post-shutdown native cleanup cannot write to the finalized recording.")
            check.equal(evidence.response.status, "closed",
                        msg="The terminal attempt honestly reports closed recorder admission rather than a fabricated stored receipt.")
            check.equal(seen["iterators"][0].closes, 2,
                        msg="Explicit native cleanup still runs under recorder shutdown; the recorder does not own lifecycle.")
            check.equal((stream._capture.chunks, stream._capture.tools, stream._capture.choices), ([], {}, {}),
                        msg="The observer releases its bounded prefix even when the recorder declines the terminal.")
        finally:
            await provider.client._client.aclose()

    run_sdk_async(scenario())
    events, _, _, _ = persisted(recorder, path)
    check.equal(wire.reads, [0], msg="Closing after recorder shutdown never drains the unconsumed native suffix.")
    check.equal([event["kind"] for event in events], ["model.request"],
                msg="A finalized recorder cannot gain a late model response.")


class UncloseableIterator:
    """Labelled unsupported-return double whose iteration must never be entered."""
    def __init__(self):
        self.reads = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        self.reads += 1
        raise StopAsyncIteration


class BrokenCleanupProperty(UncloseableIterator):
    @property
    def aclose(self):
        raise ValueError("labelled unsupported cleanup property")


@pytest.mark.parametrize("kind", ["object", "no-aclose", "broken-aclose"])
def test_unexpected_return_double_is_passed_through_without_iteration(recording_factory, run_sdk_async, kind):
    recorder, path = recording_factory()
    native = {"object": object, "no-aclose": UncloseableIterator, "broken-aclose": BrokenCleanupProperty}[kind]()
    evidence = ModelEvidence()

    async def scenario():
        result = await wrap_model(recorder, boundary_method(native), instance_id="labelled-return")(
            MODEL, [], evidence=evidence)
        check.is_true(result is native, msg="An unsupported return is preserved by identity rather than coerced into a stream.")

    run_sdk_async(scenario())
    events, artifacts, _, _ = persisted(recorder, path)
    check.equal(getattr(native, "reads", 0), 0, msg="Unsupported iterable returns must not be inspected through consumption.")
    check.equal(evidence.losses, ("projection_failed",), msg="The unsupported result uses the existing projection-failure path.")
    check.is_none(events[-1]["body"], msg="An unprojectable result has no fabricated response body.")
    check.equal(artifacts[events[-1]["payload_ref"]]["omitted_reason"], "unexposed",
                msg="MISSING output retains the core's explicit omitted artifact and unexposed reason.")
    check.equal(events[-1]["state"], "partial", msg="Native normal return does not imply complete evidence projection.")


def test_projection_gap_double_keeps_known_decision_and_partial_prefix(recording_factory, run_sdk_async):
    recorder, path = recording_factory()
    first = stream_chunk(tools=[tool_delta(0)], finish="stop")
    malformed = object()
    last = stream_chunk(content="later", finish="stop")
    native, evidence = BoundaryIterator([first, malformed, last]), ModelEvidence()

    async def scenario():
        stream = await wrap_model(recorder, boundary_method(native), instance_id="labelled-projection")(
            MODEL, [], stream=True, evidence=evidence)
        for item in (first, malformed, last):
            check.is_true(await stream.__anext__() is item,
                          msg="Projection failure must still yield the original native object.")
        with pytest.raises(StopAsyncIteration):
            await stream.__anext__()
        await stream.aclose()

    run_sdk_async(scenario())
    events, _, _, _ = persisted(recorder, path)
    check.equal(len(evidence.decisions), 1, msg="A projection gap does not invent or duplicate tool selections.")
    check.is_true(events[1]["details"]["complete"], msg="A selection finalized before a later projection failure stays complete.")
    check.equal(events[-1]["body"]["chunks"], [projection(first), projection(last)],
                msg="Only trusted projections are retained, with the omitted malformed chunk disclosed as a loss.")
    check.equal(events[-1]["state"], "partial", msg="A missing chunk projection prevents a false complete-stream claim.")
    check.is_in("projection_failed", evidence.losses, msg="The gap is explicitly reported by the shared bounded diagnostic.")


@pytest.mark.parametrize("gap", [False, True], ids=["valid-neighbor", "missing-selection-prefix"])
def test_projection_gap_double_taints_new_keys_but_preserves_prior_selection(
        recording_factory, run_sdk_async, monkeypatch, gap):
    recorder, path = recording_factory()
    known = stream_chunk(tools=[tool_delta(0, reference="known")], finish="stop")
    missing = stream_chunk(choice=1, tools=[tool_delta(0, reference="prefix-", name="prefix-", arguments=" ")])
    final = stream_chunk(choice=1, tools=[tool_delta(0, reference="suffix")], finish="stop")
    frames = [known, missing, final]
    original = type(missing).to_dict

    def projected(self, **kwargs):
        if gap and self is missing:
            raise ValueError("labelled unavailable consumed projection hides a possible tool prefix")
        return original(self, **kwargs)

    monkeypatch.setattr(type(missing), "to_dict", projected)
    native, evidence = BoundaryIterator(frames), ModelEvidence()

    async def scenario():
        stream = await wrap_model(recorder, boundary_method(native), instance_id="labelled-projection")(
            MODEL, [], stream=True, evidence=evidence)
        for frame in frames:
            check.is_true(await stream.__anext__() is frame,
                          msg="An unavailable capture projection still yields the exact native chunk to the host.")
        check.equal(len(evidence.decisions), 2,
                    msg="Both exposed positional keys have one receipt before actual EOF, even if one is partial.")
        with pytest.raises(StopAsyncIteration):
            await stream.__anext__()
        await stream.aclose()

    run_sdk_async(scenario())
    events, _, _, _ = persisted(recorder, path)
    decisions = [event for event in events if event["kind"] == "model.tool_call"]
    check.equal([event["details"]["complete"] for event in decisions], [True, not gap],
                msg="A stream gap preserves an already completed selection but taints keys first observed after the gap.")
    check.equal([event["details"]["args_digest"]["value"] is not None for event in decisions], [True, not gap],
                msg="A syntactically valid visible argument suffix cannot masquerade as the complete missing-prefix argument.")
    check.equal(events[-1]["body"]["chunks"], [projection(known), projection(final)] if gap else [projection(frame) for frame in frames],
                msg="The retained normalized output discloses a gap rather than reconstructing its missing chunk.")
    check.equal(events[-1]["state"], "partial" if gap else "complete",
                msg="Only the no-gap valid neighbor has full stream evidence.")
