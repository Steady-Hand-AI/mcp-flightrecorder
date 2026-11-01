"""OpenAI contracts through the installed SDK and an explicitly scripted peer."""
import asyncio
from contextlib import closing
from functools import wraps
import gc
import hashlib
import inspect
import json
import logging
import sqlite3
import threading
import weakref

import httpx2
from openai import AsyncOpenAI, omit
from openai.types.chat import ChatCompletion, ChatCompletionChunk
import pytest
import pytest_check as check

from mcp_flightrecorder import Recorder
from mcp_flightrecorder.integrations.openai_chat import ModelEvidence, wrap_model


COVERAGE = {"producers": {"model": "test.model", "host": "test.host", "mcp": "test.mcp"}}
TOOLS = [{"type": "function", "function": {"name": "echo", "description": "Echo text.",
          "parameters": {"type": "object", "properties": {"text": {"type": "string"}}}}}]


@pytest.fixture
def recording_factory(tmp_path):
    owned = []

    def create(name="model", **settings):
        path = tmp_path / (name + ".sqlite")
        rec = Recorder(path, capture=settings.pop("capture", "content"), coverage=COVERAGE, **settings)
        owned.append(rec)
        return rec, path

    yield create
    for rec in owned:
        closed = rec.close(timeout=3)
        assert closed.writer_stopped, "Each SDK test must join its own recorder writer before its temporary files are removed."


def accepted(receipt):
    assert receipt is not None and receipt.status == "accepted", (
        "Persisted identity assertions require an admitted public receipt rather than an assumed event."
    )
    return receipt


def persisted(rec, path):
    closed = rec.close(timeout=3)
    assert closed.writer_stopped and closed.portable, "Database assertions require joined ownership and a portable main file."
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        db.row_factory = sqlite3.Row
        rows = [dict(row) for row in db.execute("SELECT * FROM events ORDER BY sequence")]
        artifacts = {row["artifact_id"]: dict(row) for row in db.execute("SELECT * FROM artifacts")}
        links = [dict(row) for row in db.execute("SELECT * FROM links")]
    for row in rows:
        row["details"] = json.loads(row["details_json"])
        body = artifacts[row["payload_ref"]]["body_text"] if row["payload_ref"] is not None else None
        row["body"] = None if body is None else json.loads(body)
    return rows, artifacts, links


def request_body(row, artifacts):
    """Resolve only the documented request manifest, independently of product helpers."""
    manifest = row["body"]
    assert manifest is not None and manifest.get("format") == "request_refs/2", (
        "A complete ordinary request must retain its structural request_refs/2 manifest."
    )
    body = json.loads(artifacts[manifest["template_ref"]]["body_text"])
    check.equal(manifest["message_path"], "/messages", msg="The model adapter extracts the submitted messages array at its assigned path.")
    body["messages"] = [json.loads(artifacts[identity]["body_text"]) for identity in manifest["message_refs"]]
    for field in manifest["field_refs"]:
        check.equal(field["pointer"], "/tools", msg="The selected model adapter's only extracted catalog field is /tools.")
        body["tools"] = json.loads(artifacts[field["artifact_id"]]["body_text"])
    return body


def request_artifacts(row, artifacts):
    """Resolve the split request content separately from its structural manifest."""
    manifest = row["body"]
    assert manifest["format"] == "request_refs/2" and manifest["message_path"] == "/messages"
    fields = manifest["field_refs"]
    assert [field["pointer"] for field in fields] == ["/tools"]
    assert row["catalog_ref"] == fields[0]["artifact_id"]
    return [artifacts[manifest["template_ref"]],
            *[artifacts[key] for key in manifest["message_refs"]],
            artifacts[fields[0]["artifact_id"]]]


def retained_artifact(artifact, original, stored, policy, *, redacted=False,
                      truncated=False, omitted=None):
    """Compare storage to independent expected UTF-8 bytes, never product encoders."""
    assert artifact["observed_bytes"] == len(original)
    assert artifact["body_text"] == (None if stored is None else stored.decode("utf-8"))
    assert artifact["stored_bytes"] == (0 if stored is None else len(stored))
    assert artifact["stored_sha256"] == (None if stored is None else hashlib.sha256(stored).hexdigest())
    assert (artifact["policy_id"], artifact["redacted"], artifact["truncated"], artifact["omitted_reason"]) == (
        policy, int(redacted), int(truncated), omitted)


def normal(*, tools=None, finish="stop", choice=0, content="observed", **extra):
    message = {"role": "assistant", "content": content}
    if tools is not None:
        message["tool_calls"] = tools
    return {"id": "completion", "object": "chat.completion", "created": 1,
            "model": "fixture-model", "choices": [{"index": choice, "message": message,
            "finish_reason": finish}], **extra}


def function_call(raw='{"text":"selected"}', name="echo", reference="same-provider-id"):
    return {"id": reference, "type": "function", "function": {"name": name, "arguments": raw}}


def chunk(choices=None, **extra):
    return {"id": "stream", "object": "chat.completion.chunk", "created": 1,
            "model": "fixture-model", "choices": [] if choices is None else choices, **extra}


def delta(*, index=0, tools=None, finish=None, **values):
    if tools is not None:
        values["tool_calls"] = tools
    return {"index": index, "delta": values, "finish_reason": finish}


def native_client(handler, *, retries=0):
    return AsyncOpenAI(api_key="fixture-only-not-a-secret", base_url="https://fixture.invalid/v1",
                       max_retries=retries, http_client=httpx2.AsyncClient(
                           transport=httpx2.MockTransport(handler), trust_env=False))


def observed_method(native):
    """Pass-through spy: every result/error comes from the actual SDK method."""
    seen = {"kwargs": [], "results": [], "errors": []}

    @wraps(native)
    async def call(**kwargs):
        seen["kwargs"].append(kwargs)
        try:
            result = await native(**kwargs)
        except BaseException as error:
            seen["errors"].append(error)
            raise
        seen["results"].append(result)
        return result

    return call, seen


@pytest.mark.parametrize("label", [None, 1, "", "x" * 257, "é" * 129, "\ud800"],
                         ids=["null", "type", "empty", "ascii-bound", "utf8-bound", "encoding"])
def test_model_labels_rejected_before_native_use(recording_factory, run_sdk_async, label):
    rec, path = recording_factory()
    requests = []

    async def run():
        async with native_client(lambda request: requests.append(request) or httpx2.Response(200, json=normal())) as client:
            # Invalid recorder-only configuration must fail during construction.
            with pytest.raises(ValueError):
                wrap_model(rec, client.chat.completions.create, instance_id=label)

    run_sdk_async(run())
    rows, _, _ = persisted(rec, path)
    check.equal(requests, [], msg="Rejected wrapper labels cannot initiate an SDK transport request.")
    check.equal(rows, [], msg="Rejected wrapper labels cannot allocate a recorder event.")


def test_factory_binding_evidence_ownership_and_never_awaited(recording_factory, run_sdk_async):
    rec, path = recording_factory()
    requests, policy_calls = [], []

    def policy(kind, payload, context):
        policy_calls.append(kind)
        return {"policy_id": "lazy", "retained": payload}

    async def run():
        async with native_client(lambda request: requests.append(request) or httpx2.Response(200, json=normal())) as client:
            native = client.chat.completions.create
            create = wrap_model(rec, native, instance_id="é" * 128, retention_policy=policy)
            check.equal(inspect.signature(create), inspect.signature(native), msg="The decorated factory exposes the pinned native keyword-only signature through wraps.")
            for kwargs in ({"model": "fixture-model"}, {"model": "fixture-model", "messages": [], "unexpected": True}):
                # Both paths reject native argument misuse before returning a coroutine.
                with pytest.raises(TypeError):
                    native(**kwargs)
                with pytest.raises(TypeError):
                    create(**kwargs)
            # Evidence type is a declared instrumentation misuse, also at invocation.
            with pytest.raises(TypeError):
                create(model="fixture-model", messages=[], evidence=object())
            evidence = ModelEvidence()
            abandoned = create(model="fixture-model", messages=[], evidence=evidence)
            check.is_true(inspect.iscoroutine(abandoned), msg="A valid invocation returns an unscheduled Python coroutine.")
            check.is_none(evidence.request, msg="A never-executed coroutine has no request receipt.")
            check.equal(evidence.decisions, (), msg="A never-executed coroutine has no decision occurrences.")
            check.equal(rec.health().ordinary.used_slots, 0, msg="Factory construction cannot reserve ordinary recording capacity.")
            check.equal(rec.health().completion.used_slots, 0, msg="Factory construction cannot reserve completion capacity.")
            with pytest.raises(ValueError):
                create(model="fixture-model", messages=[], evidence=evidence)
            assert policy_calls == [], "Never-awaited work cannot run the configured retention callback."
            abandoned.close()
            check.equal(requests, [], msg="Closing an unexecuted wrapper coroutine must not construct or call native SDK work.")
            await create(model="fixture-model", messages=[])
            check.equal(len(requests), 1, msg="Omitting evidence still executes and captures one native method invocation.")

    run_sdk_async(run())
    rows, _, _ = persisted(rec, path)
    assert policy_calls == ["model.request", "model.response"]
    check.equal([row["kind"] for row in rows], ["model.request", "model.response"],
                msg="Only the executed evidence-omitted call contributes request and terminal events.")


def test_normal_raw_fields_transport_exclusion_and_recorder_only_arguments(recording_factory, run_sdk_async):
    rec, path = recording_factory()
    raw = '{ "text" : "selected" }'
    output = normal(tools=[function_call(raw)], finish="tool_calls", content=None,
                    usage={"prompt_tokens": 2, "completion_tokens": 3, "total_tokens": 5},
                    future_json={"kept": [False, None, 0]})
    evidence, requests = ModelEvidence(), []

    async def handler(request):
        requests.append(json.loads(request.content))
        return httpx2.Response(200, json=output)

    async def run():
        async with native_client(handler) as client:
            spy, seen = observed_method(client.chat.completions.create)
            create = wrap_model(rec, spy, instance_id="model")
            result = await create(model="fixture-model", messages=[{"role": "user", "content": "input"}],
                                  tools=TOOLS, temperature=0, user="explicit", store=False,
                                  max_tokens=omit, extra_body={}, extra_headers={"x-fixture": "header"},
                                  extra_query={"probe": "query"}, timeout=4,
                                  annotations=[{"bindings": [], "unresolved": [], "complete": True}],
                                  insertions=(), evidence=evidence)
            check.is_true(result is seen["results"][0], msg="Nonstream observation returns the exact native ChatCompletion object.")
            check.is_false(any(key in seen["kwargs"][0] for key in ("evidence", "annotations", "insertions")),
                           msg="Recorder-only options must never reach the pinned SDK method.")
            check.equal(result.to_dict(mode="json", use_api_names=True, exclude_unset=True), output,
                        msg="SDK projection preserves exposed usage, raw arguments and benign unknown JSON fields.")
            check.equal(evidence.losses, (), msg="A fully observed function response with benign extras has no adapter loss.")
            check.equal([(item.choice_index, item.tool_index, item.provider_ref) for item in evidence.decisions],
                        [(0, 0, "same-provider-id")], msg="Normal decision receipts identify exposed choice and tool-list position.")
            check.equal(accepted(evidence.response).model_call_id, accepted(evidence.request).model_call_id,
                        msg="The terminal receipt belongs to this call's admitted request occurrence.")

    run_sdk_async(run())
    rows, artifacts, _ = persisted(rec, path)
    check.equal([row["kind"] for row in rows], ["model.request", "model.tool_call", "model.response"],
                msg="A normal tool response orders request, raw decision, then one terminal.")
    captured = request_body(rows[0], artifacts)
    check.equal(captured, {"model": "fixture-model", "messages": [{"role": "user", "content": "input"}],
                          "tools": TOOLS, "temperature": 0, "user": "explicit", "store": False},
                msg="Capture contains only explicitly submitted JSON body options, with omission sentinels and transport options excluded.")
    check.equal(rows[1]["body"], {"arguments_raw": raw}, msg="The decision preserves exact raw spacing without inventing parsed arguments.")
    check.equal(rows[2]["body"], output, msg="The retained normal response is the native exposed JSON, including unknown fields.")
    check.equal(len(requests), 1, msg="Instrumentation contributes no additional native request.")


def test_execution_validation_error_keeps_native_stage_and_object(recording_factory, run_sdk_async):
    rec, path = recording_factory()
    evidence, requests = ModelEvidence(), []

    async def run():
        async with native_client(lambda request: requests.append(request) or httpx2.Response(200, json=normal())) as client:
            spy, seen = observed_method(client.chat.completions.create)
            create = wrap_model(rec, spy, instance_id="model")
            pending = create(model="fixture-model", messages=[], response_format=ChatCompletion, evidence=evidence)
            check.is_true(inspect.iscoroutine(pending), msg="SDK response-format validation is deferred until coroutine execution.")
            check.is_none(evidence.request, msg="Execution-time SDK validation must not be moved into factory invocation.")
            # The real SDK rejects a BaseModel class passed to create rather than parse.
            with pytest.raises(TypeError) as caught:
                await pending
            check.is_true(caught.value is seen["errors"][0], msg="The wrapper propagates the SDK's exact validation exception object.")
            check.is_in("request_unavailable", evidence.losses, msg="An SDK-specific non-JSON response-format object cannot be claimed as captured JSON.")

    run_sdk_async(run())
    rows, _, _ = persisted(rec, path)
    check.equal(requests, [], msg="The actual SDK validation failure occurs before transport work.")
    check.equal([row["state"] for row in rows if row["kind"] == "model.response"], ["error"],
                msg="An executed native validation failure closes its observed request once as error.")


@pytest.mark.parametrize("annotations", [object(), [{"unknown": True}]], ids=["non-json", "wrong-contract"])
def test_invalid_annotations_do_not_prevent_native_success(recording_factory, run_sdk_async, annotations):
    rec, path = recording_factory()
    evidence = ModelEvidence()

    async def run():
        async with native_client(lambda request: httpx2.Response(200, json=normal())) as client:
            spy, seen = observed_method(client.chat.completions.create)
            result = await wrap_model(rec, spy, instance_id="model")(
                model="fixture-model", messages=[{"role": "user", "content": "input"}],
                annotations=annotations, evidence=evidence)
            check.is_true(result is seen["results"][0], msg="Invalid capture annotations cannot replace a valid native result.")
            check.is_true(bool(evidence.losses), msg="Invalid annotation capture must remain visible as a bounded adapter loss.")

    run_sdk_async(run())
    rows, _, _ = persisted(rec, path)
    terminal = [row for row in rows if row["kind"] == "model.response"]
    assert len(terminal) == 1, "Even unavailable request annotations must leave one admitted terminal when the recorder remains available."
    check.equal(terminal[0]["state"], "partial", msg="Annotation loss cannot leave a falsely complete model observation.")


def test_default_tool_annotation_is_unresolved_without_provider_id_matching(recording_factory, run_sdk_async):
    rec, path = recording_factory()

    async def run():
        async with native_client(lambda request: httpx2.Response(200, json=normal())) as client:
            await wrap_model(rec, client.chat.completions.create, instance_id="model")(
                model="fixture-model", messages=[{"role": "user", "content": "input"},
                {"role": "tool", "tool_call_id": "reused", "content": "unattributed"}])

    run_sdk_async(run())
    rows, _, links = persisted(rec, path)
    unresolved = rows[0]["details"]["unresolved_answers"]
    check.equal(unresolved, [{"pointer": "/messages/1/content", "reason": "binding_not_supplied", "provider_ref": "reused"}],
                msg="An unannotated tool result retains its usable provider reference only as an unresolved diagnostic.")
    check.equal(links, [], msg="A repeated provider ID alone must never manufacture an answers relation.")


def test_scope_snapshot_execution_freeze_and_annotation_detachment(recording_factory, run_sdk_async):
    rec, path = recording_factory()
    evidence = ModelEvidence()
    messages = [{"role": "user", "content": "factory"}]
    tools = json.loads(json.dumps(TOOLS))
    annotations = [{"bindings": [], "unresolved": [], "complete": True}]
    policy_calls = []

    def policy(kind, payload, context):
        policy_calls.append((kind, json.loads(json.dumps(payload)), dict(context)))
        return {"policy_id": "scope", "retained": payload}

    async def run():
        entered, release = asyncio.Event(), asyncio.Event()

        async def handler(request):
            entered.set()
            await release.wait()
            return httpx2.Response(200, json=normal())

        async with native_client(handler) as client:
            create = wrap_model(rec, client.chat.completions.create, instance_id="model", retention_policy=policy)
            with rec.turn("A") as turn_a:
                with rec.dispatch(action="execute", name="echo", arguments={}) as dispatch_a:
                    pending = create(model="fixture-model", messages=messages, tools=tools,
                                     annotations=annotations, evidence=evidence)
            assert policy_calls == [], "Invocation snapshots scope without executing policy or projecting the body."
            messages[0]["content"] = "execution"
            tools[0]["function"]["description"] = "execution catalog"
            with rec.turn("B") as turn_b:
                task = asyncio.create_task(pending)
                try:
                    await asyncio.wait_for(entered.wait(), 2)
                    assert [kind for kind, _, _ in policy_calls] == ["model.request"]
                    assert policy_calls[0][1]["messages"][0]["content"] == "execution"
                    check.is_none(evidence.response, msg="Exiting the invocation scope does not complete outstanding native work.")
                    messages[0]["content"] = "after submission"
                    tools[0]["function"]["description"] = "after submission"
                    annotations[0]["complete"] = False
                    check.equal(rec.capture_scope().turn_id, turn_b.receipt.turn_id,
                                msg="Frozen attribution cannot overwrite the host's later ambient turn.")
                finally:
                    release.set()
                await task
                check.equal(rec.capture_scope().turn_id, turn_b.receipt.turn_id,
                            msg="Native completion must not restore a copied invocation Context over the host's current scope.")
            return turn_a.receipt.turn_id, dispatch_a.receipt.event_id

    turn_id, dispatch_id = run_sdk_async(run())
    rows, artifacts, _ = persisted(rec, path)
    assert [kind for kind, _, _ in policy_calls] == ["model.request", "model.response"]
    assert policy_calls[0][1]["tools"][0]["function"]["description"] == "execution catalog"
    model_rows = [row for row in rows if row["boundary"] == "model"]
    check.equal([row["turn_id"] for row in model_rows], [turn_id, turn_id],
                msg="Request and terminal keep invocation turn A despite execution and completion under turn B.")
    check.equal(rows[0]["event_id"], dispatch_id, msg="The host dispatch remains its original explicit event; model capture invents no dispatch.")
    body = request_body(model_rows[0], artifacts)
    check.equal(body["messages"][0]["content"], "execution", msg="Mutation before execution is observed, while mutation after submission is detached.")
    check.equal(body["tools"][0]["function"]["description"], "execution catalog", msg="Each execution freezes its currently submitted catalog independently.")
    check.is_true(model_rows[0]["details"]["complete"], msg="Mutation of annotations after observation cannot turn a frozen complete request partial.")


def test_rejected_inner_dispatch_never_recovers_outer_scope(recording_factory, run_sdk_async, monkeypatch):
    rec, path = recording_factory()
    evidence = ModelEvidence()
    scopes = []
    original = rec.observe

    def observe(*args, **kwargs):
        scopes.append(kwargs["scope"])
        return original(*args, **kwargs)

    monkeypatch.setattr(rec, "observe", observe)

    async def run():
        async with native_client(lambda request: httpx2.Response(200, json=normal())) as client:
            create = wrap_model(rec, client.chat.completions.create, instance_id="model")
            with rec.turn("turn") as turn:
                with rec.dispatch(action="execute", name="outer", arguments={}):
                    with rec.dispatch(action="invalid", name="inner", arguments={}) as inner:
                        check.equal(inner.receipt.status, "rejected", msg="The inner dispatch must actually be rejected before outer-fallback behavior is tested.")
                        pending = create(model="fixture-model", messages=[], evidence=evidence)
            result = await pending
            check.is_true(isinstance(result, ChatCompletion), msg="A rejected dispatch cannot prevent valid native execution.")
            return turn.receipt.turn_id

    turn_id = run_sdk_async(run())
    rows, _, _ = persisted(rec, path)
    check.equal([row["kind"] for row in rows], ["host.dispatch", "model.request", "model.response"],
                msg="A rejected inner dispatch clears only dispatch attribution, leaving the valid turn available for model observations.")
    check.equal([(scope.turn_id, scope.dispatch_event_id) for scope in scopes], [(turn_id, None)] * 2,
                msg="Request and terminal preserve the inner null-dispatch snapshot and never recover the accepted outer dispatch.")
    check.equal(evidence.losses, (), msg="A cleared dispatch does not disable an otherwise valid model capture scope.")


@pytest.mark.parametrize("source", ["messages", "tools", "extra_body", "legacy"],
                         ids=["one-shot-messages", "one-shot-tools", "overriding-extra-body", "legacy-functions"])
def test_unavailable_request_inputs_preserve_native_consumption(recording_factory, run_sdk_async, source):
    rec, path = recording_factory()
    observed, consumed, policy_calls = [], [], []

    def policy(kind, payload, context):
        policy_calls.append(kind)
        return {"policy_id": "available-only", "retained": payload}

    def one_shot(label, values):
        for value in values:
            consumed.append(label)
            yield value

    async def run_one(client, decorated):
        options = {"model": "fixture-model", "messages": [{"role": "user", "content": "original"}]}
        if source == "messages":
            options["messages"] = one_shot("wrapped" if decorated else "native", options["messages"])
        elif source == "tools":
            options["tools"] = one_shot("wrapped" if decorated else "native", TOOLS)
        elif source == "extra_body":
            options["extra_body"] = {"messages": [{"role": "user", "content": "overridden"}]}
        else:
            options["functions"] = [TOOLS[0]["function"]]
        evidence = ModelEvidence()
        method = wrap_model(rec, client.chat.completions.create, instance_id="model", retention_policy=policy) if decorated else client.chat.completions.create
        result = await method(**options, **({"evidence": evidence} if decorated else {}))
        return result, evidence

    async def run():
        async def handler(request):
            observed.append(json.loads(request.content))
            return httpx2.Response(200, json=normal())

        async with native_client(handler) as client:
            first, _ = await run_one(client, False)
            second, evidence = await run_one(client, True)
            check.equal(first.to_dict(), second.to_dict(), msg="Declining capture for unsupported input shapes preserves the SDK's actual result.")
            check.is_in("request_unavailable", evidence.losses, msg="The adapter must not claim a complete request reconstructed by consuming one-shot or overriding inputs.")

    run_sdk_async(run())
    rows, _, _ = persisted(rec, path)
    assert policy_calls == ["model.response"], "A MISSING submitted projection never reaches the policy."
    check.equal(observed[0], observed[1], msg="Native and decorated calls submit equal effective bodies without observer serialization changing SDK consumption.")
    if source in ("messages", "tools"):
        check.equal(consumed, ["native", "wrapped"], msg="Only the actual SDK consumes each one-shot input once.")
    check.is_none(rows[0]["body"], msg="Unsupported request capture stores MISSING rather than a guessed or partially drained body.")
    check.is_false(rows[0]["details"]["complete"], msg="Unavailable submitted inputs are persisted as explicitly incomplete.")
    check.equal(rows[-1]["state"], "partial", msg="A successful SDK result cannot erase an earlier request observation gap.")


@pytest.mark.parametrize("mode", ["native", "enabled", "closed", "failed"])
@pytest.mark.parametrize("outcome", ["return", "error", "cancel"])
def test_recorder_state_preserves_native_result_error_and_cancellation(recording_factory, run_sdk_async,
                                                                      monkeypatch, mode, outcome):
    rec, path = recording_factory()
    if mode == "closed":
        rec.close(timeout=3)
    if mode == "failed":
        def fail_write(connection, bundle):
            raise sqlite3.OperationalError("controlled writer failure")
        monkeypatch.setattr(rec, "_persist_bundle", fail_write)
        rec.observe("model.response", scope=rec.capture_scope(), payload={})
        rec._writer_thread.join(timeout=2)
        assert rec.health().state == "failed", "The failed-recorder control requires a real writer failure before the SDK call."
    requests, evidence, policy_calls = [], ModelEvidence(), []

    def policy(kind, payload, context):
        policy_calls.append(kind)
        raise ValueError("contained policy fault")

    async def run():
        entered, release = asyncio.Event(), asyncio.Event()

        async def handler(request):
            requests.append(request)
            entered.set()
            if outcome == "cancel":
                await release.wait()
            if outcome == "error":
                return httpx2.Response(400, json={"error": {"message": "fixture error", "type": "invalid_request_error"}})
            return httpx2.Response(200, json=normal())

        async with native_client(handler) as client:
            spy, seen = observed_method(client.chat.completions.create)
            method = spy if mode == "native" else wrap_model(rec, spy, instance_id="model", retention_policy=policy)
            task = asyncio.create_task(method(model="fixture-model", messages=[],
                                              **({} if mode == "native" else {"evidence": evidence})))
            await asyncio.wait_for(entered.wait(), 2)
            if outcome == "cancel":
                task.cancel("host cancellation token")
            try:
                result = await task
            except BaseException as error:
                assert seen["errors"], "Error identity checks require the pass-through spy to observe the actual SDK exception."
                check.is_true(error is seen["errors"][0], msg="Instrumentation must propagate the exact SDK exception or cancellation object.")
                check.equal(isinstance(error, asyncio.CancelledError), outcome == "cancel", msg="Capture state cannot relabel a host error as cancellation or suppress a cancellation.")
            else:
                check.equal(outcome, "return", msg="Only the native successful branch may return normally.")
                check.is_true(result is seen["results"][0], msg="Enabled, stopped and failed recorders all preserve the exact native returned object.")
            finally:
                release.set()
            check.is_false(client.is_closed(), msg="Capture-only failure cannot close the host-owned native client.")
            check.equal(len(requests), 1, msg="Recorder state cannot add a retry or duplicate SDK request.")
            if mode in ("closed", "failed"):
                check.is_in("capture_rejected", evidence.losses, msg="A stopped or failed recorder exposes capture loss without changing native work.")

    run_sdk_async(run())
    expected_policy_calls = ["model.request", "model.response"] if outcome == "return" else ["model.request"]
    assert policy_calls == ([] if mode == "native" else expected_policy_calls)
    if mode != "native":
        assert "retention_policy_failed" in evidence.losses
    if mode == "enabled":
        rows, _, _ = persisted(rec, path)
        check.equal([row["kind"] for row in rows], ["model.request", "model.response"], msg="Each executed normal SDK outcome closes its captured request with exactly one terminal.")
        check.equal(rows[-1]["state"], {"return": "complete", "error": "error", "cancel": "cancelled"}[outcome],
                    msg="Terminal capture distinguishes success, native failure and native cancellation.")


def test_native_projection_failure_remains_partial_without_replacing_result(recording_factory, run_sdk_async, monkeypatch):
    rec, path = recording_factory()
    evidence, policy_calls = ModelEvidence(), []

    def policy(kind, payload, context):
        policy_calls.append(kind)
        return {"policy_id": "available-only", "retained": payload}

    def unavailable_projection(self, **kwargs):
        raise ValueError("controlled trusted-model projection failure")

    monkeypatch.setattr(ChatCompletion, "to_dict", unavailable_projection)

    async def run():
        async with native_client(lambda request: httpx2.Response(200, json=normal())) as client:
            spy, seen = observed_method(client.chat.completions.create)
            result = await wrap_model(rec, spy, instance_id="model", retention_policy=policy)(model="fixture-model", messages=[], evidence=evidence)
            check.is_true(result is seen["results"][0], msg="A trusted model projection failure cannot replace the native response.")
            check.equal(result.choices[0].message.content, "observed", msg="The host still receives the native response's usable fields.")

    run_sdk_async(run())
    rows, _, _ = persisted(rec, path)
    assert policy_calls == ["model.request"], "Failed response projection remains MISSING rather than policy-visible JSON null."
    check.is_in("projection_failed", evidence.losses, msg="Failed trusted projection is visible in the bounded side channel.")
    check.is_none(rows[-1]["body"], msg="A failed projection stores MISSING instead of invoking an alternate serializer.")
    check.equal(rows[-1]["state"], "partial", msg="A missing normal response projection cannot be claimed as complete.")


def test_capture_losses_log_without_evidence_or_private_content(
        recording_factory, run_sdk_async, monkeypatch, caplog):
    rec, path = recording_factory()
    secret = "private-model-input-and-projection-error"

    def unavailable_projection(self, **kwargs):
        raise ValueError(secret)

    monkeypatch.setattr(ChatCompletion, "to_dict", unavailable_projection)

    async def run():
        async with native_client(lambda request: httpx2.Response(200, json=normal(content=secret))) as client:
            spy, seen = observed_method(client.chat.completions.create)
            returned = await wrap_model(rec, spy, instance_id=secret)(
                model="fixture-model", messages=[{"role": "user", "content": secret}],
                extra_body={"private_value": secret},
            )
            assert returned is seen["results"][0]
            assert returned.choices[0].message.content == secret
            assert len(seen["kwargs"]) == 1

    with caplog.at_level(logging.WARNING, logger="mcp_flightrecorder.integrations"):
        run_sdk_async(run())
    records = [record for record in caplog.records
               if record.name.startswith("mcp_flightrecorder.integrations.")]
    assert [record.getMessage() for record in records] == [
        "integration_capture_loss reason=request_unavailable",
        "integration_capture_loss reason=projection_failed",
    ]
    assert all(record.levelno == logging.WARNING and record.exc_info is None
               and record.stack_info is None and secret not in repr(record.__dict__)
               for record in records)
    rows, _, _ = persisted(rec, path)
    assert [row["kind"] for row in rows] == ["model.request", "model.response"]
    assert rows[-1]["state"] == "partial" and rows[-1]["body"] is None


@pytest.mark.parametrize("outcome", ["return", "error", "cancel"])
def test_failing_log_handler_preserves_native_outcome(
        recording_factory, run_sdk_async, monkeypatch, caplog, outcome):
    rec, _ = recording_factory()
    attempts = []
    observe = rec.observe

    def refuse_terminal(kind, *args, **kwargs):
        if kind == "model.response":
            raise RuntimeError("controlled terminal capture failure")
        return observe(kind, *args, **kwargs)

    monkeypatch.setattr(rec, "observe", refuse_terminal)

    class BrokenHandler(logging.Handler):
        def emit(self, record):
            attempts.append(record.getMessage())
            raise RuntimeError("application logging failed")

    async def run():
        entered, release = asyncio.Event(), asyncio.Event()

        async def handler(request):
            entered.set()
            if outcome == "cancel":
                await release.wait()
            if outcome == "error":
                return httpx2.Response(400, json={"error": {"message": "native failure", "type": "invalid_request_error"}})
            return httpx2.Response(200, json=normal())

        async with native_client(handler) as client:
            spy, seen = observed_method(client.chat.completions.create)
            create = wrap_model(rec, spy, instance_id="model")
            task = asyncio.create_task(create(model="fixture-model", messages=[]))
            try:
                await asyncio.wait_for(entered.wait(), 2)
                if outcome == "cancel":
                    task.cancel("original host cancellation")
                try:
                    returned = await task
                except BaseException as error:
                    assert outcome != "return" and error is seen["errors"][0]
                    assert isinstance(error, asyncio.CancelledError) == (outcome == "cancel")
                else:
                    assert outcome == "return" and returned is seen["results"][0]
                assert len(seen["kwargs"]) == 1
                assert not client.is_closed()
            finally:
                release.set()
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)

    logger = logging.getLogger("mcp_flightrecorder.integrations")
    handler = BrokenHandler()
    logger.addHandler(handler)
    try:
        with caplog.at_level(logging.WARNING, logger=logger.name):
            run_sdk_async(run())
    finally:
        logger.removeHandler(handler)
        handler.close()
    assert attempts == ["integration_capture_loss reason=capture_rejected"]


def test_model_capture_pressure_does_not_repeat_sdk_work(recording_factory, run_sdk_async, monkeypatch):
    rec, path = recording_factory(limits={"ordinary_items": 1, "batch_items": 1})
    entered, release = threading.Event(), threading.Event()
    original = rec._persist_bundle

    def hold_request(connection, bundle):
        if bundle.kind == "model.request":
            entered.set()
            assert release.wait(5), "The test must release its real writer gate during bounded cleanup."
        return original(connection, bundle)

    monkeypatch.setattr(rec, "_persist_bundle", hold_request)
    evidence, requests = ModelEvidence(), []

    async def run():
        async def handler(request):
            requests.append(request)
            assert entered.wait(2), "The real writer must own the accepted request while later model observations face capacity rejection."
            return httpx2.Response(200, json=normal(tools=[function_call()], finish="tool_calls"))

        async with native_client(handler) as client:
            spy, seen = observed_method(client.chat.completions.create)
            result = await wrap_model(rec, spy, instance_id="model")(model="fixture-model", messages=[], evidence=evidence)
            check.is_true(result is seen["results"][0], msg="Ordinary recording pressure must not alter the native result.")
            check.equal(evidence.response.status, "rejected", msg="The terminal model envelope truthfully reports exhausted ordinary capacity.")
            check.equal(len(requests), 1, msg="Rejected capture cannot repeat the provider call to obtain evidence.")
            check.equal(rec.health().completion.used_slots, 0, msg="Model observation must not invent MCP completion reservations.")

    try:
        run_sdk_async(run())
    finally:
        release.set()
    rows, _, _ = persisted(rec, path)
    check.equal([row["kind"] for row in rows], ["model.request"], msg="Rejected model events cannot appear as fabricated durable completions.")
    check.equal(rec.health().dropped_events, 2, msg="The rejected decision and response are two lost envelopes, not repeated SDK attempts.")


def test_catalog_changes_are_per_request_and_sdk_retry_is_unchanged(recording_factory, run_sdk_async):
    rec, path = recording_factory()
    attempts, observed = [], []

    async def run():
        async def handler(request):
            body = json.loads(request.content)
            observed.append(body)
            attempts[-1] += 1
            if attempts[-1] == 1:
                return httpx2.Response(429, headers={"retry-after-ms": "1"},
                                      json={"error": {"message": "retry fixture", "type": "rate_limit_error"}})
            return httpx2.Response(200, json=normal())

        async with native_client(handler, retries=1) as client:
            methods = [client.chat.completions.create, wrap_model(rec, client.chat.completions.create, instance_id="model")]
            results = []
            for method in methods:
                attempts.append(0)
                results.append(await method(model="fixture-model", messages=[], tools=TOOLS))
            check.equal(results[0].to_dict(), results[1].to_dict(), msg="The decorated call preserves the SDK retry's final native response fields.")
            changed = [TOOLS[0], {"type": "function", "function": {"name": "second", "parameters": {"type": "object"}}}]
            attempts.append(0)
            await methods[1](model="fixture-model", messages=[], tools=changed)
            attempts.append(0)
            await methods[1](model="fixture-model", messages=[], tools=list(reversed(changed)))

    run_sdk_async(run(), timeout=15)
    rows, artifacts, _ = persisted(rec, path)
    requests = [row for row in rows if row["kind"] == "model.request"]
    catalogs = [json.loads(artifacts[row["catalog_ref"]]["body_text"]) for row in requests]
    check.equal(attempts, [2, 2, 2, 2], msg="Exactly one retry belongs to the configured SDK in native and decorated calls; observation adds none.")
    check.equal(len(requests), 3, msg="One logical request is recorded per wrapper call, independent of its two HTTP attempts.")
    check.equal(catalogs, [observed[2]["tools"], observed[4]["tools"], observed[6]["tools"]],
                msg="Each request retains the ordered provider catalog actually supplied for that call, including later changes.")
    check.not_equal(requests[1]["catalog_ref"], requests[2]["catalog_ref"], msg="A catalog order change cannot be hidden by a global current-catalog cache.")


@pytest.mark.parametrize("case,expected_count,expected_state", [
    ("text-length", 0, "complete"), ("text-filter", 0, "complete"),
    ("missing-name", 1, "partial"), ("missing-arguments", 1, "partial"),
    ("missing-function", 1, "partial"), ("wrong-arguments-type", 1, "partial"),
    ("missing-reference", 1, "complete"), ("legacy", 0, "partial"),
    ("non-function", 0, "partial"), ("tool-length", 1, "partial"),
    ("tool-filter", 1, "partial"), ("malformed-choices", 0, "partial"),
    ("malformed-choice", 0, "partial"), ("malformed-tools", 0, "partial"),
    ("duplicate-json-keys", 1, "complete"),
])
def test_normal_partial_and_raw_argument_boundaries(recording_factory, run_sdk_async,
                                                    case, expected_count, expected_state):
    rec, path = recording_factory()
    output = normal(tools=[function_call()], finish="tool_calls")
    message = output["choices"][0]["message"]
    function = message["tool_calls"][0]["function"]
    if case.startswith("text-"):
        output = normal(finish="length" if case == "text-length" else "content_filter")
    elif case == "missing-name":
        del function["name"]
    elif case == "missing-arguments":
        del function["arguments"]
    elif case == "missing-function":
        del message["tool_calls"][0]["function"]
    elif case == "wrong-arguments-type":
        function["arguments"] = {"not": "raw text"}
    elif case == "missing-reference":
        del message["tool_calls"][0]["id"]
    elif case == "legacy":
        message.pop("tool_calls")
        message["function_call"] = {"name": "echo", "arguments": "{}"}
        output["choices"][0]["finish_reason"] = "function_call"
    elif case == "non-function":
        message["tool_calls"] = [{"id": "custom", "type": "custom", "custom": {"name": "echo", "input": "raw"}}]
    elif case.startswith("tool-"):
        output["choices"][0]["finish_reason"] = "length" if case == "tool-length" else "content_filter"
    elif case == "malformed-choices":
        output["choices"] = {"not": "an array"}
    elif case == "malformed-choice":
        output["choices"] = ["not an object"]
    elif case == "malformed-tools":
        message["tool_calls"] = {"not": "an array"}
    elif case == "duplicate-json-keys":
        function["arguments"] = '{"x":1,"x":2}'
    evidence = ModelEvidence()

    async def run():
        async with native_client(lambda request: httpx2.Response(200, json=output)) as client:
            spy, seen = observed_method(client.chat.completions.create)
            result = await wrap_model(rec, spy, instance_id="model")(model="fixture-model", messages=[], evidence=evidence)
            check.is_true(result is seen["results"][0], msg="Partial or unsupported provider output cannot replace the native ChatCompletion.")

    run_sdk_async(run())
    rows, _, _ = persisted(rec, path)
    decisions = [row for row in rows if row["kind"] == "model.tool_call"]
    check.equal(len(decisions), expected_count, msg="Only exposed function-tool occurrences produce decision events; malformed or legacy output does not masquerade as one.")
    check.equal(rows[-1]["state"], expected_state, msg="Incomplete tool capture is partial, while completely observed text length/filter outcomes remain complete.")
    check.equal(rows[-1]["body"], output, msg="The native exposed response is retained even when its structure cannot support a complete decision.")
    if expected_state == "partial":
        check.is_in("partial_tool_call", evidence.losses, msg="Unsupported or unfinished tool/choice structure has an explicit bounded loss reason.")
    if case == "wrong-arguments-type":
        assert len(decisions) == 1, "A malformed argument field must leave an admitted partial function decision before its unavailable payload can be inspected."
        check.equal(decisions[0]["body"], {}, msg="A non-string native argument field is unavailable raw text and must not be coerced or passed as arguments_raw.")
        check.is_false(decisions[0]["details"]["complete"], msg="Omitting malformed raw text leaves the function occurrence explicitly partial.")
        check.is_none(decisions[0]["details"]["args_digest"]["value"], msg="An unexposed raw argument string cannot acquire a complete argument comparison token.")
    if case == "duplicate-json-keys":
        assert len(decisions) == 1, "A complete raw function occurrence must exist before inspecting the core duplicate-key comparison refusal."
        check.equal(decisions[0]["body"], {"arguments_raw": '{"x":1,"x":2}'}, msg="Raw duplicate JSON keys survive capture exactly.")
        check.is_none(decisions[0]["details"]["args_digest"]["value"], msg="Only the core decides that duplicate-key raw JSON has no trustworthy argument token.")
    if case == "missing-reference":
        check.is_none(evidence.decisions[0].provider_ref, msg="A missing provider ID remains null instead of being synthesized from a tool position.")


@pytest.mark.parametrize("count", [64, 65], ids=["exact-decision-cap", "decision-overflow"])
def test_normal_decision_receipts_are_bounded(recording_factory, run_sdk_async, count):
    rec, path = recording_factory()
    output = normal(tools=[function_call("{}", reference="reused") for _ in range(count)], finish="tool_calls")
    evidence = ModelEvidence()

    async def run():
        async with native_client(lambda request: httpx2.Response(200, json=output)) as client:
            result = await wrap_model(rec, client.chat.completions.create, instance_id="model")(
                model="fixture-model", messages=[], evidence=evidence)
            check.equal(len(result.choices[0].message.tool_calls), count, msg="The collector decision cap must not trim native provider results.")
            check.equal(len(evidence.decisions), 64, msg="At most 64 call-local decision receipts are retained, with the exact cap admitted.")

    run_sdk_async(run())
    rows, _, _ = persisted(rec, path)
    check.equal(sum(row["kind"] == "model.tool_call" for row in rows), 64, msg="Decision event cardinality matches the bounded exposed receipt prefix.")
    check.equal(rows[-1]["state"], "complete" if count == 64 else "partial", msg="Overflow must persist partial observation rather than silently shortening complete evidence.")
    check.equal("decision_limit" in evidence.losses, count > 64, msg="The fixed decision-limit diagnostic occurs only after the exact allowed boundary.")


class CountedBytes(httpx2.AsyncByteStream):
    """The substituted HTTP peer counts real SDK consumption and cleanup."""
    def __init__(self, values, *, done=True, tail=True, gate_at=None, error_at=None):
        self.frames = [("data: " + json.dumps(value, ensure_ascii=False) + "\n\n").encode() for value in values]
        if done:
            self.frames.append(b"data: [DONE]\n\n")
        if tail:
            self.frames.append(b": HTTP/1.1 cleanup\n\n")
        self.gate_at, self.error_at = gate_at, error_at
        self.entered, self.release = asyncio.Event(), asyncio.Event()
        self.error = RuntimeError("native counted stream failure")
        self.reads = 0
        self.closes = 0
        self.read_attempts = []
        self.close_owners = []

    async def __aiter__(self):
        for index, frame in enumerate(self.frames):
            # Entry is observable even when the consumer has not passed its gate.
            self.read_attempts.append(asyncio.current_task())
            if index == self.gate_at:
                self.entered.set()
                await self.release.wait()
            if index == self.error_at:
                raise self.error
            self.reads += 1
            yield frame

    async def aclose(self):
        self.close_owners.append(asyncio.current_task())
        self.closes += 1


def foreign_stream_owners(peer, host):
    """Attribute activity on this actual peer, not unrelated tasks in its loop."""
    return tuple(owner for owner in (*peer.read_attempts, *peer.close_owners) if owner is not host)


async def stream_checkpoint():
    """Allow already scheduled work to reach a peer boundary without a sleep."""
    ready = asyncio.Event()
    asyncio.get_running_loop().call_soon(ready.set)
    await ready.wait()


def stream_handler(stream):
    async def handler(request):
        return httpx2.Response(200, headers={"content-type": "text/event-stream"}, stream=stream)
    return handler


def decoded_chunks(client):
    """Pass-through spy on native SDK model construction, never a fake stream."""
    seen = []
    original = client._process_response_data

    def decode(*args, **kwargs):
        value = original(*args, **kwargs)
        if isinstance(value, ChatCompletionChunk):
            seen.append(value)
        return value

    client._process_response_data = decode
    return seen


def observed_next(native):
    errors = []
    original = native.__anext__

    async def advance():
        try:
            return await original()
        except BaseException as error:
            errors.append(error)
            raise

    native.__anext__ = advance
    return errors


@pytest.mark.parametrize("action,consumed", [("zero-close", 0), ("one-close", 1), ("context", 1),
                                             ("exhaust", 3), ("anext", 3), ("repeated-close", 1)])
def test_stream_protocol_identity_and_counted_native_consumption(recording_factory, run_sdk_async, action, consumed):
    rec, path = recording_factory()
    values = [chunk([delta(content="first")]), chunk([delta(content="last", finish="stop")]),
              chunk(usage={"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3})]
    evidence = ModelEvidence()

    async def run_one(decorated):
        peer = CountedBytes(values)
        host = asyncio.current_task()
        async with native_client(stream_handler(peer)) as client:
            decoded = decoded_chunks(client)
            spy, calls = observed_method(client.chat.completions.create)
            method = wrap_model(rec, spy, instance_id="model") if decorated else spy
            stream = await method(model="fixture-model", messages=[], stream=True,
                                  **({"evidence": evidence} if decorated else {}))
            await stream_checkpoint()
            check.equal(peer.reads, 0, msg="Returning a native or observed stream must not consume its first body frame.")
            check.equal(peer.read_attempts, [], msg="No consumer may enter this stream before the host asks for a chunk, including a background read waiting at a peer gate.")
            check.equal(peer.closes, 0, msg="Creating a stream cannot close its native body before the host selects a cleanup boundary.")
            check.equal(foreign_stream_owners(peer, host), (), msg="Only activity causally attached to this stream can violate ownership; unrelated native generator finalizers are not wrapper work.")
            check.is_true(stream.response is calls["results"][0].response, msg="The proxy exposes the exact native HTTP response object.")
            check.is_true(all(hasattr(stream, name) for name in ("__aiter__", "__anext__", "__aenter__", "__aexit__", "close", "aclose")),
                          msg="All async iteration and cleanup protocols exposed by the selected SDK remain available.")
            received = []
            if action == "context":
                async with stream as entered:
                    check.is_true(entered is stream, msg="Async context entry returns the stream object that the host is consuming.")
                    received.append(await entered.__anext__())
            elif action in ("one-close", "repeated-close"):
                received.append(await stream.__anext__())
                await stream.close()
                if action == "repeated-close":
                    await stream.aclose()
                    await stream.close()
            elif action == "exhaust":
                received = [value async for value in stream]
                await stream.aclose()
            elif action == "anext":
                for _ in values:
                    received.append(await stream.__anext__())
                # Exhaustion remains the native iterator's StopAsyncIteration path.
                with pytest.raises(StopAsyncIteration):
                    await stream.__anext__()
            else:
                await stream.aclose()
            check.equal(len(received), consumed, msg="Only host-requested native chunks are returned before the selected cleanup boundary.")
            check.is_true(all(actual is expected for actual, expected in zip(received, decoded)),
                          msg="Each returned chunk is exactly the model instance created by the actual SDK parser.")
            check.equal(foreign_stream_owners(peer, host), (), msg="Every actual body-read attempt and peer close must be driven by this stream's host, never by an added background consumer.")
            if decorated:
                check.equal((stream._capture.chunks, stream._capture.tools, stream._capture.choices), ([], {}, {}),
                            msg="Terminal stream capture releases chunk, fragment and choice assembly state.")
                check.equal((stream._capture.chunk_bytes, stream._capture.fragment_bytes), (0, 0),
                            msg="Terminal cleanup also clears both independent retained-byte counters.")
            return peer.reads, peer.closes, [value.to_dict(mode="json", use_api_names=True, exclude_unset=True) for value in received]

    async def run():
        native = await run_one(False)
        decorated = await run_one(True)
        check.equal(decorated, native, msg="Native and decorated reads, HTTP cleanup and chunk order match, including SDK-owned HTTP/1.1 draining after DONE.")

    run_sdk_async(run())
    rows, _, _ = persisted(rec, path)
    check.equal([row["kind"] for row in rows], ["model.request", "model.response"], msg="Cleanup and repeated close produce at most one terminal, with no per-chunk events.")
    check.equal(rows[-1]["body"], {"format": "openai_chat_stream/1", "chunks": values[:consumed]},
                msg="The terminal stream payload is a named ordered observation of consumed chunks, not a fabricated ChatCompletion.")
    check.equal(rows[-1]["state"], "complete" if action in ("exhaust", "anext") else "partial", msg="Only fully witnessed exhaustion is complete; explicit early close remains partial.")


@pytest.mark.parametrize("decorated", [False, True], ids=["native", "decorated"])
def test_stream_activity_ignores_collected_native_predecessor(recording_factory, run_sdk_async, decorated):
    """GC runs inside the next real request; no task-type or task-count exception."""
    rec, path = recording_factory()
    values = [chunk([delta(content="first")]), chunk([delta(content="last", finish="stop")])]
    evidence = ModelEvidence()

    async def predecessor():
        peer = CountedBytes(values)
        async with native_client(stream_handler(peer)) as client:
            stream = await client.chat.completions.create(model="fixture-model", messages=[], stream=True)
            async with stream:
                await stream.__anext__()
            # Retain the actual closed, partially consumed native stream until
            # the next HTTP handler chooses the collection window.
            return [stream], weakref.ref(stream), peer

    async def run():
        assert gc.isenabled(), "The native-GC control requires ordinary collection enabled; disabling cleanup cannot make it pass."
        retained, previous, prior_peer = await predecessor()
        assert previous() is not None, "The test must own the prior native stream until the deliberate next-request collection window."
        peer = CountedBytes(values)
        host = asyncio.current_task()

        async def handler(request):
            retained.clear()
            gc.collect()
            await stream_checkpoint()
            assert previous() is None, "The actual predecessor must be collected during this request, not merely scheduled for later disposal."
            return httpx2.Response(200, headers={"content-type": "text/event-stream"}, stream=peer)

        async with native_client(handler) as client:
            decoded = decoded_chunks(client)
            method = wrap_model(rec, client.chat.completions.create, instance_id="model") if decorated else client.chat.completions.create
            stream = await method(model="fixture-model", messages=[], stream=True,
                                  **({"evidence": evidence} if decorated else {}))
            await stream_checkpoint()
            check.is_true(gc.isenabled(), msg="Native cleanup runs under the ordinary enabled collector throughout the disputed observation window.")
            check.equal((prior_peer.reads, prior_peer.closes), (1, 1), msg="Collecting the predecessor preserves its one host-consumed frame and one explicit native close.")
            check.equal((peer.reads, peer.closes, peer.read_attempts), (0, 0, []), msg="Native predecessor finalizers cannot be blamed on a new stream that has performed no read attempt or close.")
            check.equal(foreign_stream_owners(peer, host), (), msg="The causal detector accepts ordinary SDK generator GC while the current stream is idle.")
            value = await stream.__anext__()
            check.is_true(value is decoded[0], msg="After native GC the current request still yields the exact SDK-created chunk object.")
            await stream.aclose()
            await stream.close()
            check.equal((peer.reads, peer.closes), (1, 1), msg="Early and repeated cleanup preserve the native one-read, one-close boundary after the GC control.")
            check.equal(foreign_stream_owners(peer, host), (), msg="Only the explicit current host drives its new stream despite unrelated native cleanup in the same loop.")

    run_sdk_async(run())
    rows, _, _ = persisted(rec, path)
    check.equal([row["kind"] for row in rows], ["model.request", "model.response"] if decorated else [],
                msg="The GC control records only its selected decorated request and one terminal; the native predecessor creates no recorder evidence.")
    if decorated:
        assert len(rows) == 2, "A decorated early-close request must have one response before inspecting its retained consumed prefix."
        check.equal(rows[-1]["body"], {"format": "openai_chat_stream/1", "chunks": values[:1]},
                    msg="Native cleanup cannot cause capture to drain or retain the unconsumed suffix of the new stream.")
        check.equal(rows[-1]["state"], "partial", msg="The host's explicit early close remains partial even when native GC also ran.")


def test_stream_cancellation_before_first_chunk_is_empty_terminal(recording_factory, run_sdk_async):
    rec, path = recording_factory()
    values = [chunk([delta(content="first", finish="stop")])]
    evidence = ModelEvidence()

    async def run():
        peer = CountedBytes(values, gate_at=0)
        async with native_client(stream_handler(peer)) as client:
            spy, seen = observed_method(client.chat.completions.create)
            method = wrap_model(rec, spy, instance_id="model")
            stream = await method(model="fixture-model", messages=[], stream=True,
                                  evidence=evidence)
            errors = observed_next(seen["results"][0])
            consumer = asyncio.create_task(stream.__anext__())
            try:
                await asyncio.wait_for(peer.entered.wait(), timeout=2)
                check.is_false(peer.release.is_set() or consumer.done(), msg="The first host read must remain held at the real HTTP body gate before cancellation.")
                check.equal(peer.reads, 0, msg="Cancellation occurs before the SDK delivers any frame.")
                consumer.cancel("host cancellation before first chunk")
                with pytest.raises(asyncio.CancelledError) as caught:
                    await consumer
                check.equal(caught.value.args, ("host cancellation before first chunk",), msg="The wrapper preserves the original host cancellation payload.")
                assert errors, "The actual SDK read must expose its cancellation before identity is compared."
                check.is_true(caught.value is errors[-1], msg="The wrapper propagates the exact native cancellation object.")
                terminal = accepted(evidence.response)
                check.equal(evidence.decisions, (), msg="An unconsumed stream cannot supply a model decision.")
            finally:
                if not consumer.done():
                    consumer.cancel("host stream cleanup")
                await asyncio.gather(consumer, return_exceptions=True)
                peer.release.set()
                await stream.aclose()
            await stream.close()
            check.equal(peer.reads, 0, msg="Reaping the cancelled read and closing cannot drain an unconsumed native frame.")
            check.equal(peer.closes, 1, msg="Explicit and repeated cleanup preserve the native single close.")
            check.equal(peer.close_owners, [consumer], msg="Native cleanup remains in the host's cancelled read task.")
            check.is_true(evidence.response is terminal, msg="Cleanup cannot replace or duplicate the cancellation terminal.")
            check.equal(evidence.decisions, (), msg="Cleanup cannot fabricate a decision from an unconsumed stream.")

    run_sdk_async(run())
    rows, _, _ = persisted(rec, path)
    check.equal([row["kind"] for row in rows], ["model.request", "model.response"],
                msg="Cancellation followed by cleanup persists exactly one request and one terminal, without fabricated decisions.")
    assert len(rows) == 2, "The cancelled stream must persist one response before its state is inspected."
    check.equal(rows[-1]["state"], "cancelled", msg="The terminal preserves cancellation before the first consumed chunk.")
    check.equal(rows[-1]["body"], {"format": "openai_chat_stream/1", "chunks": []},
                msg="An attempted but Event-blocked read supplies no chunk payload.")


def test_stream_interleaved_position_keys_finish_receipts_and_usage(recording_factory, run_sdk_async):
    rec, path = recording_factory()
    values = [chunk([
        delta(index=1, tools=[{"index": 1, "id": "same-", "type": "function", "function": {"name": "ec", "arguments": '{"text":"'}},
                              {"index": 0, "id": "same-", "type": "function", "function": {"name": "e", "arguments": '{"v":'}}]),
        delta(index=0, tools=[{"index": 0, "id": "same-", "type": "function", "function": {"name": "ec", "arguments": '{"value":'}},
                              {"index": 1, "id": "same-", "type": "function", "function": {"name": "e", "arguments": '{"n":'}}])]),
        chunk([delta(index=0, tools=[{"index": 1, "id": "id", "function": {"name": "cho", "arguments": "2}"}},
                                    {"index": 0, "id": "id", "function": {"name": "ho", "arguments": "1}"}}], finish="tool_calls")]),
        chunk([delta(index=1, tools=[{"index": 0, "id": "id", "function": {"name": "cho", "arguments": "3}"}},
                                    {"index": 1, "id": "id", "function": {"name": "ho", "arguments": 'x"}'}}], finish="tool_calls")]),
        chunk(usage={"prompt_tokens": 5, "completion_tokens": 7, "total_tokens": 12}, future_json={"kept": True})]
    evidence = ModelEvidence()

    async def run():
        async with native_client(stream_handler(CountedBytes(values))) as client:
            decoded = decoded_chunks(client)
            stream = await wrap_model(rec, client.chat.completions.create, instance_id="model")(
                model="fixture-model", messages=[], stream=True, evidence=evidence)
            for index, value in enumerate(values):
                observed = await stream.__anext__()
                check.is_true(observed is decoded[-1], msg="Fragment assembly cannot substitute a reconstructed chunk for the native object.")
                check.equal(len(evidence.decisions), [0, 2, 4, 4][index], msg="A finishing choice's decisions are available before its native chunk reaches the host; usage creates none.")
                if index == 1:
                    check.equal([(entry.choice_index, entry.tool_index) for entry in evidence.decisions], [(0, 0), (0, 1)],
                                msg="Available entries preserve first-observed key order within the completed subset.")
            with pytest.raises(StopAsyncIteration):
                await stream.__anext__()
            check.equal([(entry.choice_index, entry.tool_index, entry.provider_ref) for entry in evidence.decisions],
                        [(1, 1, "same-id"), (1, 0, "same-id"), (0, 0, "same-id"), (0, 1, "same-id")],
                        msg="Final receipt order follows initial positional occurrences even when choices finish in reverse order and provider IDs coincide.")
            check.equal(len({accepted(entry.receipt).decision_id for entry in evidence.decisions}), 4,
                        msg="Reused provider IDs cannot collapse four call-local decision identities.")
            check.equal(evidence.losses, (), msg="Interleaving, split ID/name/raw strings and benign unknown usage fields are fully observed.")

    run_sdk_async(run())
    rows, _, _ = persisted(rec, path)
    decisions = {row["event_id"]: row for row in rows if row["kind"] == "model.tool_call"}
    ordered = [decisions[entry.receipt.event_id] for entry in evidence.decisions]
    check.equal([row["body"] for row in ordered], [{"arguments_raw": '{"text":"x"}'}, {"arguments_raw": '{"v":3}'},
                                                  {"arguments_raw": '{"value":1}'}, {"arguments_raw": '{"n":2}'}],
                msg="Raw argument fragments append by choice/tool positions, never by repeated provider IDs or arriving tool-list order.")
    check.equal([row["name"] for row in ordered], ["echo"] * 4, msg="Exposed name fragments append in received order for each positional occurrence.")
    check.is_true(all(row["details"]["complete"] for row in ordered), msg="Every complete tool choice retains a complete decision even before stream usage arrives.")
    check.equal(rows[-1]["body"], {"format": "openai_chat_stream/1", "chunks": values}, msg="All consumed usage and unknown fields survive the named stream observation.")


@pytest.mark.parametrize("body_overflow", [False, True], ids=["body-retained", "body-overflow"])
@pytest.mark.parametrize("ending", ["eof", "close", "error", "cancel"])
def test_stream_completed_decisions_survive_pending_failure_and_frozen_scope(recording_factory, run_sdk_async, ending, body_overflow):
    rec, path = recording_factory()
    values = [chunk([delta(index=0, tools=[{"index": 0, **function_call("{}", reference="complete")}], finish="tool_calls")]),
              chunk([delta(index=1, tools=[{"index": 0, **function_call('{"pending":', reference="pending")}])])]
    if body_overflow:
        values.insert(1, chunk(padding="x" * 1_048_576))
    evidence = ModelEvidence()
    messages = [{"role": "user", "content": "factory"}]
    policy_calls = []

    def policy(kind, payload, context):
        policy_calls.append((kind, json.loads(json.dumps(payload))))
        return {"policy_id": "stream-lifecycle", "retained": payload}

    async def run():
        peer = CountedBytes(values, done=ending != "eof", tail=ending != "eof",
                            gate_at=len(values) if ending == "cancel" else None,
                            error_at=len(values) if ending == "error" else None)
        async with native_client(stream_handler(peer)) as client:
            decoded = decoded_chunks(client)
            spy, seen = observed_method(client.chat.completions.create)
            create = wrap_model(rec, spy, instance_id="model", retention_policy=policy)
            with rec.turn("A") as turn_a:
                with rec.dispatch(action="execute", name="echo", arguments={}):
                    pending = create(model="fixture-model", messages=messages, stream=True, evidence=evidence)
            messages[0]["content"] = "execution"
            with rec.turn("B") as turn_b:
                stream = await pending
                errors = observed_next(seen["results"][0])
                messages[0]["content"] = "after submission"
                for _ in values:
                    returned = await stream.__anext__()
                    assert returned is decoded[-1]
                check.equal(len(evidence.decisions), 1, msg="Only the choice with a consumed tool_calls finish witness is complete before the later boundary.")
                check.is_none(evidence.response, msg="Scope exit and partial consumption cannot complete or cancel a still-open native stream.")
                if ending == "close":
                    await stream.aclose()
                elif ending == "eof":
                    with pytest.raises(StopAsyncIteration):
                        await stream.__anext__()
                elif ending == "error":
                    with pytest.raises(RuntimeError) as caught:
                        await stream.__anext__()
                    check.is_true(caught.value is peer.error is errors[-1], msg="A consumed-stream SDK failure propagates its exact original exception object.")
                else:
                    task = asyncio.create_task(stream.__anext__())
                    await asyncio.wait_for(peer.entered.wait(), 2)
                    task.cancel("stream host cancellation")
                    try:
                        with pytest.raises(asyncio.CancelledError) as caught:
                            await task
                        check.is_true(caught.value is errors[-1], msg="Stream cancellation propagates the original native cancellation object.")
                    finally:
                        peer.release.set()
                assert (stream._capture.chunks, stream._capture.tools, stream._capture.choices) == ([], {}, {})
                assert (stream._capture.chunk_bytes, stream._capture.fragment_bytes) == (0, 0)
                response = accepted(evidence.response)
                await stream.close()
                await stream.aclose()
                assert evidence.response is response
                assert (stream._capture.chunks, stream._capture.tools, stream._capture.choices) == ([], {}, {})
                assert (stream._capture.chunk_bytes, stream._capture.fragment_bytes) == (0, 0)
                assert len(decoded) == len(values) and len(seen["kwargs"]) == 1
                check.equal(rec.capture_scope().turn_id, turn_b.receipt.turn_id, msg="Stream completion and cleanup cannot overwrite the host's later ambient context.")
                check.equal(len(evidence.decisions), 2, msg="Every consumed pending key emits once at the boundary, while completed keys are not duplicated.")
            return turn_a.receipt.turn_id

    turn_a = run_sdk_async(run())
    rows, artifacts, _ = persisted(rec, path)
    assert [kind for kind, _ in policy_calls] == ["model.request", "model.tool_call", "model.tool_call", "model.response"]
    aggregate = {"format": "openai_chat_stream/1", "chunks": values[:1] if body_overflow else values}
    assert policy_calls[-1][1] == aggregate
    model = [row for row in rows if row["boundary"] == "model"]
    decisions = [row for row in model if row["kind"] == "model.tool_call"]
    check.equal([row["turn_id"] for row in model], [turn_a] * 4, msg="Every stream request, completed/pending decision and terminal retains invocation turn A.")
    check.equal(request_body(model[0], artifacts)["messages"][0]["content"], "execution", msg="Streaming requests observe execution-time input and detach later caller mutations.")
    check.equal([row["details"]["complete"] for row in decisions], [True, False], msg="Completed decisions stay complete; only the unfinished consumed key becomes partial.")
    assert [row["body"] for row in decisions] == [{"arguments_raw": "{}"}, {"arguments_raw": '{"pending":'}]
    assert [row["details"]["args_digest"]["value"] is not None for row in decisions] == [True, False]
    assert model[-1]["body"] == aggregate
    check.equal(model[-1]["state"], {"eof": "partial", "close": "partial", "error": "error", "cancel": "cancelled"}[ending],
                msg="The single stream terminal preserves the actual exhaustion, explicit-close, native-failure or cancellation boundary.")


@pytest.mark.parametrize("case", ["empty-eof", "text-eof", "tool-eof", "missing-name", "missing-arguments",
                                  "legacy", "non-function", "malformed-choices", "malformed-choice", "malformed-tool-index",
                                  "text-length", "text-filter", "duplicate-json-keys", "unknown-fields"])
def test_stream_partial_structure_and_finish_witnesses(recording_factory, run_sdk_async, case):
    rec, path = recording_factory()
    tool = {"index": 0, **function_call("{}")}
    values = [chunk([delta(tools=[tool], finish="tool_calls")])]
    partial = case not in ("text-length", "text-filter", "duplicate-json-keys", "unknown-fields")
    if case == "empty-eof":
        values = []
    elif case == "text-eof":
        values = [chunk([delta(content="unfinished")])]
    elif case == "tool-eof":
        values[0]["choices"][0]["finish_reason"] = None
    elif case == "missing-name":
        del tool["function"]["name"]
    elif case == "missing-arguments":
        del tool["function"]["arguments"]
    elif case == "legacy":
        values = [chunk([delta(function_call={"name": "echo", "arguments": "{}"}, finish="function_call")])]
    elif case == "non-function":
        tool["type"] = "custom"
    elif case == "malformed-choices":
        values = [chunk()]
        values[0]["choices"] = "not an array"
    elif case == "malformed-choice":
        values = [chunk(["not an object"])]
    elif case == "malformed-tool-index":
        tool["index"] = None
    elif case.startswith("text-"):
        values = [chunk([delta(content="bounded", finish="length" if case == "text-length" else "content_filter")])]
    elif case == "duplicate-json-keys":
        tool["function"]["arguments"] = '{"x":1,"x":2}'
    else:
        values = [chunk([delta(content="known", finish="stop", future_delta={"kept": True})], future_chunk=[0, False, None])]
    evidence = ModelEvidence()

    async def run():
        peer = CountedBytes(values, done=False, tail=False)
        async with native_client(stream_handler(peer)) as client:
            decoded = decoded_chunks(client)
            stream = await wrap_model(rec, client.chat.completions.create, instance_id="model")(
                model="fixture-model", messages=[], stream=True, evidence=evidence)
            returned = [value async for value in stream]
            check.equal(len(returned), len(values), msg="Unsupported capture structure cannot remove native SDK chunks from host consumption.")
            check.is_true(all(actual is expected for actual, expected in zip(returned, decoded)), msg="Every malformed or partial chunk remains the actual native SDK model object.")

    run_sdk_async(run())
    rows, _, _ = persisted(rec, path)
    check.equal(rows[-1]["state"], "partial" if partial else "complete", msg="Completion requires exposed choice finish witnesses; invisible DONE bytes are neither required nor inferred.")
    check.equal(rows[-1]["body"], {"format": "openai_chat_stream/1", "chunks": values}, msg="Malformed, partial and benign unknown projections remain ordered consumed JSON evidence.")
    if case in ("legacy", "non-function", "malformed-choices", "malformed-choice", "malformed-tool-index"):
        check.equal(evidence.decisions, (), msg="Unsupported structures cannot silently become captured function decisions.")
    if case == "duplicate-json-keys":
        decisions = [row for row in rows if row["kind"] == "model.tool_call"]
        assert len(decisions) == 1, "A complete raw function occurrence must exist before inspecting the core comparison refusal."
        check.equal(decisions[0]["body"], {"arguments_raw": '{"x":1,"x":2}'}, msg="Streaming preserves duplicate raw keys without adding a second parsing policy.")
        check.is_none(decisions[0]["details"]["args_digest"]["value"], msg="The core alone refuses a complete comparison token for duplicate raw JSON keys.")


@pytest.mark.parametrize("case", ["completed-later", "still-missing", "non-null-wrong-type"])
def test_stream_nullable_fragments_wait_for_exposed_strings(recording_factory, run_sdk_async, case):
    rec, path = recording_factory()
    function = {"name": None, "arguments": None}
    if case == "non-null-wrong-type":
        function = {"name": 37, "arguments": {"not": "a string fragment"}}
    first = chunk([delta(tools=[{"index": 0, "id": None, "type": "function", "function": function}])])
    exposed = {} if case == "still-missing" else {"name": "echo", "arguments": "{}"}
    second = chunk([delta(tools=[{"index": 0, "id": "observed-id", "function": exposed}], finish="tool_calls")])
    evidence = ModelEvidence()
    complete = case == "completed-later"

    async def run():
        async with native_client(stream_handler(CountedBytes([first, second]))) as client:
            decoded = decoded_chunks(client)
            stream = await wrap_model(rec, client.chat.completions.create, instance_id="model")(
                model="fixture-model", messages=[], stream=True, evidence=evidence)
            returned = [value async for value in stream]
            check.is_true(all(actual is expected for actual, expected in zip(returned, decoded)),
                          msg="Null or malformed native fragments cannot replace or remove the original SDK chunk objects.")
            check.equal(len(evidence.decisions), 1, msg="One positional tool key emits one receipt when its choice finishes.")

    run_sdk_async(run())
    rows, _, _ = persisted(rec, path)
    decisions = [row for row in rows if row["kind"] == "model.tool_call"]
    assert len(decisions) == 1, "The consumed function position must persist once before its accumulated completeness is inspected."
    check.equal(decisions[0]["details"]["complete"], complete,
                msg="Optional null fragments are unexposed until later strings arrive; fields missing at finish or malformed non-null fragments stay partial.")
    check.equal(decisions[0]["body"], {} if case == "still-missing" else {"arguments_raw": "{}"},
                msg="Only exposed string fragments enter raw arguments; nulls and malformed values are never coerced or parsed.")
    check.equal(rows[-1]["state"], "complete" if complete else "partial",
                msg="The terminal uses accumulated exposed fields plus the consumed finish witness, without treating optional nulls as a permanent gap.")
    check.equal("partial_tool_call" in evidence.losses, not complete,
                msg="A valid null-then-exposed tool call has no invented loss, while genuinely unavailable or malformed fields remain explicit.")
    check.equal(rows[-1]["body"]["chunks"], [first, second], msg="Both raw native projections, including the initial nulls, remain retained exactly.")


@pytest.mark.parametrize("optional", ["absent", "null"])
@pytest.mark.parametrize("case", ["leading-id", "id-continuation", "index-only", "function-empty"])
def test_stream_optional_tool_fields_preserve_complete_decision(recording_factory, run_sdk_async, case, optional):
    rec, path = recording_factory()
    initial = {"index": 0, **function_call("{}", reference="call-")}
    if case == "leading-id":
        first, second = {"index": 0, "id": "call-"}, {"index": 0, **function_call("{}", reference="suffix")}
        sparse = first
    else:
        first = initial
        second = {"index": 0}
        if case != "index-only":
            second["id"] = "suffix"
        if case == "function-empty":
            second["function"] = {}
        sparse = second
    if optional == "null":
        for field in ("id", "type", "function"):
            sparse.setdefault(field, None)
        if case == "function-empty":
            sparse["function"] = {"name": None, "arguments": None}
    values = [chunk([delta(tools=[first])]), chunk([delta(tools=[second])]),
              chunk([delta(finish="tool_calls")])]
    expected_reference = "call-" if case == "index-only" else "call-suffix"
    evidence = ModelEvidence()

    async def run():
        peer = CountedBytes(values)
        async with native_client(stream_handler(peer)) as client:
            decoded = decoded_chunks(client)
            stream = await wrap_model(rec, client.chat.completions.create, instance_id="model")(
                model="fixture-model", messages=[], stream=True, evidence=evidence)
            for index in range(len(values)):
                returned = await stream.__anext__()
                check.is_true(returned is decoded[-1], msg="Optional-field assembly yields the exact model parsed by the actual SDK, never a replacement chunk.")
                check.equal(len(evidence.decisions), int(index == 2), msg="The sole positional decision is emitted once before the consumed finish chunk reaches its caller.")
                if index == 2:
                    assert len(evidence.decisions) == 1, "A consumed tool_calls finish witness must expose one receipt before inspecting its assembled provider identity."
                    check.equal(evidence.decisions[0].provider_ref, expected_reference,
                                msg="Every exposed ID fragment survives absent/null optional neighbors; an index-only continuation changes nothing.")
                    accepted(evidence.decisions[0].receipt)
                    check.is_none(evidence.response, msg="A completed decision does not invent whole-stream exhaustion before the caller consumes it.")
            # Native StopAsyncIteration is the actual complete-stream boundary.
            with pytest.raises(StopAsyncIteration):
                await stream.__anext__()
            await stream.aclose()
            check.equal(len(decoded), len(values), msg="Capture neither synthesizes nor consumes extra native chunks around optional deltas.")
            check.equal(evidence.losses, (), msg="Absent, null and empty optional neighbors are no evidence loss after the function fields and finish witness are observed.")

    run_sdk_async(run())
    rows, _, _ = persisted(rec, path)
    check.equal([row["kind"] for row in rows], ["model.request", "model.tool_call", "model.response"],
                msg="One function key and repeated cleanup produce one decision and one terminal, with no extra chunk or provisional events.")
    decisions = [row for row in rows if row["kind"] == "model.tool_call"]
    assert len(decisions) == 1, "The exact persisted function occurrence is required before inspecting its raw arguments and complete token."
    decision = decisions[0]
    check.equal((decision["name"], decision["details"]["provider_ref"]), ("echo", expected_reference),
                msg="Persisted identity retains the full observed provider reference and function name through every valid optional shape.")
    check.equal(decision["body"], {"arguments_raw": "{}"}, msg="Raw arguments remain the exact exposed string; absent/null fields cannot erase or coerce them.")
    check.is_true(decision["details"]["complete"], msg="A witnessed function with complete observed name/raw arguments remains complete despite harmless sparse deltas.")
    token = decision["details"]["args_digest"]
    check.equal((token["scope"], token["projection"], token["basis"]),
                (rec.capture_scope().run_id, "args_json_python/1", "observed_original"),
                msg="The complete argument token uses the existing run-scoped original JSON projection.")
    check.is_not_none(token["value"], msg="Harmless optional deltas cannot make a valid complete raw-argument comparison unavailable.")
    check.equal((rows[-1]["event_id"], rows[-1]["state"], rows[-1]["details"]["complete"]),
                (accepted(evidence.response).event_id, "complete", True),
                msg="The complete terminal is attached to its public response receipt and carries no invented partial loss.")
    check.equal(rows[-1]["body"], {"format": "openai_chat_stream/1", "chunks": values},
                msg="Exact native projections preserve the distinction between absent fields, explicit nulls and empty function objects.")


def test_stream_provisional_positions_keep_first_observed_receipt_order(recording_factory, run_sdk_async):
    rec, path = recording_factory()
    values = [chunk([
        delta(index=1, tools=[{"index": 1, "id": "first-"}, {"index": 0}]),
        delta(index=0, tools=[{"index": 0, "id": "third-", "type": None, "function": None}])]),
        chunk([delta(index=0, tools=[{"index": 0, **function_call('{"v":3}', reference="end")}], finish="tool_calls")]),
        chunk([delta(index=1, tools=[{"index": 0, **function_call('{"v":2}', reference="second")},
                                    {"index": 1, **function_call('{"v":1}', reference="end")}], finish="tool_calls")]),
        chunk(usage={"prompt_tokens": 1, "completion_tokens": 3, "total_tokens": 4})]
    evidence = ModelEvidence()

    async def run():
        async with native_client(stream_handler(CountedBytes(values))) as client:
            decoded = decoded_chunks(client)
            stream = await wrap_model(rec, client.chat.completions.create, instance_id="model")(
                model="fixture-model", messages=[], stream=True, evidence=evidence)
            for index in range(len(values)):
                returned = await stream.__anext__()
                check.is_true(returned is decoded[-1], msg="Interleaved provisional-key assembly preserves every actual SDK chunk object.")
                check.equal(len(evidence.decisions), [0, 1, 3, 3][index], msg="Finishing choices publish their decisions before yielding, and usage publishes no additional key.")
                if index == 1:
                    check.equal([(entry.choice_index, entry.tool_index, entry.provider_ref) for entry in evidence.decisions],
                                [(0, 0, "third-end")], msg="The third first-observed key may finish first without stealing a provisional neighbor's identity.")
                if index == 2:
                    check.equal([(entry.choice_index, entry.tool_index, entry.provider_ref) for entry in evidence.decisions],
                                [(1, 1, "first-end"), (1, 0, "second"), (0, 0, "third-end")],
                                msg="Receipt order follows initial ID-only and index-only positions, not later function arrival or finish order.")
            with pytest.raises(StopAsyncIteration):
                await stream.__anext__()
            await stream.close()
            check.equal(len({accepted(entry.receipt).decision_id for entry in evidence.decisions}), 3,
                        msg="Each provisional positional key emits one distinct decision identity despite repeated calls to cleanup.")
            check.equal(evidence.losses, (), msg="Delayed function confirmation and reverse choice completion introduce no observation loss.")

    run_sdk_async(run())
    rows, _, _ = persisted(rec, path)
    by_event = {row["event_id"]: row for row in rows if row["kind"] == "model.tool_call"}
    assert len(by_event) == len(evidence.decisions) == 3, "All three accepted provisional keys must be persisted before their receipt-order payloads are compared."
    ordered = [by_event[entry.receipt.event_id] for entry in evidence.decisions]
    check.equal([row["body"] for row in ordered], [{"arguments_raw": '{"v":1}'}, {"arguments_raw": '{"v":2}'}, {"arguments_raw": '{"v":3}'}],
                msg="Raw arguments remain attached to their original positional occurrences even when function fields arrive in another order.")
    check.is_true(all(row["details"]["complete"] and row["details"]["args_digest"]["value"] is not None for row in ordered),
                  msg="Every confirmed and finished provisional key receives complete evidence and an available argument token.")
    check.equal(rows[-1]["state"], "complete", msg="All observed choice finishes and optional provisional fields support a complete response.")
    check.equal(rows[-1]["body"], {"format": "openai_chat_stream/1", "chunks": values},
                msg="Interleaving changes assembly only; the persisted consumed-chunk observation retains native order and nulls exactly.")


@pytest.mark.parametrize("case", ["early-close", "native-error", "unconfirmed-finish", "unsupported-after-provisional",
                                  "unsupported-after-function", "unsupported-before-function", "malformed-before-function"])
def test_stream_provisional_and_unsupported_neighbors_remain_partial(recording_factory, run_sdk_async, case):
    rec, path = recording_factory()
    provisional = {"index": 0, "id": "call-", "type": None, "function": None}
    confirmed = {"index": 0, **function_call("{}", reference="call-")}
    unsupported = {"index": 0, "type": "custom"}
    first = confirmed if case == "unsupported-after-function" else unsupported if case == "unsupported-before-function" else provisional
    if case == "malformed-before-function":
        first = {"index": 0, "id": "call-", "function": "not a function object"}
    if case in ("unsupported-after-provisional", "unsupported-after-function"):
        following = unsupported
    elif case == "unsupported-before-function":
        following = confirmed
    elif case == "malformed-before-function":
        following = {"index": 0, "type": "function", "function": {"name": "echo", "arguments": "{}"}}
    else:
        following = None
    values = [chunk([delta(tools=[first])])]
    if following is not None:
        values.append(chunk([delta(tools=[following])]))
    values.append(chunk([delta(finish="tool_calls")]))
    evidence = ModelEvidence()
    partial_function = case in ("unsupported-after-function", "unsupported-before-function", "malformed-before-function")
    expected_count = 0 if case == "unsupported-after-provisional" else 1
    consumed = 1 if case in ("early-close", "native-error") else len(values)

    async def run():
        peer = CountedBytes(values, error_at=1 if case == "native-error" else None)
        async with native_client(stream_handler(peer)) as client:
            decoded = decoded_chunks(client)
            spy, calls = observed_method(client.chat.completions.create)
            stream = await wrap_model(rec, spy, instance_id="model")(
                model="fixture-model", messages=[], stream=True, evidence=evidence)
            errors = observed_next(calls["results"][0])
            first_returned = await stream.__anext__()
            check.is_true(first_returned is decoded[0], msg="A provisional, malformed or unsupported first delta remains the actual native chunk seen by the host.")
            if case == "early-close":
                await stream.aclose()
            elif case == "native-error":
                # The controlled peer's original failure must reach the host.
                with pytest.raises(RuntimeError) as caught:
                    await stream.__anext__()
                check.is_true(caught.value is peer.error is errors[-1], msg="Provisional-key cleanup preserves the exact native exception object instead of converting capture incompleteness into a host failure.")
            else:
                for index in range(1, len(values)):
                    returned = await stream.__anext__()
                    check.is_true(returned is decoded[index], msg="Actual unsupported or malformed neighbor fields cannot remove or replace native stream chunks.")
                if case == "unsupported-after-provisional":
                    assert stream._capture.fragment_bytes == 0
                with pytest.raises(StopAsyncIteration):
                    await stream.__anext__()
            await stream.close()
            await stream.aclose()
            check.equal(len(evidence.decisions), expected_count, msg="Unknown provisional keys receive honest partial receipts, while a key exposed only as unsupported never becomes a function decision.")
            check.is_in("partial_tool_call", evidence.losses, msg="A truly unfinished, unsupported or malformed observation retains its explicit loss; repairing optional fields cannot erase it.")
            check.equal(len(decoded), consumed, msg="Early close and native error preserve only the requested prefix; cleanup never drains a late completion.")

    run_sdk_async(run())
    rows, _, _ = persisted(rec, path)
    decisions = [row for row in rows if row["kind"] == "model.tool_call"]
    check.equal(len(decisions), expected_count, msg="Exactly-once partial emission and unsupported-type suppression must agree between public receipts and SQLite.")
    if expected_count:
        assert len(decisions) == 1, "One retained positional occurrence is required before checking its partial raw evidence."
        check.equal(decisions[0]["details"]["provider_ref"], "call-", msg="Partial evidence retains every previously observed usable ID prefix without inventing later fragments.")
        check.equal(decisions[0]["body"], {"arguments_raw": "{}"} if partial_function else {},
                    msg="Partial function observations preserve exposed raw text; ID-only observations cannot synthesize unavailable arguments.")
        check.is_false(decisions[0]["details"]["complete"], msg="A finish witness cannot make an unconfirmed or previously unsupported/malformed key complete.")
        check.is_none(decisions[0]["details"]["args_digest"]["value"], msg="Incomplete or unsupported history never receives an available complete argument comparison token.")
    check.equal([row["kind"] for row in rows].count("model.response"), 1, msg="Failure, early close and repeated cleanup submit only one terminal response.")
    check.equal(rows[-1]["state"], "error" if case == "native-error" else "partial", msg="The terminal distinguishes native failure from successful but incomplete capture.")
    check.is_false(rows[-1]["details"]["complete"], msg="Every genuine incomplete or unsupported neighbor remains explicitly incomplete after the optional-field repair.")
    check.equal(rows[-1]["body"], {"format": "openai_chat_stream/1", "chunks": values[:consumed]},
                msg="The response preserves the exact consumed raw prefix, including unsupported or null field evidence.")


@pytest.mark.parametrize("count", [64, 65], ids=["exact-provisional-cap", "provisional-overflow"])
def test_stream_provisional_tool_keys_share_existing_capacity(recording_factory, run_sdk_async, count):
    rec, path = recording_factory()
    first = chunk([delta(tools=[{"index": index, "id": "prefix-"} for index in range(count)])])
    confirmed = chunk([delta(tools=[{"index": index, **function_call("{}", reference=str(index))}
                                   for index in range(count)], finish="tool_calls")])
    values = [first, confirmed]
    evidence = ModelEvidence()
    encode = lambda value: json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
    initial_chunk_bytes = len(encode(first))
    initial_fragment_bytes = 64 * len(encode("prefix-"))

    async def run():
        async with native_client(stream_handler(CountedBytes(values))) as client:
            decoded = decoded_chunks(client)
            stream = await wrap_model(rec, client.chat.completions.create, instance_id="model")(
                model="fixture-model", messages=[], stream=True, evidence=evidence)
            returned = await stream.__anext__()
            check.is_true(returned is decoded[0], msg="A provisional-key cardinality boundary cannot trim or replace the native initial chunk.")
            check.equal(len(returned.choices[0].delta.tool_calls), count, msg="The provider's 65th key still reaches the host even when capture is bounded at 64.")
            check.equal(len(stream._capture.tools), 64, msg="ID-only provisional keys consume the same 64-key pool before any function fields arrive.")
            check.equal((stream._capture.chunk_bytes, stream._capture.fragment_bytes),
                        (initial_chunk_bytes, initial_fragment_bytes),
                        msg="The retained first chunk and all 64 provisional ID encodings are charged to their independent pools.")
            check.equal(evidence.decisions, (), msg="Provisional admission alone cannot fabricate completed decision receipts.")
            returned = await stream.__anext__()
            check.is_true(returned is decoded[1], msg="Late function fields remain native host input even after capture overflow freezes its prefix.")
            check.equal(len(stream._capture.tools), 64, msg="Late type/function confirmation reuses existing positional capacity rather than allocating a second pool.")
            check.equal(len(evidence.decisions), 64 if count == 64 else 0, msg="Only a still-observing exact-cap call may publish complete receipts before its finishing chunk is yielded.")
            check.equal(stream._capture.fragment_bytes, 0 if count == 64 else initial_fragment_bytes,
                        msg="Exact-cap confirmation releases fragments; stopped overflow preserves its pending prefix until terminal emission.")
            check.equal(stream._capture.chunk_bytes, initial_chunk_bytes + (len(encode(confirmed)) if count == 64 else 0),
                        msg="Fragment emission never reduces the retained raw-chunk pool.")
            with pytest.raises(StopAsyncIteration):
                await stream.__anext__()
            check.equal((stream._capture.chunks, stream._capture.tools, stream._capture.choices), ([], {}, {}),
                        msg="Terminal submission releases the retained chunks, provisional fragments and choice state.")

    run_sdk_async(run())
    rows, _, _ = persisted(rec, path)
    decisions = [row for row in rows if row["kind"] == "model.tool_call"]
    assert len(decisions) == len(evidence.decisions) == 64, "Both exact-cap and frozen-overflow calls retain exactly the first 64 key occurrences."
    check.equal([(entry.choice_index, entry.tool_index) for entry in evidence.decisions], [(0, index) for index in range(64)],
                msg="Provisional capacity preserves the first-observed positional prefix and emits each retained key once.")
    check.equal([entry.provider_ref for entry in evidence.decisions],
                ["prefix-" + str(index) for index in range(64)] if count == 64 else ["prefix-"] * 64,
                msg="Exact-cap confirmation retains all ID suffixes; overflow cannot splice unretained later fragments into the prefix.")
    check.equal([row["body"] for row in decisions], [{"arguments_raw": "{}"}] * 64 if count == 64 else [{}] * 64,
                msg="Only consumed and admitted function fragments become raw decision arguments.")
    check.is_true(all(row["details"]["complete"] == (count == 64) for row in decisions), msg="Provisional overflow leaves all pending keys partial while the exact existing key cap can finish completely.")
    check.is_true(all((row["details"]["args_digest"]["value"] is not None) == (count == 64) for row in decisions),
                  msg="Available argument tokens agree with the completed or frozen provisional evidence for every retained key.")
    check.equal(evidence.losses, () if count == 64 else ("decision_limit", "partial_tool_call"),
                msg="The existing decision_limit code accounts for provisional overflow, followed by partial emission of unfinished retained keys.")
    check.equal(rows[-1]["state"], "complete" if count == 64 else "partial", msg="Successful native exhaustion cannot erase a provisional key capacity loss.")
    check.equal(rows[-1]["body"], {"format": "openai_chat_stream/1", "chunks": values if count == 64 else [first]},
                msg="The existing first-overflow rule retains the raw admitted prefix and never resumes capture for later confirmations.")


@pytest.mark.parametrize("overflow", [False, True], ids=["exact-provisional-byte-cap", "one-byte-provisional-overflow"])
def test_stream_provisional_id_fragment_uses_independent_budget(recording_factory, run_sdk_async, overflow):
    rec, path = recording_factory()
    cap = 1_048_576
    prefix = "prefix-"
    first = chunk([delta(tools=[{"index": 0, "id": prefix}])], padding="")
    encode = lambda value: json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
    fragment_size = len(encode(prefix))
    first["padding"] = "x" * (cap - len(encode(first)) + int(overflow))
    encoded_chunk_size = len(encode(first))
    evidence = ModelEvidence()

    async def run():
        peer = CountedBytes([first])
        async with native_client(stream_handler(peer)) as client:
            decoded = decoded_chunks(client)
            stream = await wrap_model(rec, client.chat.completions.create, instance_id="model")(
                model="fixture-model", messages=[], stream=True, evidence=evidence)
            returned = await stream.__anext__()
            check.is_true(returned is decoded[0], msg="A provisional fragment byte overflow cannot replace the actual native chunk.")
            check.equal((stream._capture.chunk_bytes, stream._capture.fragment_bytes),
                        (0 if overflow else cap, fragment_size),
                        msg="A provisional ID fits its own pool even when the independently encoded raw chunk exceeds its cap.")
            assert stream._capture.body_stopped == overflow
            assert stream._capture.stopped is False and stream._capture.selection_gap is False
            check.equal(evidence.decisions, (), msg="An unconfirmed positional key has no decision receipt until the explicit terminal boundary.")
            await stream.aclose()
            check.equal(peer.reads, 1, msg="The boundary control closes after its single exposed prefix without draining DONE or later native frames.")

    run_sdk_async(run())
    rows, _, _ = persisted(rec, path)
    decisions = [row for row in rows if row["kind"] == "model.tool_call"]
    assert len(decisions) == 1, "The admitted provisional key emits exactly one partial receipt at early close in both raw-boundary cases."
    check.equal(encoded_chunk_size, cap + int(overflow), msg="The independent fixture encoding straddles the raw-chunk limit by exactly one byte.")
    check.equal(decisions[0]["details"]["provider_ref"], prefix,
                msg="Both raw-boundary cases retain the independently admitted provisional ID fragment.")
    check.equal(decisions[0]["body"], {}, msg="A provisional ID-only prefix cannot invent raw function arguments during early closure.")
    check.is_false(decisions[0]["details"]["complete"], msg="Neither exact-cap prefix retention nor overflow proves an observed complete function call.")
    assert decisions[0]["details"]["args_digest"]["value"] is None
    check.equal("stream_capture_limit" in evidence.losses, overflow, msg="Only the one-byte excess adds the assigned stream byte-limit loss.")
    check.equal(rows[-1]["state"], "partial", msg="Closing a provisional prefix remains partial regardless of whether its exact byte charge fit.")
    check.equal(rows[-1]["body"], {"format": "openai_chat_stream/1", "chunks": [] if overflow else [first]},
                msg="Only an entire raw chunk at or below its own cap is retained; provisional fragments do not share that admission.")


@pytest.mark.parametrize("overflow", [False, True], ids=["exact-byte-cap", "one-byte-overflow"])
def test_stream_byte_limit_exact_boundary(recording_factory, run_sdk_async, overflow):
    rec, path = recording_factory()
    cap = 1_048_576
    value = chunk([delta(content="", finish="stop")])
    base = len(json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8"))
    value["choices"][0]["delta"]["content"] = "x" * (cap - base + int(overflow))
    expected_bytes = len(json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8"))
    evidence = ModelEvidence()

    async def run():
        async with native_client(stream_handler(CountedBytes([value]))) as client:
            decoded = decoded_chunks(client)
            stream = await wrap_model(rec, client.chat.completions.create, instance_id="model")(
                model="fixture-model", messages=[], stream=True, evidence=evidence)
            observed = await stream.__anext__()
            check.is_true(observed is decoded[0], msg="Even an oversized observed chunk is yielded unchanged to the host.")
            check.equal((stream._capture.chunk_bytes, stream._capture.fragment_bytes), (0 if overflow else cap, 0),
                        msg="Chunk admission uses its single exact UTF-8 encoding; the cap is inclusive and overflow retains none of that chunk.")
            assert stream._capture.body_stopped == overflow
            assert stream._capture.stopped is False and stream._capture.selection_gap is False
            assert stream._capture.choices == {0: "stop"}
            with pytest.raises(StopAsyncIteration):
                await stream.__anext__()
            check.equal((stream._capture.chunks, stream._capture.tools, stream._capture.choices), ([], {}, {}),
                        msg="Terminal submission releases actual retained buffers at both the exact byte cap and overflow.")
            assert (stream._capture.chunk_bytes, stream._capture.fragment_bytes) == (0, 0)

    run_sdk_async(run())
    rows, _, _ = persisted(rec, path)
    check.equal(expected_bytes, cap + int(overflow), msg="The independently encoded fixture truly straddles the assigned one-MiB boundary by one byte.")
    check.equal(rows[-1]["body"]["chunks"], [] if overflow else [value], msg="The persisted prefix is exact and never silently shortened while claiming completion.")
    check.equal(rows[-1]["state"], "partial" if overflow else "complete", msg="A byte overflow is explicit partial evidence even when native consumption finishes normally.")
    check.equal("stream_capture_limit" in evidence.losses, overflow, msg="The bounded stream-loss code distinguishes exact-cap admission from overflow.")


@pytest.mark.parametrize("capture,placement", [("content", "overflow-chunk"), ("content", "later-chunk"),
                                               ("metadata", "overflow-chunk"), ("metadata", "later-chunk")],
                         ids=["content-overflow-chunk", "content-later-chunk",
                              "metadata-overflow-chunk", "metadata-later-chunk"])
def test_stream_late_tool_survives_body_overflow(recording_factory, run_sdk_async, monkeypatch, capture, placement):
    from mcp_flightrecorder.integrations import _model_stream

    rec, path = recording_factory(capture=capture)
    cap = 1_048_576
    raw = '{ "text": "late-λ", "n": 2 }'
    tool = {"index": 0, **function_call(raw=raw, name="echo", reference="late-reference")}
    first = chunk([delta(content="prefix")])
    if placement == "overflow-chunk":
        values = [first, chunk([delta(content="x" * cap, tools=[tool], finish="tool_calls")])]
    else:
        values = [first, chunk([delta(content="x" * cap)]),
                  chunk([delta(tools=[tool], finish="tool_calls")])]
    values.append(chunk(usage={"prompt_tokens": 2, "completion_tokens": 1, "total_tokens": 3}))
    evidence, policy_calls, full_encodings = ModelEvidence(), [], []
    original = _model_stream.encode_json

    def encode(value):
        if type(value) is dict:
            full_encodings.append(value)
        return original(value)

    def policy(kind, payload, context):
        policy_calls.append((kind, json.loads(json.dumps(payload, ensure_ascii=False, allow_nan=False))))
        return {"policy_id": "late-stream", "retained": payload}

    monkeypatch.setattr(_model_stream, "encode_json", encode)

    async def run():
        peer = CountedBytes(values)
        async with native_client(stream_handler(peer)) as client:
            decoded = decoded_chunks(client)
            spy, calls = observed_method(client.chat.completions.create)
            stream = await wrap_model(rec, spy, instance_id="model", retention_policy=policy)(
                model="fixture-model", messages=[], stream=True, evidence=evidence)
            assert peer.reads == 0
            consumed = []
            for _ in values[:-1]:
                consumed.append(await stream.__anext__())
            # This prerequisite must fail meaningfully on the installed old implementation.
            assert len(evidence.decisions) == 1
            accepted(evidence.decisions[0].receipt)
            assert evidence.response is None
            assert (stream._capture.body_stopped, stream._capture.stopped, stream._capture.selection_gap) == (True, False, False)
            assert stream._capture.fragment_bytes == 0
            consumed.append(await stream.__anext__())
            assert evidence.response is None
            assert len(consumed) == len(decoded) == len(values)
            assert all(actual is expected for actual, expected in zip(consumed, decoded))
            assert peer.reads == len(values)
            with pytest.raises(StopAsyncIteration):
                await stream.__anext__()
            response = accepted(evidence.response)
            await stream.close()
            await stream.aclose()
            assert evidence.response is response and len(evidence.decisions) == 1
            assert (stream._capture.chunks, stream._capture.tools, stream._capture.choices) == ([], {}, {})
            assert (stream._capture.chunk_bytes, stream._capture.fragment_bytes) == (0, 0)
            assert len(calls["kwargs"]) == 1
            return [item.to_dict(mode="json", use_api_names=True, exclude_unset=True) for item in consumed]

    normalized = run_sdk_async(run())
    rows, artifacts, _ = persisted(rec, path)
    assert len(full_encodings) == 2
    assert full_encodings == normalized[:2]
    assert [(entry.choice_index, entry.tool_index, entry.provider_ref) for entry in evidence.decisions] == [(0, 0, "late-reference")]
    assert [row["kind"] for row in rows] == ["model.request", "model.tool_call", "model.response"]
    decision, response = rows[1:]
    assert decision["name"] == "echo" and decision["details"]["provider_ref"] == "late-reference"
    assert decision["details"]["complete"] is True and decision["details"]["args_digest"]["value"] is not None
    assert response["state"] == "partial"
    assert evidence.losses == ("stream_capture_limit",)
    aggregate = {"format": "openai_chat_stream/1", "chunks": normalized[:1]}
    assert policy_calls == [("model.request", {"model": "fixture-model", "messages": [], "stream": True}),
                            ("model.tool_call", {"arguments_raw": raw}), ("model.response", aggregate)]
    if capture == "content":
        assert decision["body"] == {"arguments_raw": raw} and response["body"] == aggregate
    else:
        assert decision["body"] is None and response["body"] is None
        assert sum(a["body_text"] is not None and a["representation"] != "request_refs/2" for a in artifacts.values()) == 0


def test_stream_pending_tool_completes_across_body_overflow(recording_factory, run_sdk_async):
    rec, path = recording_factory()
    first = chunk([delta(tools=[{"index": 0, **function_call('{"text":"prefix', reference="ref")}])])
    too_large = chunk([delta(content="x" * 1_048_576)])
    final = chunk([delta(tools=[{"index": 0, "function": {"arguments": '"}'}}], finish="tool_calls")])
    evidence = ModelEvidence()
    encoded = lambda value: json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
    chunk_bytes = len(encoded(first))
    fragment_bytes = sum(len(encoded(value)) for value in ("ref", "echo", '{"text":"prefix'))

    async def run():
        peer = CountedBytes([first, too_large, final])
        async with native_client(stream_handler(peer)) as client:
            stream = await wrap_model(rec, client.chat.completions.create, instance_id="model")(
                model="fixture-model", messages=[], stream=True, evidence=evidence)
            await stream.__anext__()
            check.equal((stream._capture.chunk_bytes, stream._capture.fragment_bytes), (chunk_bytes, fragment_bytes),
                        msg="ID, name and raw-argument encodings are charged separately from the retained chunk buffer.")
            await stream.__anext__()
            check.equal((stream._capture.chunk_bytes, stream._capture.fragment_bytes), (chunk_bytes, fragment_bytes),
                        msg="Raw overflow preserves both already admitted pools without retaining the oversized chunk.")
            assert (stream._capture.body_stopped, stream._capture.stopped, stream._capture.selection_gap) == (True, False, False)
            await stream.__anext__()
            assert len(evidence.decisions) == 1
            accepted(evidence.decisions[0].receipt)
            check.equal((stream._capture.chunk_bytes, stream._capture.fragment_bytes), (chunk_bytes, 0),
                        msg="The later observed suffix and finish complete the pending tool, releasing only its fragments.")
            with pytest.raises(StopAsyncIteration):
                await stream.__anext__()

    run_sdk_async(run())
    rows, _, _ = persisted(rec, path)
    decisions = [row for row in rows if row["kind"] == "model.tool_call"]
    assert len(decisions) == 1, "The retained pending tool key must emit exactly once after overflow."
    check.equal(decisions[0]["body"], {"arguments_raw": '{"text":"prefix"}'}, msg="Selection inspection observes the exact suffix even after raw retention closes.")
    check.is_true(decisions[0]["details"]["complete"], msg="Body-only loss leaves fully observed tool selection complete.")
    assert decisions[0]["details"]["args_digest"]["value"] is not None
    assert evidence.losses == ("stream_capture_limit",)
    check.equal(rows[-1]["body"]["chunks"], [first], msg="Only the already retained ordered prefix survives the byte limit.")
    check.equal(rows[-1]["state"], "partial", msg="Successful native exhaustion cannot erase stream capture overflow.")


@pytest.mark.parametrize("overflow", [False, True], ids=["exact-fragment-cap", "one-byte-fragment-overflow"])
def test_stream_pending_fragment_budget_exact_boundary(recording_factory, run_sdk_async, overflow):
    rec, path = recording_factory()
    cap = 1_048_576
    encoded = lambda value: json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
    length = lambda value: len(encoded(value))
    base = '{"text":"λ"}'
    overhead = length("first") + length("echo") + length("second") + length("echo")
    raw0 = '{"text":"λ' + "x" * (cap // 2 - length(base)) + '"}'
    raw1 = '{"text":"λ' + "x" * (cap - cap // 2 - overhead + int(overflow) - length(base)) + '"}'
    assert length(raw0) == cap // 2
    assert length(raw1) == cap - cap // 2 - overhead + int(overflow)
    first = chunk([delta(index=0, tools=[{"index": 0, **function_call(raw0, reference="first")}])])
    values = [first,
              chunk([delta(index=1, tools=[{"index": 0, **function_call(raw1, reference="second")}])]),
              chunk([delta(index=0, finish="tool_calls"), delta(index=1, finish="tool_calls")]),
              chunk([delta(index=2, tools=[{"index": 0, **function_call("{}", reference="third")}], finish="tool_calls")])]
    evidence = ModelEvidence()

    async def run():
        async with native_client(stream_handler(CountedBytes(values))) as client:
            decoded = decoded_chunks(client)
            stream = await wrap_model(rec, client.chat.completions.create, instance_id="model")(
                model="fixture-model", messages=[], stream=True, evidence=evidence)
            consumed = [await stream.__anext__()]
            assert stream._capture.chunk_bytes == length(first)
            assert stream._capture.fragment_bytes == length("first") + length("echo") + length(raw0)
            consumed.append(await stream.__anext__())
            pending_bytes = cap // 2 + overhead if overflow else cap
            assert stream._capture.fragment_bytes == pending_bytes
            assert stream._capture.chunk_bytes == length(first)
            assert stream._capture.body_stopped is True
            assert stream._capture.stopped == stream._capture.selection_gap == overflow
            assert list(stream._capture.tools) == [(0, 0), (1, 0)]
            assert all(not tool.emitted for tool in stream._capture.tools.values())
            assert evidence.decisions == ()
            consumed.append(await stream.__anext__())
            assert len(evidence.decisions) == (0 if overflow else 2)
            assert stream._capture.fragment_bytes == (pending_bytes if overflow else 0)
            consumed.append(await stream.__anext__())
            assert len(evidence.decisions) == (0 if overflow else 3)
            assert list(stream._capture.tools) == ([(0, 0), (1, 0)] if overflow else [(0, 0), (1, 0), (2, 0)])
            assert stream._capture.fragment_bytes == (pending_bytes if overflow else 0)
            assert stream._capture.chunk_bytes == length(first)
            assert len(consumed) == len(decoded) == 4
            assert all(actual is expected for actual, expected in zip(consumed, decoded))
            with pytest.raises(StopAsyncIteration):
                await stream.__anext__()
            await stream.aclose()
            assert (stream._capture.chunks, stream._capture.tools, stream._capture.choices) == ([], {}, {})
            assert (stream._capture.chunk_bytes, stream._capture.fragment_bytes) == (0, 0)

    run_sdk_async(run())
    rows, _, _ = persisted(rec, path)
    decisions = [row for row in rows if row["kind"] == "model.tool_call"]
    assert len(decisions) == len(evidence.decisions) == (2 if overflow else 3)
    assert [(entry.choice_index, entry.tool_index, entry.provider_ref) for entry in evidence.decisions] == (
        [(0, 0, "first"), (1, 0, "second")] if overflow else [(0, 0, "first"), (1, 0, "second"), (2, 0, "third")])
    assert all(accepted(entry.receipt) for entry in evidence.decisions)
    assert [row["name"] for row in decisions] == ["echo"] * len(decisions)
    assert [row["body"] for row in decisions] == (
        [{"arguments_raw": raw0}, {}] if overflow else
        [{"arguments_raw": raw0}, {"arguments_raw": raw1}, {"arguments_raw": "{}"}])
    assert [row["details"]["complete"] for row in decisions] == [not overflow] * len(decisions)
    assert [row["details"]["args_digest"]["value"] is not None for row in decisions] == [not overflow] * len(decisions)
    assert rows[-1]["state"] == "partial"
    assert rows[-1]["body"] == {"format": "openai_chat_stream/1", "chunks": [first]}
    assert evidence.losses == (("stream_capture_limit", "partial_tool_call") if overflow else ("stream_capture_limit",))


@pytest.mark.parametrize("count", [64, 65], ids=["exact-tool-cap", "tool-overflow"])
def test_stream_tool_key_limit_is_call_local_and_bounded(recording_factory, run_sdk_async, count):
    rec, path = recording_factory()
    value = chunk([delta(tools=[{"index": index, **function_call("{}", reference="reused")} for index in range(count)], finish="tool_calls")])
    evidence = ModelEvidence()

    async def run():
        async with native_client(stream_handler(CountedBytes([value]))) as client:
            stream = await wrap_model(rec, client.chat.completions.create, instance_id="model")(
                model="fixture-model", messages=[], stream=True, evidence=evidence)
            returned = await stream.__anext__()
            check.equal(len(returned.choices[0].delta.tool_calls), count, msg="Tool collector limits cannot trim the actual native chunk.")
            check.equal(len(stream._capture.tools), 64, msg="Live call state retains no more than 64 positional tool keys, including already emitted keys.")
            assert 0 <= stream._capture.chunk_bytes <= 1_048_576
            assert 0 <= stream._capture.fragment_bytes <= 1_048_576
            assert stream._capture.chunk_bytes + stream._capture.fragment_bytes <= 2_097_152
            with pytest.raises(StopAsyncIteration):
                await stream.__anext__()

    run_sdk_async(run())
    rows, _, _ = persisted(rec, path)
    decisions = [row for row in rows if row["kind"] == "model.tool_call"]
    check.equal(len(evidence.decisions), 64, msg="Only the bounded first-observed key prefix receives call-local decision entries.")
    check.equal(len(decisions), 64, msg="Each retained tool key emits exactly one decision event at completion or partial termination.")
    check.equal(rows[-1]["state"], "complete" if count == 64 else "partial", msg="The exact key cap remains complete; the first excess key freezes partial evidence.")
    check.equal("decision_limit" in evidence.losses, count > 64, msg="Key overflow uses decision_limit rather than inventing a provider-limit claim.")
    check.is_true(all(row["details"]["complete"] == (count == 64) for row in decisions), msg="Keys pending at overflow remain partial even though the native chunk carries a later finish marker.")


@pytest.mark.parametrize("count", [64, 65], ids=["exact-choice-cap", "choice-overflow"])
def test_stream_choice_states_are_bounded_without_tool_calls(recording_factory, run_sdk_async, count):
    rec, path = recording_factory()
    value = chunk([delta(index=index, content="observed", finish="stop") for index in range(count)])
    evidence = ModelEvidence()

    async def run():
        async with native_client(stream_handler(CountedBytes([value]))) as client:
            stream = await wrap_model(rec, client.chat.completions.create, instance_id="model")(
                model="fixture-model", messages=[], stream=True, evidence=evidence)
            returned = await stream.__anext__()
            check.equal(len(returned.choices), count, msg="Choice-state limits cannot alter native response choices.")
            check.equal(len(stream._capture.choices), 64, msg="Even no-tool streams retain at most 64 observed choice states.")
            assert 0 <= stream._capture.chunk_bytes <= 1_048_576
            assert 0 <= stream._capture.fragment_bytes <= 1_048_576
            assert stream._capture.chunk_bytes + stream._capture.fragment_bytes <= 2_097_152
            with pytest.raises(StopAsyncIteration):
                await stream.__anext__()

    run_sdk_async(run())
    rows, _, _ = persisted(rec, path)
    check.equal(evidence.decisions, (), msg="Text-only choice accounting cannot create synthetic tool decisions.")
    check.equal(rows[-1]["state"], "complete" if count == 64 else "partial", msg="Only the exact state bound can retain complete choice-finish coverage.")
    check.equal("stream_capture_limit" in evidence.losses, count > 64, msg="Choice-state overflow uses the assigned stream_capture_limit code.")


def test_chunk_projection_failure_and_close_after_recorder_shutdown(recording_factory, run_sdk_async, monkeypatch):
    rec, path = recording_factory()
    values = [chunk([delta(content="first")]), chunk([delta(content="last", finish="stop")])]
    evidence = ModelEvidence()
    original = ChatCompletionChunk.to_dict

    def projected(self, **kwargs):
        if self.choices[0].delta.content == "first":
            raise ValueError("controlled first-chunk projection failure")
        return original(self, **kwargs)

    monkeypatch.setattr(ChatCompletionChunk, "to_dict", projected)

    async def run():
        peer = CountedBytes(values)
        async with native_client(stream_handler(peer)) as client:
            stream = await wrap_model(rec, client.chat.completions.create, instance_id="model")(
                model="fixture-model", messages=[], stream=True, evidence=evidence)
            native = await stream.__anext__()
            check.equal(native.choices[0].delta.content, "first", msg="A failed chunk projection must still return the native chunk's usable fields.")
            check.is_in("projection_failed", evidence.losses, msg="Trusted chunk projection failure is explicit before yielding that chunk.")
            closed = rec.close(timeout=3)
            assert closed.portable, "The shutdown control must establish a portable recording before later native cleanup."
            before = persisted(rec, path)[0]
            await stream.aclose()
            check.equal(peer.reads, 1, msg="Closing after recorder shutdown must not drain unconsumed native chunks.")
            check.equal(peer.closes, 1, msg="Native HTTP cleanup still occurs after recording has stopped.")
            check.is_false(client.is_closed(), msg="Closing the native stream cannot close the caller's entire SDK client.")
            check.equal(evidence.response.status, "closed", msg="Post-shutdown terminal capture preserves the core's closed receipt instead of writing to a finalized recording.")
            check.equal(persisted(rec, path)[0], before, msg="Cleanup after recorder closure cannot mutate already portable recorded events.")

    run_sdk_async(run())


def test_chunk_projection_failure_persists_partial_terminal(recording_factory, run_sdk_async, monkeypatch):
    rec, path = recording_factory()
    values = [chunk([delta(content="first")]), chunk([delta(content="last", finish="stop")])]
    evidence, policy_calls = ModelEvidence(), []

    def policy(kind, payload, context):
        policy_calls.append((kind, json.loads(json.dumps(payload))))
        return {"policy_id": "available-stream", "retained": payload}
    original = ChatCompletionChunk.to_dict

    def projected(self, **kwargs):
        if self.choices[0].delta.content == "first":
            raise ValueError("controlled trusted projection failure")
        return original(self, **kwargs)

    monkeypatch.setattr(ChatCompletionChunk, "to_dict", projected)

    async def consume(decorated):
        peer = CountedBytes(values)
        async with native_client(stream_handler(peer)) as client:
            decoded = decoded_chunks(client)
            method = (wrap_model(rec, client.chat.completions.create, instance_id="model", retention_policy=policy) if decorated
                      else client.chat.completions.create)
            stream = await method(model="fixture-model", messages=[], stream=True,
                                  **({"evidence": evidence} if decorated else {}))
            returned = [value async for value in stream]
            check.equal(len(returned), 2, msg="Trusted projection failure cannot drop a native consumed chunk from the host's iteration.")
            check.is_true(all(actual is expected for actual, expected in zip(returned, decoded)),
                          msg="Even the chunk whose projection failed is returned as its original native SDK object.")
            return peer.reads, peer.closes

    async def run():
        native = await consume(False)
        decorated = await consume(True)
        check.equal(decorated, native, msg="Projection failure adds no transport reads or cleanup calls beyond the identical actual native SDK baseline.")

    run_sdk_async(run())
    rows, _, _ = persisted(rec, path)
    assert [kind for kind, _ in policy_calls] == ["model.request", "model.response"]
    assert policy_calls[-1][1] == {"format": "openai_chat_stream/1", "chunks": [values[1]]}
    check.equal([row["kind"] for row in rows], ["model.request", "model.response"], msg="Projection failure produces one terminal rather than duplicate capture attempts.")
    check.is_in("projection_failed", evidence.losses, msg="The unavailable trusted projection has its fixed side-channel loss code.")
    check.equal(rows[-1]["body"], {"format": "openai_chat_stream/1", "chunks": [values[1]]},
                msg="Only successfully exposed JSON projections are retained; the failed chunk is never serialized through an arbitrary fallback.")
    check.equal(rows[-1]["state"], "partial", msg="A later valid finish witness cannot erase an earlier stream projection gap.")
    check.is_false(rows[-1]["details"]["complete"], msg="The persisted terminal explicitly rejects a complete-capture claim after projection loss.")


def test_repeated_stream_projection_losses_log_once_per_invocation(
        recording_factory, run_sdk_async, monkeypatch, caplog):
    rec, path = recording_factory()
    values = [chunk([delta(content="first")]), chunk([delta(content="second")]),
              chunk([delta(content="last", finish="stop")])]
    original = ChatCompletionChunk.to_dict

    def projected(self, **kwargs):
        if self.choices[0].delta.content != "last":
            raise ValueError("private rejected chunk")
        return original(self, **kwargs)

    monkeypatch.setattr(ChatCompletionChunk, "to_dict", projected)

    async def run():
        peer = CountedBytes(values)
        async with native_client(stream_handler(peer)) as client:
            decoded = decoded_chunks(client)
            stream = await wrap_model(rec, client.chat.completions.create, instance_id="model")(
                model="fixture-model", messages=[], stream=True)
            returned = [value async for value in stream]
            assert len(returned) == len(values)
            assert all(actual is expected for actual, expected in zip(returned, decoded))

    with caplog.at_level(logging.WARNING, logger="mcp_flightrecorder.integrations"):
        run_sdk_async(run())
    messages = [record.getMessage() for record in caplog.records
                if record.name.startswith("mcp_flightrecorder.integrations.")]
    assert messages == ["integration_capture_loss reason=projection_failed"]
    rows, _, _ = persisted(rec, path)
    assert rows[-1]["state"] == "partial"
    assert rows[-1]["body"] == {"format": "openai_chat_stream/1", "chunks": [values[-1]]}


@pytest.mark.parametrize("outcome", ["error", "cancel"])
def test_stream_create_failure_before_return_has_one_terminal(recording_factory, run_sdk_async, outcome):
    rec, path = recording_factory()
    evidence, calls = ModelEvidence(), []

    async def run():
        entered, release = asyncio.Event(), asyncio.Event()

        async def handler(request):
            calls.append(request)
            entered.set()
            if outcome == "cancel":
                await release.wait()
            return httpx2.Response(400, json={"error": {"message": "before stream", "type": "invalid_request_error"}})

        async with native_client(handler) as client:
            spy, seen = observed_method(client.chat.completions.create)
            pending = asyncio.create_task(wrap_model(rec, spy, instance_id="model")(
                model="fixture-model", messages=[], stream=True, evidence=evidence))
            await asyncio.wait_for(entered.wait(), 2)
            if outcome == "cancel":
                pending.cancel("before stream cancellation")
            try:
                await pending
            except BaseException as error:
                check.is_true(error is seen["errors"][0], msg="A stream-creation failure propagates the actual native exception or cancellation object.")
            else:
                pytest.fail("The actual native stream-creation failure must reach its caller.")
            finally:
                release.set()

    run_sdk_async(run())
    rows, _, _ = persisted(rec, path)
    check.equal(len(calls), 1, msg="Observing a failed stream creation must not retry the SDK call.")
    check.equal([row["kind"] for row in rows], ["model.request", "model.response"], msg="Even a create that never returns a stream closes its admitted request exactly once.")
    check.equal(rows[-1]["state"], "error" if outcome == "error" else "cancelled", msg="The terminal preserves the native pre-stream failure category.")


@pytest.mark.parametrize("body_overflow", [False, True], ids=["body-retained", "body-overflow"])
@pytest.mark.parametrize("gap", [False, True], ids=["valid-neighbor", "missing-selection-prefix"])
def test_stream_projection_gap_taints_later_keys_without_erasing_completed_selection(
        recording_factory, run_sdk_async, monkeypatch, gap, body_overflow):
    """Shared-engine extraction regression: a missing chunk can hide a new key's prefix."""
    rec, path = recording_factory()
    known = chunk([delta(tools=[{"index": 0, **function_call("{}", reference="known")}], finish="tool_calls")])
    missing = chunk([delta(index=1, tools=[{"index": 0, **function_call(" ", name="prefix-", reference="prefix-")}])])
    missing["id"] = "missing-selection-prefix"
    final = chunk([delta(index=1, tools=[{"index": 0, **function_call("{}", reference="suffix")}], finish="tool_calls")])
    values, evidence = [known, missing, final], ModelEvidence()
    if body_overflow:
        values.insert(1, chunk(padding="x" * 1_048_576))
    original = ChatCompletionChunk.to_dict

    def projected(self, **kwargs):
        if gap and self.id == "missing-selection-prefix":
            raise ValueError("labelled projection failure hides a possible positional prefix")
        return original(self, **kwargs)

    monkeypatch.setattr(ChatCompletionChunk, "to_dict", projected)

    async def run():
        async with native_client(stream_handler(CountedBytes(values))) as client:
            decoded = decoded_chunks(client)
            stream = await wrap_model(rec, client.chat.completions.create, instance_id="model")(
                model="fixture-model", messages=[], stream=True, evidence=evidence)
            returned = [item async for item in stream]
            await stream.aclose()
            check.equal(len(returned), len(values), msg="Raw overflow and a failed trusted projection cannot remove native SDK output from host consumption.")
            check.is_true(all(actual is expected for actual, expected in zip(returned, decoded)),
                          msg="All consumed chunks preserve actual SDK object identity despite a capture gap.")

    run_sdk_async(run())
    rows, _, _ = persisted(rec, path)
    decisions = [row for row in rows if row["kind"] == "model.tool_call"]
    check.equal([(item.choice_index, item.tool_index) for item in evidence.decisions], [(0, 0), (1, 0)],
                msg="The earlier completed occurrence and later visible positional key remain distinct.")
    check.equal([row["details"]["complete"] for row in decisions], [True, not gap],
                msg="A selection completed before the gap stays complete; a key first seen after missing projection cannot recover a hidden prefix.")
    check.equal([row["details"]["args_digest"]["value"] is not None for row in decisions], [True, not gap],
                msg="Only independently complete argument observations retain available comparison tokens.")
    check.equal(rows[-1]["body"]["chunks"], [known] if body_overflow else [known, final] if gap else values,
                msg="The stored stream retains exactly the successfully exposed normalized projections.")
    check.equal(rows[-1]["state"], "partial" if gap or body_overflow else "complete",
                msg="The no-gap neighbor remains complete while a selection-affecting projection loss stays visible.")


def test_model_selective_retention_preserves_originals_and_detaches_output(
        recording_factory, run_sdk_async, monkeypatch):
    rec, path = recording_factory()
    marker = "model-private-Ω"
    messages = [{"role": "system", "content": marker}, {"role": "user", "content": marker}]
    tools = [{"type": "function", "function": {"name": "echo", "description": marker}}]
    raw = '{"text":"model-private-Ω"}'
    output = normal(tools=[function_call(raw)], finish="tool_calls", choice=3, content=marker)
    submitted = {"model": "fixture-model", "messages": messages, "tools": tools, "temperature": 0, "user": marker}
    snapshots, mappings, requests = [], [], []
    evidence = [ModelEvidence(), ModelEvidence()]
    entered, release = threading.Event(), threading.Event()
    original_write = rec._persist_bundle

    def held_write(connection, bundle):
        entered.set()
        assert release.wait(5), "The test must release its own held writer after mutating callback values."
        return original_write(connection, bundle)

    monkeypatch.setattr(rec, "_persist_bundle", held_write)

    def policy(kind, payload, context):
        snapshots.append((kind, json.loads(json.dumps(payload)), dict(context)))
        context.clear()
        context["instance_id"] = "callback-mutated"
        if kind == "model.request":
            payload["messages"][0]["content"] = "SAFE-SYSTEM"
            payload["messages"][1]["content"] = "SAFE-USER"
            payload["tools"] = []
            payload["model"], payload["user"] = "stored-model", "safe-user"
        elif kind == "model.tool_call":
            payload["arguments_raw"] = '{"text":"MASKED"}'
        else:
            payload["choices"][0]["message"]["content"] = "callback mutation"
            payload = {"safe": "response"}
        mapping = {"policy_id": "model-mask", "retained": payload, "redacted": True,
                   "truncated": kind == "model.response"}
        mappings.append(mapping)
        return mapping

    async def handler(request):
        requests.append(json.loads(request.content))
        return httpx2.Response(200, json=output)

    async def run():
        async with native_client(handler) as client:
            spy, seen = observed_method(client.chat.completions.create)
            baseline = await wrap_model(rec, spy, instance_id="model", retention_policy=None)(
                **submitted, evidence=evidence[0])
            assert baseline is seen["results"][0] and not snapshots
            original_decision = accepted(evidence[0].decisions[0].receipt)
            source = accepted(rec.observe("host.tool_result", scope=rec.capture_scope(), payload=marker,
                facts={"retention": {"policy_id": "source-omission", "omitted_reason": "private-source"}}))
            annotations = [{"bindings": [], "complete": True}, {"bindings": [{
                "decision_event_id": original_decision.event_id, "pointer": "/content",
                "result_event_id": None, "text_paths": [""]}], "complete": True}]
            selected = wrap_model(rec, spy, instance_id="model", retention_policy=policy)
            assert inspect.signature(selected) == inspect.signature(client.chat.completions.create)
            result = await selected(**submitted, evidence=evidence[1], annotations=annotations,
                insertions=[{"source_event_id": source.event_id, "target_pointer": "/messages/1/content",
                             "content": marker}])
            assert result is seen["results"][1]
            assert baseline.to_dict(mode="json", use_api_names=True, exclude_unset=True) == output
            assert result.to_dict(mode="json", use_api_names=True, exclude_unset=True) == output
            assert seen["kwargs"] == [submitted, submitted] and requests == [submitted, submitted]
            assert messages == [{"role": "system", "content": marker}, {"role": "user", "content": marker}]
            assert tools[0]["function"]["description"] == marker
            assert entered.wait(2), "The writer must be held before callback-owned objects are mutated."
            for mapping in mappings:
                mapping["retained"].clear()
                mapping.update(policy_id="late-policy", redacted=False, truncated=False)
            return source.event_id, original_decision.event_id

    try:
        source_id, decision_id = run_sdk_async(run())
    finally:
        release.set()
    rows, artifacts, links = persisted(rec, path)
    assert [row["kind"] for row in rows] == ["model.request", "model.tool_call", "model.response",
        "host.tool_result", "model.request", "model.tool_call", "model.response"]
    baseline_request, baseline_decision, _, _, selected_request, selected_decision, selected_response = rows
    assert request_body(baseline_request, artifacts) == submitted
    assert len(selected_request["body"]["message_refs"]) == 2
    originals = [
        '{"model":"fixture-model","messages":null,"tools":null,"temperature":0,"user":"model-private-Ω"}'.encode(),
        '{"role":"system","content":"model-private-Ω"}'.encode(),
        '{"role":"user","content":"model-private-Ω"}'.encode(),
        '[{"type":"function","function":{"name":"echo","description":"model-private-Ω"}}]'.encode(),
    ]
    replacements = [
        b'{"model":"stored-model","messages":null,"tools":null,"temperature":0,"user":"safe-user"}',
        b'{"role":"system","content":"SAFE-SYSTEM"}',
        b'{"role":"user","content":"SAFE-USER"}', b'[]',
    ]
    members = request_artifacts(selected_request, artifacts)
    for artifact, original, stored in zip(members, originals, replacements, strict=True):
        retained_artifact(artifact, original, stored, "model-mask", redacted=True)
    decision_original = '{"arguments_raw":"{\\"text\\":\\"model-private-Ω\\"}"}'.encode()
    retained_artifact(artifacts[selected_decision["payload_ref"]], decision_original,
        b'{"arguments_raw":"{\\"text\\":\\"MASKED\\"}"}', "model-mask", redacted=True)
    response_original = json.dumps(output, ensure_ascii=False, separators=(",", ":")).encode()
    retained_artifact(artifacts[selected_response["payload_ref"]], response_original,
        b'{"safe":"response"}', "model-mask", redacted=True, truncated=True)
    selected_bodies = [*members, artifacts[selected_decision["payload_ref"]], artifacts[selected_response["payload_ref"]]]
    assert all(marker not in artifact["body_text"] for artifact in selected_bodies)
    assert [kind for kind, _, _ in snapshots] == ["model.request", "model.tool_call", "model.response"]
    plain_context = {"instance_id": "model", "choice_index": None, "tool_index": None, "name": None, "provider_ref": None}
    assert [context for _, _, context in snapshots] == [plain_context,
        {"instance_id": "model", "choice_index": 3, "tool_index": 0, "name": "echo", "provider_ref": "same-provider-id"}, plain_context]
    assert snapshots[0][1] == submitted and snapshots[1][1] == {"arguments_raw": raw} and snapshots[2][1] == output
    left, right = baseline_decision["details"]["args_digest"], selected_decision["details"]["args_digest"]
    assert left == right and left["value"] and left["basis"] == "observed_original" and left.get("reason") is None
    assert baseline_decision["decision_id"] != selected_decision["decision_id"]
    assert baseline_request["model_call_id"] != selected_request["model_call_id"]
    assert {row["model_call_id"] for row in rows[-3:]} == {selected_request["model_call_id"]}
    assert all(row["details"]["complete"] for row in rows if row["boundary"] == "model")
    assert selected_response["state"] == "complete"
    inserted = [row for row in links if row["relation"] == "inserted_into"]
    assert [(row["source_event_id"], row["target_event_id"], row["target_pointer"]) for row in inserted] == [
        (source_id, selected_request["event_id"], "/messages/1/content")]
    answers = [row for row in links if row["relation"] == "answers"]
    assert [(row["source_event_id"], row["target_pointer"]) for row in answers] == [(decision_id, "/messages/1/content")]
    token = json.loads(answers[0]["details_json"])["submitted_text_digest"]
    assert token["value"] and token["basis"] == "observed_original" and token.get("reason") is None
    assert "insertion_diagnostics" not in selected_request["details"] and "unresolved_answers" not in selected_request["details"]
    with closing(sqlite3.connect(path)) as db:
        positions = db.execute("SELECT request_event_id,message_index,message_path FROM request_members ORDER BY request_sequence,message_index").fetchall()
    assert positions == [(row["event_id"], index, "/messages") for row in (baseline_request, selected_request) for index in range(2)]
    for item in evidence:
        assert item.losses == ()
        assert all(receipt.status == "accepted" for receipt in [item.request, item.decisions[0].receipt, item.response])
    closed, health = rec.close(timeout=3), rec.health()
    assert closed.run_close_state == "clean" and health.dropped_events == 0
    assert (health.ordinary.used_slots, health.ordinary.used_bytes, health.completion.used_slots, health.completion.used_bytes) == (0, 0, 0, 0)


@pytest.mark.parametrize("change", ["null", "missing-messages", "non-array", "count", "reorder", "missing-tools", "position-replacement"])
def test_model_request_retention_preserves_topology_or_omits_before_admission(
        recording_factory, run_sdk_async, monkeypatch, caplog, change):
    rec, path = recording_factory()
    marker = "model-topology-private"
    messages = [{"role": "system", "content": marker}, {"role": "user", "content": marker}]
    submitted = {"model": "fixture-model", "messages": messages, "tools": TOOLS}
    evidence, calls, requests = ModelEvidence(), [], []
    attempts, original_observe = [], rec.observe

    def observe(kind, **kwargs):
        attempts.append((kind, kwargs["facts"].get("retention")))
        return original_observe(kind, **kwargs)

    monkeypatch.setattr(rec, "observe", observe)

    def policy(kind, payload, context):
        calls.append(kind)
        if kind != "model.request":
            return {"policy_id": "topology", "omitted_reason": "not-retained"}
        if change == "null":
            payload = None
        elif change == "missing-messages":
            del payload["messages"]
        elif change == "non-array":
            payload["messages"] = {}
        elif change == "count":
            payload["messages"].pop()
        elif change == "reorder":
            payload["messages"].reverse()
        elif change == "missing-tools":
            del payload["tools"]
        else:
            payload["messages"] = [{"safe": "first"}, {"safe": "second"}]
            payload["tools"] = {"safe": "replacement catalog"}
        return {"policy_id": "topology", "retained": payload, "redacted": True}

    async def run():
        async with native_client(lambda request: requests.append(json.loads(request.content)) or httpx2.Response(200, json=normal())) as client:
            spy, seen = observed_method(client.chat.completions.create)
            result = await wrap_model(rec, spy, instance_id="model", retention_policy=policy)(**submitted, evidence=evidence)
            assert result is seen["results"][0] and len(seen["kwargs"]) == len(requests) == 1
            assert requests == [submitted]
            assert accepted(evidence.request).losses == (() if change == "position-replacement" else ("body_omitted",))
            accepted(evidence.response)

    with caplog.at_level(logging.WARNING, logger="mcp_flightrecorder.integrations"):
        run_sdk_async(run())
    rows, artifacts, _ = persisted(rec, path)
    assert [row["kind"] for row in rows] == ["model.request", "model.response"] and calls == ["model.request", "model.response"]
    members = request_artifacts(rows[0], artifacts)
    assert len(rows[0]["body"]["message_refs"]) == 2
    assert all(row["details"]["complete"] for row in rows) and rows[-1]["state"] == "complete"
    invalid = change != "position-replacement"
    assert [kind for kind, _ in attempts] == ["model.request", "model.response"]
    if invalid:
        assert attempts[0][1] == {"policy_id": "adapter_policy_error", "omitted_reason": "retention_policy_failed"}
    assert evidence.losses == (("retention_policy_failed",) if invalid else ())
    records = [r for r in caplog.records if r.name.startswith("mcp_flightrecorder.integrations.")]
    assert [r.getMessage() for r in records] == (["integration_capture_loss reason=retention_policy_failed"] if invalid else [])
    assert all(marker not in repr(r.__dict__) and r.exc_info is None and r.stack_info is None for r in records)
    if invalid:
        for artifact in members:
            assert (artifact["body_text"], artifact["stored_sha256"], artifact["stored_bytes"], artifact["policy_id"], artifact["omitted_reason"]) == (
                None, None, 0, "adapter_policy_error", "retention_policy_failed")
            assert artifact["observed_bytes"] > 0
    else:
        assert request_body(rows[0], artifacts) == {"model": "fixture-model", "messages": [{"safe": "first"}, {"safe": "second"}],
                                                  "tools": {"safe": "replacement catalog"}}
    with closing(sqlite3.connect(path)) as db:
        assert db.execute("SELECT message_index,message_path FROM request_members ORDER BY message_index").fetchall() == [(0, "/messages"), (1, "/messages")]
    assert rec.close(timeout=3).run_close_state == "clean" and rec.health().dropped_events == 0


@pytest.mark.parametrize("failure", ["exception", "callback-cancel", "none"])
def test_model_policy_failures_omit_all_bodies_once_without_native_retry(
        recording_factory, run_sdk_async, monkeypatch, caplog, failure):
    rec, path = recording_factory()
    marker = "model-fault-private"
    output = normal(tools=[function_call('{"text":"model-fault-private"}')], finish="tool_calls", content=marker)
    evidence, calls, requests = ModelEvidence(), [], []
    attempts, original_observe = [], rec.observe

    def observe(kind, **kwargs):
        attempts.append((kind, kwargs["facts"].get("retention")))
        return original_observe(kind, **kwargs)

    monkeypatch.setattr(rec, "observe", observe)

    def policy(kind, payload, context):
        calls.append(kind)
        if failure == "exception":
            raise ValueError(marker)
        if failure == "callback-cancel":
            raise asyncio.CancelledError(marker)
        return None

    async def run():
        async with native_client(lambda request: requests.append(request) or httpx2.Response(200, json=output)) as client:
            spy, seen = observed_method(client.chat.completions.create)
            result = await wrap_model(rec, spy, instance_id="model", retention_policy=policy)(
                model="fixture-model", messages=[{"role": "user", "content": marker}], tools=TOOLS, evidence=evidence)
            assert result is seen["results"][0] and len(seen["kwargs"]) == len(requests) == 1
            assert result.choices[0].message.content == marker
            assert [accepted(value).losses for value in [evidence.request, evidence.decisions[0].receipt, evidence.response]] == [("body_omitted",)] * 3

    with caplog.at_level(logging.WARNING, logger="mcp_flightrecorder.integrations"):
        run_sdk_async(run())
    rows, artifacts, _ = persisted(rec, path)
    assert [row["kind"] for row in rows] == calls == ["model.request", "model.tool_call", "model.response"]
    assert attempts == [(kind, {"policy_id": "adapter_policy_error", "omitted_reason": "retention_policy_failed"})
                        for kind in ("model.request", "model.tool_call", "model.response")]
    content = [*request_artifacts(rows[0], artifacts), artifacts[rows[1]["payload_ref"]], artifacts[rows[2]["payload_ref"]]]
    for artifact in content:
        assert (artifact["body_text"], artifact["stored_sha256"], artifact["stored_bytes"], artifact["policy_id"], artifact["omitted_reason"]) == (
            None, None, 0, "adapter_policy_error", "retention_policy_failed")
        assert artifact["observed_bytes"] > 0
    assert all(marker not in (artifact["body_text"] or "") for artifact in artifacts.values())
    assert evidence.losses == ("retention_policy_failed",)
    records = [r for r in caplog.records if r.name.startswith("mcp_flightrecorder.integrations.")]
    assert [r.getMessage() for r in records] == ["integration_capture_loss reason=retention_policy_failed"]
    assert all(marker not in repr(r.__dict__) and r.exc_info is None and r.stack_info is None for r in records)
    token = rows[1]["details"]["args_digest"]
    assert token["value"] and token["basis"] == "observed_original" and token.get("reason") is None
    assert all(row["details"]["complete"] for row in rows) and rows[-1]["state"] == "complete"
    closed, health = rec.close(timeout=3), rec.health()
    assert closed.run_close_state == "clean" and health.dropped_events == 0
    assert (health.ordinary.used_slots, health.completion.used_slots) == (0, 0)


@pytest.mark.parametrize("ordinary", ["null", "omission"])
def test_model_retained_null_and_explicit_omission_remain_distinct(recording_factory, run_sdk_async, ordinary):
    rec, path = recording_factory()
    output = normal(tools=[function_call()], finish="tool_calls")
    evidence, calls = ModelEvidence(), []

    def policy(kind, payload, context):
        calls.append(kind)
        if kind == "model.request" or ordinary == "omission":
            return {"policy_id": "explicit", "omitted_reason": "chosen-omission", "redacted": True}
        return {"policy_id": "explicit", "retained": None, "redacted": True}

    async def run():
        async with native_client(lambda request: httpx2.Response(200, json=output)) as client:
            spy, seen = observed_method(client.chat.completions.create)
            result = await wrap_model(rec, spy, instance_id="model", retention_policy=policy)(
                model="fixture-model", messages=[], tools=TOOLS, evidence=evidence)
            assert result is seen["results"][0] and len(seen["kwargs"]) == 1

    run_sdk_async(run())
    rows, artifacts, _ = persisted(rec, path)
    assert [row["kind"] for row in rows] == calls == ["model.request", "model.tool_call", "model.response"]
    assert all(a["body_text"] is None and a["omitted_reason"] == "chosen-omission" for a in request_artifacts(rows[0], artifacts))
    originals = [b'{"arguments_raw":"{\\"text\\":\\"selected\\"}"}',
                 json.dumps(output, ensure_ascii=False, separators=(",", ":")).encode()]
    for row, original in zip(rows[1:], originals, strict=True):
        retained_artifact(artifacts[row["payload_ref"]], original, b'null' if ordinary == "null" else None,
                          "explicit", redacted=True, omitted=None if ordinary == "null" else "chosen-omission")
    assert evidence.losses == () and rows[-1]["state"] == "complete"
    assert all(receipt.status == "accepted" for receipt in [evidence.request, evidence.decisions[0].receipt, evidence.response])


@pytest.mark.parametrize("invalid_request", [False, True], ids=["valid-mapping", "invalid-topology"])
def test_model_metadata_overrides_valid_or_invalid_request_retention(recording_factory, run_sdk_async, invalid_request):
    rec, path = recording_factory(capture="metadata")
    evidence, calls = ModelEvidence(), []
    output = normal(tools=[function_call()], finish="tool_calls")

    def policy(kind, payload, context):
        calls.append(kind)
        if kind == "model.request":
            if invalid_request:
                payload = None
            else:
                payload["messages"] = [{"safe": True}]
                payload["tools"] = []
        else:
            payload = None
        return {"policy_id": "metadata-policy", "retained": payload, "redacted": True, "truncated": True}

    async def run():
        async with native_client(lambda request: httpx2.Response(200, json=output)) as client:
            spy, seen = observed_method(client.chat.completions.create)
            result = await wrap_model(rec, spy, instance_id="model", retention_policy=policy)(
                model="fixture-model", messages=[{"role": "user", "content": "original"}], tools=TOOLS, evidence=evidence)
            assert result is seen["results"][0] and len(seen["kwargs"]) == 1

    run_sdk_async(run())
    rows, artifacts, _ = persisted(rec, path)
    assert [row["kind"] for row in rows] == calls == ["model.request", "model.tool_call", "model.response"]
    assert len(rows[0]["body"]["message_refs"]) == 1
    request_parts = request_artifacts(rows[0], artifacts)
    for artifact in request_parts + [artifacts[row["payload_ref"]] for row in rows[1:]]:
        failed = invalid_request and artifact in request_parts
        assert (artifact["body_text"], artifact["stored_sha256"], artifact["stored_bytes"], artifact["omitted_reason"]) == (None, None, 0, "capture_metadata")
        assert artifact["policy_id"] == ("adapter_policy_error" if failed else "metadata-policy")
        assert (artifact["redacted"], artifact["truncated"]) == ((0, 0) if failed else (1, 1))
        assert artifact["observed_bytes"] > 0
    assert evidence.losses == (("retention_policy_failed",) if invalid_request else ())
    assert all(row["details"]["complete"] for row in rows) and rows[-1]["state"] == "complete"
    assert rec.close(timeout=3).run_close_state == "clean" and rec.health().dropped_events == 0


@pytest.mark.parametrize("configuration", ["noncallable", "coroutine-function", "async-generator"])
def test_model_policy_configuration_rejects_async_before_native_work(recording_factory, run_sdk_async, configuration):
    rec, path = recording_factory()
    calls = []

    async def coroutine(*args):
        calls.append("coroutine executed")
        return {}

    async def generator(*args):
        calls.append("generator executed")
        yield {}

    configured = {"noncallable": object(), "coroutine-function": coroutine, "async-generator": generator}[configuration]

    async def run():
        async with native_client(lambda request: calls.append("native executed") or httpx2.Response(200, json=normal())) as client:
            with pytest.raises(TypeError, match="synchronous callable"):
                wrap_model(rec, client.chat.completions.create, instance_id="model", retention_policy=configured)

    run_sdk_async(run())
    assert calls == [] and persisted(rec, path)[0] == []


def test_model_overlapping_policies_keep_invocation_context_local(recording_factory, run_sdk_async):
    rec, path = recording_factory()
    calls, requests = [], []
    evidence = {name: ModelEvidence() for name in ("A", "B")}

    def policy(kind, payload, context):
        calls.append((kind, dict(context)))
        identity = context["instance_id"]
        context.clear()
        context["instance_id"] = "mutated"
        if kind == "model.request":
            payload["messages"] = [{"safe": identity}]
        else:
            payload = {"safe": identity}
        return {"policy_id": "overlap", "retained": payload, "redacted": True}

    async def run():
        entered = {name: asyncio.Event() for name in evidence}
        release = {name: asyncio.Event() for name in evidence}

        async def handler(request):
            payload = json.loads(request.content)
            label = payload["messages"][0]["content"]
            requests.append(label)
            entered[label].set()
            await release[label].wait()
            return httpx2.Response(200, json=normal(tools=[function_call(name="tool-" + label, reference="ref-" + label)],
                finish="tool_calls", choice=3 if label == "A" else 5))

        async with native_client(handler) as client:
            spy, seen = observed_method(client.chat.completions.create)
            wrappers = {name: wrap_model(rec, spy, instance_id=name, retention_policy=policy) for name in evidence}
            tasks, turns = {}, {}
            try:
                for name in ("A", "B"):
                    with rec.turn(name) as turn:
                        turns[name] = turn.receipt.turn_id
                        pending = wrappers[name](model="fixture-model", messages=[{"role": "user", "content": name}], evidence=evidence[name])
                    tasks[name] = asyncio.create_task(pending)
                    await asyncio.wait_for(entered[name].wait(), 2)
                assert not tasks["A"].done() and not tasks["B"].done()
                assert [(kind, context["instance_id"]) for kind, context in calls] == [("model.request", "A"), ("model.request", "B")]
                release["B"].set()
                result_b = await tasks["B"]
                assert not tasks["A"].done()
                release["A"].set()
                result_a = await tasks["A"]
                assert result_b is seen["results"][0] and result_a is seen["results"][1]
                assert requests == ["A", "B"] and len(seen["kwargs"]) == 2
            finally:
                for event in release.values():
                    event.set()
                for task in tasks.values():
                    if not task.done():
                        task.cancel("overlap fixture cleanup")
                await asyncio.gather(*tasks.values(), return_exceptions=True)
            return turns

    turns = run_sdk_async(run())
    rows, artifacts, _ = persisted(rec, path)
    base = lambda name: {"instance_id": name, "choice_index": None, "tool_index": None, "name": None, "provider_ref": None}
    assert calls == [("model.request", base("A")), ("model.request", base("B")),
        ("model.tool_call", {"instance_id": "B", "choice_index": 5, "tool_index": 0, "name": "tool-B", "provider_ref": "ref-B"}),
        ("model.response", base("B")),
        ("model.tool_call", {"instance_id": "A", "choice_index": 3, "tool_index": 0, "name": "tool-A", "provider_ref": "ref-A"}),
        ("model.response", base("A"))]
    assert len(rows) == 6
    for name, item in evidence.items():
        own = [row for row in rows if row["model_call_id"] == accepted(item.request).model_call_id]
        assert [row["kind"] for row in own] == ["model.request", "model.tool_call", "model.response"]
        assert all(row["turn_id"] == turns[name] and row["details"]["instance_id"] == name for row in own)
        assert own[1]["name"] == "tool-" + name and own[1]["details"]["provider_ref"] == "ref-" + name
        assert own[1]["body"] == own[2]["body"] == {"safe": name}
        assert request_body(own[0], artifacts)["messages"] == [{"safe": name}]
        assert item.losses == () and own[-1]["state"] == "complete"
    assert evidence["A"].request.model_call_id != evidence["B"].request.model_call_id


@pytest.mark.parametrize("exception_type", [SystemExit, KeyboardInterrupt], ids=["system-exit", "keyboard-interrupt"])
@pytest.mark.parametrize("boundary", ["request", "decision", "terminal", "stream-decision", "stream-terminal"])
def test_model_policy_process_control_propagates_at_its_boundary(
        recording_factory, run_sdk_async, exception_type, boundary):
    rec, path = recording_factory()
    interruption = exception_type("owned policy process control")
    evidence, calls, requests = ModelEvidence(), [], []
    target = "model.request" if boundary == "request" else "model.tool_call" if boundary.endswith("decision") else "model.response"

    def policy(kind, payload, context):
        calls.append(kind)
        if kind == target:
            raise interruption
        return {"policy_id": "process-control", "retained": payload}

    async def run():
        peer = CountedBytes([chunk([delta(tools=[{"index": 0, **function_call()}], finish="tool_calls")])]) if boundary.startswith("stream-") else None

        async def handler(request):
            requests.append(request)
            if peer is not None:
                return httpx2.Response(200, headers={"content-type": "text/event-stream"}, stream=peer)
            return httpx2.Response(200, json=normal(tools=[function_call()], finish="tool_calls"))

        async with native_client(handler) as client:
            decoded = decoded_chunks(client)
            spy, seen = observed_method(client.chat.completions.create)
            create = wrap_model(rec, spy, instance_id="model", retention_policy=policy)
            stream = None
            forwarded = []
            try:
                with pytest.raises(exception_type) as caught:
                    if peer is None:
                        await create(model="fixture-model", messages=[], evidence=evidence)
                    else:
                        stream = await create(model="fixture-model", messages=[], stream=True, evidence=evidence)
                        forwarded.append(await stream.__anext__())
                        if boundary == "stream-terminal":
                            await stream.__anext__()
                assert caught.value is interruption
                assert len(seen["kwargs"]) == len(requests) == (0 if boundary == "request" else 1)
                if boundary == "request":
                    assert seen["results"] == [] and evidence.request is None
                else:
                    accepted(evidence.request)
                    assert len(seen["results"]) == 1
                if peer is not None:
                    assert len(decoded) == 1
                    if boundary == "stream-decision":
                        assert forwarded == [] and peer.reads == 1 and peer.closes == 0
                        assert evidence.decisions == () and not stream._capture.call.finished
                    else:
                        assert forwarded == [decoded[0]] and forwarded[0] is decoded[0]
                        assert len(evidence.decisions) == 1 and stream._capture.call.finished
                        assert (stream._capture.chunks, stream._capture.tools, stream._capture.choices) == ([], {}, {})
                assert evidence.response is None and evidence.losses == ()
            finally:
                if stream is not None:
                    # The host owns native cleanup after process control; it does not
                    # resume the interrupted observer or claim a new retry guarantee.
                    await seen["results"][0].aclose()
            if peer is not None:
                assert peer.closes == 1
            assert not client.is_closed()

    run_sdk_async(run())
    rows, _, _ = persisted(rec, path)
    expected = [] if boundary == "request" else ["model.request"]
    if boundary in ("terminal", "stream-terminal"):
        expected.append("model.tool_call")
    assert [row["kind"] for row in rows] == expected
    assert calls == ["model.request"] + ([] if boundary == "request" else ["model.tool_call"]) + (
        ["model.response"] if boundary in ("terminal", "stream-terminal") else [])
    assert rec.close(timeout=3).run_close_state == ("clean" if boundary == "request" else "incomplete")
    health = rec.health()
    assert health.dropped_events == 0 and health.open_operations == 0
    assert (health.ordinary.used_slots, health.completion.used_slots) == (0, 0)


@pytest.mark.parametrize("early", [False, True], ids=["complete-consumption", "early-close"])
def test_stream_policy_retains_only_consumed_decisions_and_aggregate(recording_factory, run_sdk_async, early):
    rec, path = recording_factory()
    marker = "stream-policy-private"
    raw = '{"text":"stream-policy-private"}'
    values = [chunk([delta(index=2, tools=[{"index": 4, **function_call(raw)}], content=marker, finish="tool_calls")]),
              chunk(usage={"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3})]
    evidence, calls, requests = [ModelEvidence(), ModelEvidence()], [], []

    def policy(kind, payload, context):
        calls.append((kind, json.loads(json.dumps(payload)), dict(context)))
        if kind == "model.request":
            payload["messages"] = [{"safe": "request"}]
        elif kind == "model.tool_call":
            payload = {"safe": "decision"}
        else:
            payload = {"consumed": len(payload["chunks"])}
        return {"policy_id": "stream-policy", "retained": payload, "redacted": True}

    async def consume(configured, item):
        peer = CountedBytes(values)
        host = asyncio.current_task()

        async def handler(request):
            requests.append(json.loads(request.content))
            return httpx2.Response(200, headers={"content-type": "text/event-stream"}, stream=peer)

        async with native_client(handler) as client:
            decoded = decoded_chunks(client)
            spy, seen = observed_method(client.chat.completions.create)
            stream = await wrap_model(rec, spy, instance_id="stream-model", retention_policy=configured)(
                model="fixture-model", messages=[{"role": "user", "content": marker}], stream=True, evidence=item)
            assert peer.reads == 0 and peer.read_attempts == []
            if configured:
                assert [kind for kind, _, _ in calls] == ["model.request"]
            if early:
                received = [await stream.__anext__()]
                assert peer.reads == 1
            else:
                received = [value async for value in stream]
            assert len(received) == (1 if early else 2)
            assert all(value is original for value, original in zip(received, decoded, strict=True))
            assert len(item.decisions) == 1 and accepted(item.decisions[0].receipt)
            await stream.aclose()
            before = list(calls)
            await stream.aclose()
            assert calls == before and peer.closes == 1
            if early:
                assert peer.reads == 1, "Early cleanup cannot drain the unread native chunk."
            # At EOF the genuine SDK owns HTTP/1.1 draining. Compare its actual
            # read/close counts below against the same peer with default policy.
            assert foreign_stream_owners(peer, host) == ()
            assert (stream._capture.chunks, stream._capture.tools, stream._capture.choices) == ([], {}, {})
            assert len(seen["kwargs"]) == 1 and item.losses == ()
            accepted(item.request)
            accepted(item.response)
            return peer.reads, peer.closes

    async def run():
        native_policy_default = await consume(None, evidence[0])
        selected = await consume(policy, evidence[1])
        assert selected == native_policy_default and len(requests) == 2 and requests[0] == requests[1]

    run_sdk_async(run())
    rows, artifacts, _ = persisted(rec, path)
    assert [row["kind"] for row in rows] == ["model.request", "model.tool_call", "model.response"] * 2
    assert [kind for kind, _, _ in calls] == ["model.request", "model.tool_call", "model.response"]
    consumed = values[:1] if early else values
    assert calls[-1][1] == {"format": "openai_chat_stream/1", "chunks": consumed}
    assert calls[1][1] == {"arguments_raw": raw}
    assert calls[1][2] == {"instance_id": "stream-model", "choice_index": 2, "tool_index": 4,
                           "name": "echo", "provider_ref": "same-provider-id"}
    context = {"instance_id": "stream-model", "choice_index": None, "tool_index": None, "name": None, "provider_ref": None}
    assert calls[0][2] == calls[-1][2] == context
    assert rows[2]["body"] == {"format": "openai_chat_stream/1", "chunks": consumed}
    assert request_body(rows[3], artifacts) == {"model": "fixture-model", "messages": [{"safe": "request"}], "stream": True}
    assert rows[2]["state"] == rows[5]["state"] == ("partial" if early else "complete")
    assert rows[4]["details"]["complete"] is True and rows[5]["details"]["complete"] is (not early)
    before, after = rows[1]["details"]["args_digest"], rows[4]["details"]["args_digest"]
    assert before == after and before["value"] and before["basis"] == "observed_original"
    assert rows[1]["decision_id"] != rows[4]["decision_id"] and rows[0]["model_call_id"] != rows[3]["model_call_id"]
    original_decision = b'{"arguments_raw":"{\\"text\\":\\"stream-policy-private\\"}"}'
    retained_artifact(artifacts[rows[4]["payload_ref"]], original_decision,
                      b'{"safe":"decision"}', "stream-policy", redacted=True)
    original_aggregate = json.dumps({"format": "openai_chat_stream/1", "chunks": consumed}, ensure_ascii=False, separators=(",", ":")).encode()
    retained_artifact(artifacts[rows[5]["payload_ref"]], original_aggregate,
                      b'{"consumed":1}' if early else b'{"consumed":2}', "stream-policy", redacted=True)
    assert marker not in artifacts[rows[4]["payload_ref"]]["body_text"] and marker not in artifacts[rows[5]["payload_ref"]]["body_text"]
    health = rec.health()
    assert health.dropped_events == 0 and (health.ordinary.used_slots, health.completion.used_slots) == (0, 0)
