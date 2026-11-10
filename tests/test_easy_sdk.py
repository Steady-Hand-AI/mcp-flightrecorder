"""Facade contracts through genuine SDKs and deterministic in-process peers."""
import asyncio
from contextlib import closing
from functools import wraps
import json
import sqlite3

import httpx2
from mcp import Client
from mcp.server import Server
import mcp_types
from openai import AsyncOpenAI
import pytest

from mcp_flightrecorder import Recorder
from mcp_flightrecorder._types import CaptureResult
from mcp_flightrecorder.easy import AgentRecorder


def tool(label):
    return {"id": "call_" + label, "type": "function", "function": {
        "name": "echo", "arguments": json.dumps({"text": label})}}


ASSISTANT = {"role": "assistant", "content": None, "tool_calls": [tool("alpha"), tool("beta")]}
COMPLETION = {"id": "completion", "object": "chat.completion", "created": 0,
              "model": "fixture-model", "choices": [
                  {"index": 0, "message": ASSISTANT, "finish_reason": "tool_calls"},
                  {"index": 1, "message": {"role": "assistant", "content": None,
                   "tool_calls": [tool("unselected")]}, "finish_reason": "tool_calls"}]}


def persisted(path):
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        db.row_factory = sqlite3.Row
        assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert db.execute("PRAGMA foreign_key_check").fetchall() == []
        events = [dict(row) for row in db.execute("SELECT * FROM events ORDER BY sequence")]
        links = [dict(row) for row in db.execute("SELECT * FROM links")]
    for event in events:
        event["details"] = json.loads(event["details_json"])
    return events, links


async def parallel_host(path):
    """The host chooses native completion positions and passes explicit attempts."""
    observed = {"requests": [], "tools": [], "returns": []}
    entered = {label: asyncio.Event() for label in ("alpha", "beta")}
    release = {label: asyncio.Event() for label in ("alpha", "beta")}
    alpha_count = 0

    async def provider(request):
        observed["requests"].append(request.content)
        return httpx2.Response(200, json=COMPLETION)

    async def listing(context, params):
        return mcp_types.ListToolsResult(tools=[mcp_types.Tool(
            name="echo", description="Return the same value for distinct selected inputs.",
            input_schema={"type": "object", "properties": {"text": {"type": "string"}},
                          "required": ["text"]})])

    async def calling(context, params):
        nonlocal alpha_count
        label = params.arguments["text"]
        observed["tools"].append({"name": params.name, "arguments": dict(params.arguments)})
        if label == "alpha":
            alpha_count += 1
        if label == "beta" or alpha_count == 2:
            entered[label].set()
            await release[label].wait()
        observed["returns"].append(label)
        # Equal result bytes are deliberate: they must never select an invocation.
        return mcp_types.CallToolResult(content=[mcp_types.TextContent(type="text", text="same")], is_error=False)

    server = Server("facade-test", on_list_tools=listing, on_call_tool=calling)
    recorder = AgentRecorder(path, capture="content", all_model_calls_recorded=True).start()
    conversation = recorder.conversation("parallel")
    tasks = []
    try:
        async with AsyncOpenAI(api_key="fixture-only", base_url="https://fixture.invalid/v1", max_retries=0,
                http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(provider), trust_env=False)) as model:
            async with Client(server) as native:
                tools = recorder.mcp(native)
                with conversation.turn("Choose two tools from one alternative."):
                    await tools.list_tools()
                    messages = [{"role": "user", "content": "Choose two tools from one alternative."}]
                    payload = {"model": "fixture-model", "messages": messages, "n": 2}
                    with conversation.model_call(payload, create=model.chat.completions.create) as call:
                        response = await call.create()
                    assert len(response.choices) == 2
                    selected = response.choices[0]
                    choices = [call.choice(item.function.name, json.loads(item.function.arguments),
                                           choice_index=selected.index, tool_index=index)
                               for index, item in enumerate(selected.message.tool_calls)]
                    with choices[0].run():
                        earlier = await tools.call_tool("echo", {"text": "alpha"})
                    assert earlier.content[0].text == "same"

                    async def execute(index, label):
                        with choices[index].run() as attempt:
                            result = await tools.call_tool("echo", {"text": label})
                        message = {"role": "tool", "tool_call_id": "call_" + label,
                                   "content": result.content[0].text}
                        assert attempt.answer(message) is message
                        return message

                    tasks = [asyncio.create_task(execute(0, "alpha")), asyncio.create_task(execute(1, "beta"))]
                    await asyncio.gather(*(event.wait() for event in entered.values()))
                    release["beta"].set()
                    beta = await tasks[1]
                    release["alpha"].set()
                    alpha = await tasks[0]
                    messages += [selected.message.to_dict(mode="json", use_api_names=True, exclude_unset=True), alpha, beta]
                    with conversation.model_call({"model": "fixture-model", "messages": messages},
                                                 create=model.chat.completions.create) as followup:
                        await followup.create()
    finally:
        for event in release.values():
            event.set()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        conversation.close()
        closed = await recorder.aclose()
        if path is not None:
            assert closed.writer_stopped and closed.portable
    return observed


@pytest.mark.parametrize("reject_selected", [False, True])
def test_native_alternatives_parallel_and_selected_later_attempt(tmp_path, run_sdk_async, monkeypatch, reject_selected):
    baseline = run_sdk_async(parallel_host(None))
    original = Recorder.finish_operation
    terminals = []

    def finish(recorder, ticket, **kwargs):
        operation = json.loads(ticket.encoded)
        if operation["method"] == "tools/call":
            # Completion order: earlier alpha, beta, selected alpha. Reject only the last.
            receipt = (CaptureResult("rejected", "queue_full") if reject_selected and len(terminals) == 2
                       else original(recorder, ticket, **kwargs))
            terminals.append(receipt)
            return receipt
        return original(recorder, ticket, **kwargs)

    monkeypatch.setattr(Recorder, "finish_operation", finish)
    path = tmp_path / "parallel.sqlite"
    captured = run_sdk_async(parallel_host(path))
    assert captured == baseline
    assert captured["tools"] == [{"name": "echo", "arguments": {"text": label}}
                                 for label in ("alpha", "alpha", "beta")]
    assert captured["returns"] == ["alpha", "beta", "alpha"]
    assert [row.status for row in terminals] == ["accepted", "accepted", "rejected" if reject_selected else "accepted"]
    events, links = persisted(path)
    # Both completions remain recorded, even though the host executes only choice zero.
    decisions = [row for row in events if row["kind"] == "model.tool_call"]
    assert len(decisions) == 6  # Three native decisions per each of two actual responses.
    requests = [row for row in events if row["kind"] == "model.request"]
    answer_links = [json.loads(row["details_json"]) for row in links if row["relation"] == "answers"]
    bound_results = {row["result_event_id"] for row in answer_links}
    assert terminals[0].event_id not in bound_results, "Equal earlier result bytes cannot replace the selected invocation."
    assert terminals[1].event_id in bound_results
    if reject_selected:
        assert bound_results == {terminals[1].event_id}
        assert requests[-1]["details"]["unresolved_answers"] == [
            {"pointer": "/messages/2/content", "reason": "capture_unavailable"}]
    else:
        assert bound_results == {terminals[1].event_id, terminals[2].event_id}
        assert "unresolved_answers" not in requests[-1]["details"]
    assert [row["details"]["provider_ref"] for row in decisions] == [
        "call_alpha", "call_beta", "call_unselected"] * 2
    selected_decisions = {row["decision_id"] for row in decisions[:2]}
    operation_decisions = {row["decision_id"] for row in events if row["kind"] == "mcp.request" and row["method"] == "tools/call"}
    assert operation_decisions == selected_decisions


class Frames(httpx2.AsyncByteStream):
    def __init__(self):
        self.reads = self.closes = 0
        self.frames = []
        for text, finish in (("one", None), ("two", "stop")):
            chunk = {"id": "stream", "object": "chat.completion.chunk", "created": 0,
                     "model": "fixture-model", "choices": [{"index": 0, "delta": {"content": text},
                                                              "finish_reason": finish}]}
            self.frames.append(("data: " + json.dumps(chunk) + "\n\n").encode())
        self.frames.append(b"data: [DONE]\n\n")

    async def __aiter__(self):
        while self.frames:
            self.reads += 1
            yield self.frames.pop(0)

    async def aclose(self):
        self.closes += 1


async def stream_host(path, early):
    frames, requests = Frames(), []

    async def provider(request):
        requests.append(request.content)
        return httpx2.Response(200, headers={"content-type": "text/event-stream"}, stream=frames)

    recorder = AgentRecorder(path, capture="content").start()
    conversation = recorder.conversation("stream")
    chunks = []
    try:
        async with AsyncOpenAI(api_key="fixture-only", base_url="https://fixture.invalid/v1", max_retries=0,
                http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(provider), trust_env=False)) as model:
            with conversation.turn("Stream text."):
                payload = {"model": "fixture-model", "messages": [{"role": "user", "content": "Stream text."}], "stream": True}
                with conversation.model_call(payload, create=model.chat.completions.create) as call:
                    stream = await call.create()
                    assert frames.reads == 0, "Recording must not pre-consume the host's stream."
                    try:
                        async for chunk in stream:
                            chunks.append(chunk.to_dict(mode="json", use_api_names=True, exclude_unset=True))
                            if early:
                                break
                    finally:
                        await stream.close()
    finally:
        conversation.close()
        closed = await recorder.aclose()
        if path is not None:
            assert closed.writer_stopped and closed.portable
    return requests, chunks, frames.reads, frames.closes


@pytest.mark.parametrize("early", [False, True])
def test_sdk_stream_consumption_and_host_close_are_unchanged(tmp_path, run_sdk_async, early):
    baseline = run_sdk_async(stream_host(None, early))
    path = tmp_path / "stream.sqlite"
    assert run_sdk_async(stream_host(path, early)) == baseline
    assert len(baseline[1]) == (1 if early else 2)
    assert baseline[2:] == (1 if early else 3, 1)
    events, _ = persisted(path)
    terminal = [row for row in events if row["kind"] == "model.response"]
    assert len(terminal) == 1
    assert terminal[0]["state"] == ("partial" if early else "complete")


@pytest.mark.parametrize("failure", ["http_error", "cancel"])
def test_native_model_failure_identity_and_cleanup_are_preserved(tmp_path, run_sdk_async, failure):
    async def run(path):
        entered = asyncio.Event()
        observed = []

        async def provider(request):
            entered.set()
            if failure == "cancel":
                await asyncio.Event().wait()
            return httpx2.Response(503, json={"error": {"message": "fixture outage"}})

        recorder = AgentRecorder(path, capture="content").start()
        conversation = recorder.conversation("failure")
        try:
            async with AsyncOpenAI(api_key="fixture-only", base_url="https://fixture.invalid/v1", max_retries=0,
                    http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(provider), trust_env=False)) as model:
                native = model.chat.completions.create

                @wraps(native)
                async def observe_native(**kwargs):
                    try:
                        return await native(**kwargs)
                    except BaseException as error:
                        observed.append(error)
                        raise

                async def submit():
                    with conversation.turn("Preserve native failure."):
                        with conversation.model_call({"model": "fixture-model", "messages": []},
                                                     create=observe_native) as call:
                            await call.create()

                task = asyncio.create_task(submit())
                try:
                    await entered.wait()
                    if failure == "cancel":
                        task.cancel()
                    with pytest.raises(BaseException) as caught:
                        await task
                    assert len(observed) == 1
                    assert caught.value is observed[0]
                    if failure == "cancel":
                        assert isinstance(caught.value, asyncio.CancelledError)
                    else:
                        assert caught.value.status_code == 503
                finally:
                    if not task.done():
                        task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
        finally:
            conversation.close()
            closed = await recorder.aclose()
            if path is not None:
                assert closed.writer_stopped and closed.portable

    run_sdk_async(run(None))
    path = tmp_path / "failure.sqlite"
    run_sdk_async(run(path))
    events, _ = persisted(path)
    terminal = [row for row in events if row["kind"] == "model.response"]
    assert len(terminal) == 1
    assert terminal[0]["state"] == ("cancelled" if failure == "cancel" else "error")


def echo_server(observed):
    async def listing(context, params):
        return mcp_types.ListToolsResult(tools=[mcp_types.Tool(
            name="echo", input_schema={"type": "object", "properties": {"text": {"type": "string"}},
                                       "required": ["text"]})])

    async def calling(context, params):
        observed.append({"name": params.name, "arguments": dict(params.arguments)})
        return mcp_types.CallToolResult(content=[mcp_types.TextContent(type="text", text=params.arguments["text"])],
                                       is_error=False)

    return Server("facade-ownership", on_list_tools=listing, on_call_tool=calling)


@pytest.mark.parametrize("mutation", ["replace", "shift", "replace_field"])
def test_deferred_native_request_resolves_messages_at_actual_await(tmp_path, run_sdk_async, mutation):
    async def run(path):
        observed, requests = [], []

        async def provider(request):
            requests.append(json.loads(request.content))
            return httpx2.Response(200, json=COMPLETION)

        recorder = AgentRecorder(path, capture="content").start()
        conversation = recorder.conversation("deferred")
        try:
            async with AsyncOpenAI(api_key="fixture-only", base_url="https://fixture.invalid/v1", max_retries=0,
                    http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(provider), trust_env=False)) as model:
                async with Client(echo_server(observed)) as native:
                    tools = recorder.mcp(native)
                    with conversation.turn("Prepare one answer."):
                        await tools.list_tools()
                        with conversation.model_call({"messages": []}) as selection:
                            selection.done({"tool": "echo", "arguments": {"text": "original"}})
                        choice = selection.choice("echo", {"text": "original"})
                        with choice.run() as attempt:
                            result = await tools.call_tool("echo", {"text": "original"})
                        bound = {"role": "tool", "tool_call_id": "call_original", "content": result.content[0].text}
                        assert attempt.answer(bound) is bound
                        messages = [bound]
                        payload = {"model": "fixture-model", "messages": messages}
                        with conversation.model_call(payload, create=model.chat.completions.create) as call:
                            pending = call.create()
                            assert requests == [], "Creating a native coroutine must remain lazy."
                            if mutation == "replace":
                                messages[0] = dict(bound)  # Same bytes/position, deliberately no binding.
                            elif mutation == "shift":
                                messages.insert(0, {"role": "user", "content": "An intervening message."})
                            else:
                                payload["messages"] = [{"role": "user", "content": "Replacement field."}]
                            response = await pending
                            assert response.id == "completion"
        finally:
            conversation.close()
            closed = await recorder.aclose()
            if path is not None:
                assert closed.writer_stopped and closed.portable
        return observed, requests

    baseline = run_sdk_async(run(None))
    path = tmp_path / "deferred.sqlite"
    assert run_sdk_async(run(path)) == baseline
    assert baseline[0] == [{"name": "echo", "arguments": {"text": "original"}}]
    assert len(baseline[1]) == 1
    events, links = persisted(path)
    submitted = [row for row in events if row["kind"] == "model.request"][-1]
    answer_links = [row for row in links if row["relation"] == "answers"]
    if mutation == "replace":
        assert answer_links == [], "A new equal object cannot inherit the former occupant's binding."
        assert submitted["details"]["unresolved_answers"] == [
            {"pointer": "/messages/0/content", "reason": "binding_key_missing"}]
    else:
        assert "unresolved_answers" not in submitted["details"]
        assert len(answer_links) == 1
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
            pointer = "/messages/1/content" if mutation == "shift" else "/messages/0/content"
            assert db.execute("SELECT target_pointer FROM answer_occurrences").fetchall() == [(pointer,)]


@pytest.mark.parametrize("boundary", ["closed_attempt", "old_choice", "other_live_turn"])
def test_mcp_attempt_scope_never_borrows_another_turn(tmp_path, run_sdk_async, boundary):
    async def run(path):
        observed = []
        recorder = AgentRecorder(path, capture="content").start()
        origin, other = recorder.conversation("origin"), recorder.conversation("other")
        try:
            async with Client(echo_server(observed)) as native:
                tools = recorder.mcp(native)
                with origin.turn("Originating turn."):
                    await tools.list_tools()
                    with origin.model_call({"messages": []}) as call:
                        call.done({"tool": "echo", "arguments": {"text": "selected"}})
                    choice = call.choice("echo", {"text": "selected"})
                    if boundary == "closed_attempt":
                        with choice.run() as attempt:
                            pending = tools.call_tool("echo", {"text": "selected"})
                    elif boundary == "other_live_turn":
                        with choice.run() as attempt:
                            with other.turn("Different live conversation."):
                                result = await tools.call_tool("echo", {"text": "selected"})
                if boundary != "other_live_turn":
                    with origin.turn("Later turn."):
                        if boundary == "closed_attempt":
                            result = await pending
                        else:
                            with choice.run() as attempt:
                                result = await tools.call_tool("echo", {"text": "selected"})
                        message = {"role": "tool", "tool_call_id": "call_selected", "content": result.content[0].text}
                        assert attempt.answer(message) is message
                        with origin.model_call({"messages": [message]}) as submitted:
                            submitted.done({"answer": "selected"})
                assert result.content[0].text == "selected"
        finally:
            origin.close()
            other.close()
            closed = await recorder.aclose()
            if path is not None:
                assert closed.writer_stopped and closed.portable
        return observed

    baseline = run_sdk_async(run(None))
    path = tmp_path / "scope.sqlite"
    assert run_sdk_async(run(path)) == baseline == [{"name": "echo", "arguments": {"text": "selected"}}]
    events, links = persisted(path)
    decision = next(row for row in events if row["kind"] == "model.tool_call")
    tool_requests = [row for row in events if row["kind"] == "mcp.request" and row["method"] == "tools/call"]
    if boundary == "other_live_turn":
        assert len(tool_requests) == 1
        assert tool_requests[0]["turn_id"] == decision["turn_id"]
        assert tool_requests[0]["decision_id"] == decision["decision_id"]
    else:
        assert tool_requests == [], "Late native work must not acquire the ambient later turn."
        assert not [row for row in links if row["relation"] == "answers"]
        submitted = [row for row in events if row["kind"] == "model.request"][-1]
        assert submitted["turn_id"] != decision["turn_id"]
        assert submitted["details"]["unresolved_answers"] == [
            {"pointer": "/messages/0/content", "reason": "capture_unavailable"}]


def test_host_initiated_mcp_records_actual_work_without_a_model_choice(tmp_path, run_sdk_async):
    async def run(path):
        observed = []
        recorder = AgentRecorder(path, capture="content").start()
        conversation = recorder.conversation("host")
        try:
            async with Client(echo_server(observed)) as native:
                tools = recorder.mcp(native)
                with conversation.turn("Refresh local context."):
                    await tools.list_tools()
                    operation = conversation.host_call("echo", {"text": "host selected"})
                    with operation.run():
                        result = await tools.call_tool("echo", {"text": "host selected"})
                    assert result.content[0].text == "host selected"
                    assert result.is_error is False
        finally:
            conversation.close()
            closed = await recorder.aclose()
            if path is not None:
                assert closed.writer_stopped and closed.portable
        return observed

    baseline = run_sdk_async(run(None))
    path = tmp_path / "host.sqlite"
    assert run_sdk_async(run(path)) == baseline == [{"name": "echo", "arguments": {"text": "host selected"}}]
    events, links = persisted(path)
    assert not [row for row in events if row["kind"].startswith("model.")]
    dispatches = [row for row in events if row["kind"] == "host.dispatch"]
    assert len(dispatches) == 1
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        operations = db.execute("SELECT decision_id,dispatch_event_id,link_basis,status FROM operations WHERE method='tools/call'").fetchall()
    assert operations == [(None, dispatches[0]["event_id"], "host_hook", "ok")]
    assert not [row for row in links if row["relation"] in ("answers", "dispatch_source")]


def test_deferred_model_from_closed_turn_does_not_borrow_new_turn(tmp_path, run_sdk_async):
    async def run(path):
        requests = []

        async def provider(request):
            requests.append(request.content)
            return httpx2.Response(200, json=COMPLETION)

        recorder = AgentRecorder(path, capture="content").start()
        conversation = recorder.conversation("late-model")
        try:
            async with AsyncOpenAI(api_key="fixture-only", base_url="https://fixture.invalid/v1", max_retries=0,
                    http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(provider), trust_env=False)) as model:
                with conversation.turn("Original model turn."):
                    with conversation.model_call({"model": "fixture-model", "messages": []},
                                                 create=model.chat.completions.create) as call:
                        pending = call.create()
                assert requests == []
                with conversation.turn("A different later turn."):
                    response = await pending
                    assert response.id == "completion"
        finally:
            conversation.close()
            closed = await recorder.aclose()
            if path is not None:
                assert closed.writer_stopped and closed.portable
        return requests

    baseline = run_sdk_async(run(None))
    path = tmp_path / "late-model.sqlite"
    assert run_sdk_async(run(path)) == baseline
    assert len(baseline) == 1
    events, _ = persisted(path)
    assert not [row for row in events if row["kind"].startswith("model.")]
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        assert db.execute("SELECT count(*) FROM turns").fetchone()[0] == 2
