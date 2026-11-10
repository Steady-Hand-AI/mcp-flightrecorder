"""The source-shipped roundtrip against the installed package and real SDKs."""
import asyncio
from collections import Counter
from contextlib import closing
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import site
import sqlite3
import subprocess
import sys

import pytest

import mcp_flightrecorder
from mcp_flightrecorder import Recorder, inspect

from .fixture_checks import body, reconstruct


ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "examples/tool_roundtrip.py"
CLI = str(Path(sys.executable).parent / "fr")
QUESTION = "Use the answer tool, then report its result."
TOOLS = [{"type": "function", "function": {
    "name": "answer", "description": "Return the example's answer.",
    "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
}}]
ASSISTANT = {"role": "assistant", "content": None, "tool_calls": [{
    "id": "call_answer", "type": "function", "function": {"name": "answer", "arguments": "{}"},
}]}
MESSAGES = [
    [{"role": "user", "content": QUESTION}],
    [{"role": "user", "content": QUESTION}, ASSISTANT,
     {"role": "tool", "tool_call_id": "call_answer", "content": "Tool result: 42"}],
]
REQUESTS = [{"model": "local-example-model", "messages": messages, "tools": TOOLS,
             "temperature": 0} for messages in MESSAGES]
RESPONSES = [
    {"id": "completion_1", "object": "chat.completion", "created": 0,
     "model": "local-example-model", "choices": [{"index": 0, "message": ASSISTANT,
                                                  "finish_reason": "tool_calls"}]},
    {"id": "completion_2", "object": "chat.completion", "created": 0,
     "model": "local-example-model", "choices": [{"index": 0,
         "message": {"role": "assistant", "content": "The answer is 42."}, "finish_reason": "stop"}]},
]
TOOL_RESULT = {"content": [{"type": "text", "text": "42"}], "isError": False}
HOST = {"selected_tool": {"name": "answer", "arguments": {}}, "raw_tool_text": "42",
        "prepared_text": "Tool result: 42", "answer": "The answer is 42."}


def command(argv, cwd):
    return subprocess.run(argv, cwd=cwd, capture_output=True, text=True, timeout=60)


def successful(result):
    assert result["ok"] is True, result
    assert result["error"] is None, result
    return result["data"]


def cli(path, *arguments):
    result = command([CLI, *arguments[:1], str(path), *arguments[1:]], path.parent)
    assert result.returncode == 0, result.stderr or result.stdout
    envelope = json.loads(result.stdout)
    successful(envelope)
    return envelope


def load_example():
    spec = importlib.util.spec_from_file_location("tool_roundtrip_example", EXAMPLE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def assert_work(summary):
    # These expectations are literals, independent of capture IDs and stored rows.
    assert {key: summary["host"][key] for key in HOST} == HOST
    assert summary["host"]["discovery"]["tools"] == [{
        "name": "answer", "description": "Return the example's answer.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
    }]
    backend = summary["backend"]
    assert backend["provider_requests"] == REQUESTS
    assert backend["provider_responses"] == RESPONSES
    assert backend["tool_calls"] == [{"name": "answer", "arguments": {}}]
    assert backend["tool_results"] == [TOOL_RESULT]
    assert backend["list_calls"] == 1


@pytest.fixture(scope="module")
def recording(tmp_path_factory):
    work = tmp_path_factory.mktemp("roundtrip")
    script = work / "tool_roundtrip.py"
    shutil.copyfile(EXAMPLE, script)
    path = work / "roundtrip.sqlite"
    result = command([sys.executable, "-I", "-B", str(script), "--output", str(path)], work)
    assert result.returncode == 0, result.stderr
    return path, json.loads(result.stdout), script


def test_installed_example_preserves_native_work_with_capture_disabled(recording):
    path, captured, script = recording
    installed = Path(mcp_flightrecorder.__file__).resolve()
    assert sys.prefix != sys.base_prefix
    assert installed.is_relative_to(Path(sys.prefix).resolve())
    assert any(installed.is_relative_to(Path(p).resolve()) for p in site.getsitepackages())
    assert not installed.is_relative_to(ROOT)
    assert not path.parent.is_relative_to(ROOT)
    native = command([sys.executable, "-I", "-B", str(script), "--no-capture"], path.parent)
    assert native.returncode == 0, native.stderr
    uncaptured = json.loads(native.stdout)
    assert_work(captured)
    assert_work(uncaptured)
    assert captured["host"] == uncaptured["host"]
    assert captured["backend"] == uncaptured["backend"]
    assert uncaptured["capture"]["status"] == "disabled"
    assert captured["capture"]["status"] == "accepted"
    assert captured["close"] == {"writer_stopped": True, "portable": True,
                                  "run_close_state": "clean", "reason": None}
    assert all(not Path(str(path) + suffix).exists() for suffix in ("-wal", "-shm", "-journal"))


@pytest.mark.parametrize("suffix", ["", "-wal"])
def test_existing_destination_is_refused_before_host_work(tmp_path, suffix):
    path = tmp_path / "existing.sqlite"
    existing = Path(str(path) + suffix)
    existing.write_bytes(b"keep this existing evidence")
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    result = command([sys.executable, "-I", "-B", str(EXAMPLE), "--output", str(path)], tmp_path)
    assert result.returncode != 0
    assert "FileExistsError" in result.stderr
    assert result.stdout == ""
    assert {p.name: p.read_bytes() for p in tmp_path.iterdir()} == before


def test_observed_work_and_prepared_value_have_exact_evidence(recording):
    path, summary, _ = recording
    capture = summary["capture"]
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as connection:
        connection.row_factory = sqlite3.Row
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        events = list(connection.execute("SELECT * FROM events ORDER BY sequence"))
        assert Counter(row["kind"] for row in events) == {
            "model.request": 2, "model.response": 2, "model.tool_call": 1,
            "mcp.request": 2, "mcp.response": 2, "host.tool_result": 1,
        }
        requests = [row for row in events if row["kind"] == "model.request"]
        assert [row["model_call_id"] for row in requests] == capture["model_call_ids"]
        assert len(set(capture["model_call_ids"])) == 2
        assert [reconstruct(connection, body(connection, row["payload_ref"]))
                for row in requests] == REQUESTS
        responses = [row for row in events if row["kind"] == "model.response"]
        assert [body(connection, row["payload_ref"]) for row in responses] == RESPONSES
        prepared = next(row for row in events if row["kind"] == "host.tool_result")
        assert prepared["event_id"] == capture["preparation"]["event_id"]
        assert body(connection, prepared["payload_ref"]) == "Tool result: 42"
        operations = list(connection.execute("SELECT * FROM operations ORDER BY start_event_id"))
        assert sorted(row["method"] for row in operations) == ["tools/call", "tools/list"]
        tool = next(row for row in operations if row["method"] == "tools/call")
        assert tool["decision_id"] == capture["decision"]["decision_id"]
        assert tool["model_call_id"] == capture["model_call_ids"][0]
        assert (tool["link_basis"], tool["status"], tool["resolution"], tool["input_rounds"]) == (
            "propagated", "ok", "unknown", None)
        assert tool["end_event_id"] == capture["operation"]["terminal"]["event_id"]
        assert Counter(row[0] for row in connection.execute("SELECT relation FROM links")) == {
            "answers": 1, "transformed_from": 1, "inserted_into": 1,
        }
    checked = successful(inspect.check(path, capture["run_id"]))
    assert checked["status"] == "pass"
    assert checked["required_failures"] == checked["required_unknowns"] == []
    expected = {"turns": 1, "events": 10, "observed_events": 10, "model_calls": 2,
                "model_requests": 2, "model_responses": 2, "decisions": 1, "operations": 2,
                "links": 3, "findings": 0, "request_members": 4, "answer_occurrences": 1,
                "linked_answers": 1, "unresolved_answers": 0, "text_equal": 0, "text_mismatch": 1}
    assert {key: checked["stats"][key] for key in expected} == expected


def test_documented_cli_finds_difference_and_both_sides_of_preparation(recording):
    path, summary, _ = recording
    capture = summary["capture"]
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    turns = cli(path, "turns", "--text-mismatch", "true")
    assert turns["cutoffs"] == []
    assert turns["data"]["next_cursor"] is None
    assert [row["turn"]["turn_id"] for row in turns["data"]["items"]] == [capture["turn_id"]]
    assert turns["data"]["items"][0]["flags"]["text_mismatch"] is True
    assert cli(path, "turns", "--text-mismatch", "true", "--tool-error", "true")["data"]["items"] == []
    overview = cli(path, "explain", "--turn", capture["turn_id"])
    assert overview["data"]["detail"] is None
    assert [row["model_call_id"] for row in overview["data"]["calls"]] == capture["model_call_ids"]
    first, second = [cli(path, "explain", "--turn", capture["turn_id"], "--model-call", call)
                     for call in capture["model_call_ids"]]
    assert first["cutoffs"] == second["cutoffs"] == []
    answers = first["data"]["detail"]["produced_answers"]
    assert len(answers) == 1
    answer = answers[0]
    assert answer["source_event_id"] == capture["decision"]["event_id"]
    assert answer["result_event_id"] == capture["operation"]["terminal"]["event_id"]
    assert answer["request_event_id"] == capture["models"][1]["request"]["event_id"]
    assert (answer["association_state"], answer["target_pointer"], answer["text_equal"],
            answer["comparison_reason"]) == ("linked", "/messages/2/content", False, None)
    assert second["data"]["detail"]["input_answers"] == answers
    sources = [row for row in first["data"]["detail"]["links"] if row["relation"] == "transformed_from"]
    assert [(row["source_event_id"], row["target_event_id"], row["source_pointer"], row["witness_event_id"])
            for row in sources] == [(capture["operation"]["terminal"]["event_id"],
                capture["preparation"]["event_id"], "/content/0/text", capture["preparation"]["event_id"])]
    placements = [row for row in second["data"]["detail"]["links"] if row["relation"] == "inserted_into"]
    assert [(row["source_event_id"], row["target_event_id"], row["target_pointer"], row["witness_event_id"])
            for row in placements] == [(capture["preparation"]["event_id"], answer["request_event_id"],
                                       "/messages/2/content", answer["request_event_id"])]
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before


def test_rejected_selected_result_keeps_host_work_and_never_borrows_prior_result(tmp_path, monkeypatch, run_sdk_async):
    example = load_example()
    path = tmp_path / "rejected.sqlite"

    async def scenario():
        native = await example.run_host()
        recorder = Recorder(path, capture="content", coverage=example.COVERAGE)
        try:
            # A real earlier result with the same tool/value must not become the selected result.
            with recorder.turn("Earlier independent operation"):
                prior = recorder.start_operation(scope=recorder.capture_scope(), client_id="earlier",
                    method="tools/call", name="answer", arguments={})
                earlier = recorder.finish_operation(prior.ticket, status="ok", result=TOOL_RESULT,
                                                     text_paths=["/content/0/text"])
                assert earlier.status == "accepted"
            finish = recorder.finish_operation
            rejected = []

            def reject_selected(ticket, **kwargs):
                if "content" in kwargs.get("result", {}):
                    kwargs["status"] = "invalid-status-for-control"
                    receipt = finish(ticket, **kwargs)
                    rejected.append(receipt)
                    return receipt
                return finish(ticket, **kwargs)

            monkeypatch.setattr(recorder, "finish_operation", reject_selected)
            captured = await example.run_host(recorder)
        finally:
            closed = await recorder.aclose(timeout=5)
        assert len(rejected) == 1
        assert (rejected[0].status, rejected[0].reason) == ("rejected", "invalid_input")
        assert closed.writer_stopped and closed.portable
        assert closed.run_close_state == "incomplete"
        return native, captured, earlier.event_id

    native, captured, earlier_id = run_sdk_async(scenario(), timeout=30)
    assert_work(captured)
    assert captured["host"] == native["host"]
    assert captured["backend"] == native["backend"]
    capture = captured["capture"]
    assert capture["status"] == "unavailable"
    assert capture["operation"]["terminal"]["status"] == "rejected"
    assert capture["operation"]["losses"] == ["capture_rejected"]
    assert capture["preparation"]["status"] == "unavailable"
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as connection:
        connection.row_factory = sqlite3.Row
        request = connection.execute("SELECT * FROM events WHERE event_id=?",
                                     (capture["models"][1]["request"]["event_id"],)).fetchone()
        assert reconstruct(connection, body(connection, request["payload_ref"])) == REQUESTS[1]
        details = json.loads(request["details_json"])
        assert any(item["pointer"] == "/messages/2/content" and item["reason"] == "capture_unavailable"
                   for item in details["unresolved_answers"])
        assert connection.execute("SELECT count(*) FROM links").fetchone()[0] == 0
        assert connection.execute("SELECT count(*) FROM links WHERE source_event_id=?", (earlier_id,)).fetchone()[0] == 0
    checked = successful(inspect.check(path, capture["run_id"]))
    bindings = next(row for row in checked["items"] if row["id"] == "answer_bindings")
    assert bindings["status"] == "unknown"
    assert bindings["unknown"] > 0


@pytest.mark.parametrize("failure", ["error", "cancelled"])
def test_host_model_failure_remains_host_owned(tmp_path, run_sdk_async, failure):
    example = load_example()

    class InterruptedServices(example.LocalServices):
        async def provider(self, request):
            if self.provider_requests:
                if failure == "cancelled":
                    raise asyncio.CancelledError("host cancellation")
                raise RuntimeError("host model failure")
            return await super().provider(request)

    def leaves(error):
        if isinstance(error, BaseExceptionGroup):
            return [item for child in error.exceptions for item in leaves(child)]
        return [(type(error).__name__, str(error))]

    async def scenario():
        outcomes, observations = [], []
        for capture in (False, True):
            services = InterruptedServices()
            recorder = Recorder(tmp_path / "interrupted.sqlite", capture="content", coverage=example.COVERAGE) if capture else None
            try:
                await example.run_host(recorder, services=services)
            except BaseException as error:
                outcomes.append(leaves(error))
            else:
                pytest.fail("The host's model failure must propagate")
            finally:
                if recorder is not None:
                    closed = await recorder.aclose(timeout=5)
                    assert closed.writer_stopped and closed.portable
            observations.append(services.observations())
        assert outcomes[0] == outcomes[1]
        expected = ("CancelledError", "host cancellation") if failure == "cancelled" else ("RuntimeError", "host model failure")
        assert outcomes[0] == [expected]
        assert observations[0] == observations[1]
        assert observations[0]["tool_calls"] == [{"name": "answer", "arguments": {}}]
        return recorder.capture_scope().run_id

    run_id = run_sdk_async(scenario(), timeout=30)
    turns = successful(inspect.find_turns(tmp_path / "interrupted.sqlite", run_id))["items"]
    assert len(turns) == 1
    assert turns[0]["turn"]["host_outcome"] == failure
