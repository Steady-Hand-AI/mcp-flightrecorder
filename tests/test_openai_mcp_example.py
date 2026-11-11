"""Check actual SDK requests and the facade's retained evidence independently."""
from contextlib import closing
import json
from pathlib import Path
import runpy
import sqlite3

import mcp_types
import pytest

from mcp_flightrecorder import inspect
from mcp_flightrecorder.testing import assert_evidence

from .fixture_checks import body, reconstruct


EXAMPLES = Path(__file__).resolve().parents[1] / "examples"
QUESTION = "Use the answer tool, then report its result."
DISCOVERED_TOOLS = [{
    "type": "function",
    "function": {
        "name": "answer",
        "description": "Return the example's answer.",
        "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
    },
}]
FIRST_REQUEST = {
    "model": "local-example-model",
    "messages": [{"role": "user", "content": QUESTION}],
    "tools": DISCOVERED_TOOLS,
    "temperature": 0,
}
ASSISTANT = {
    "role": "assistant",
    "content": None,
    "tool_calls": [{"id": "call_answer", "type": "function",
                    "function": {"name": "answer", "arguments": "{}"}}],
}
TEXT_PARTS = [{"type": "text", "text": "4"}, {"type": "text", "text": "2"}]


@pytest.fixture
def example():
    return runpy.run_path(str(EXAMPLES / "openai_mcp.py"))


def observed_requests(peers):
    return [json.loads(request) for request in peers.serialized_requests]


def captured_run(path):
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as connection:
        connection.row_factory = sqlite3.Row
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
        requests = connection.execute(
            "SELECT payload_ref FROM events WHERE kind='model.request' ORDER BY sequence"
        ).fetchall()
        submitted = [reconstruct(connection, body(connection, row["payload_ref"])) for row in requests]
        operation, = connection.execute(
            "SELECT name, status FROM operations WHERE method='tools/call'"
        ).fetchall()
        return submitted, tuple(operation)


@pytest.mark.parametrize(("case", "content", "operation_status", "raw_equal", "verdict"), [
    ("single", "42", "ok", True, "pass"),
    ("multiple", TEXT_PARTS, "ok", True, "pass"),
    ("tool-error", "Tool unavailable", "tool_error", True, "pass"),
    ("joined", "42", "ok", False, "fail"),
    ("prepared", "Tool result: 4 | 2", "ok", False, "pass"),
])
def test_actual_sdk_submission_and_evidence(tmp_path, example, run_sdk_async, case, content,
                                           operation_status, raw_equal, verdict):
    path = tmp_path / "roundtrip.sqlite"
    peers = example["ExamplePeers"](case)
    summary = run_sdk_async(example["run_example"](path, case=case, peers=peers))

    followup = {**FIRST_REQUEST, "messages": [
        *FIRST_REQUEST["messages"], ASSISTANT,
        {"role": "tool", "tool_call_id": "call_answer", "content": content},
    ]}
    expected_requests = [FIRST_REQUEST, followup]
    observed = observed_requests(peers)
    assert observed == expected_requests
    assert summary["answer"] == "Tool response received."
    assert peers.list_calls == 1
    assert peers.tool_calls == [{"name": "answer", "arguments": {}}]
    assert captured_run(path) == (expected_requests, ("answer", operation_status))

    if case == "joined":
        assert observed[1]["messages"][-1]["content"] != TEXT_PARTS
        with pytest.raises(AssertionError, match="VERDICT: FAIL"):
            assert_evidence(path)
        result = inspect.gate(path, expect=("model", "host", "mcp"),
                              min_activity={"answer_occurrences": 1})
    else:
        result = assert_evidence(path)
    assert result["ok"] and result["data"]["status"] == verdict
    witness, = result["data"]["answer_witnesses"]
    assert witness["origin"] == "mcp" and witness["text_equal"] is raw_equal
    assert witness["reason"] == ("text_mismatch" if case == "joined" else None)
    assert witness["preparation"] == ("matches" if case == "prepared" else "unknown")


@pytest.mark.parametrize("shape", ["mixed", "empty", "structured"])
def test_unsupported_native_result_stops_before_followup(tmp_path, example, run_sdk_async, shape):
    path = tmp_path / "unsupported.sqlite"
    alternative = {
        "mixed": None,
        "empty": mcp_types.CallToolResult(content=[]),
        "structured": mcp_types.CallToolResult(
            content=[mcp_types.TextContent(type="text", text="42")], structured_content={"value": 42}
        ),
    }[shape]
    peers = example["ExamplePeers"]("unsupported", result=alternative)
    with pytest.raises(example["UnsupportedToolResult"], match="complete nonempty text-only MCP result"):
        run_sdk_async(example["run_example"](path, case="unsupported", peers=peers))
    assert observed_requests(peers) == [FIRST_REQUEST]
    assert peers.tool_calls == [{"name": "answer", "arguments": {}}]
    assert captured_run(path) == ([FIRST_REQUEST], ("answer", "ok"))


def test_transform_propagates_original_exception(tmp_path, example, run_sdk_async):
    path = tmp_path / "callback.sqlite"
    peers = example["ExamplePeers"]("multiple")
    original = RuntimeError("example callback sentinel")
    arguments = []

    def transform(parts):
        arguments.append(parts)
        raise original

    with pytest.raises(RuntimeError) as caught:
        run_sdk_async(example["run_example"](path, case="multiple", peers=peers, transform=transform))
    assert caught.value is original
    assert arguments == [["4", "2"]]
    assert observed_requests(peers) == [FIRST_REQUEST]
    assert captured_run(path) == ([FIRST_REQUEST], ("answer", "ok"))


def test_transform_must_return_string(tmp_path, example, run_sdk_async):
    path = tmp_path / "callback.sqlite"
    peers = example["ExamplePeers"]("multiple")
    with pytest.raises(TypeError, match="return.*str"):
        run_sdk_async(example["run_example"](path, case="multiple", peers=peers, transform=tuple))
    assert observed_requests(peers) == [FIRST_REQUEST]
    assert captured_run(path) == ([FIRST_REQUEST], ("answer", "ok"))
