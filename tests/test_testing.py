"""Test the assertion wrapper through real recordings and controlled gate seams."""
from unittest.mock import Mock

import pytest

from mcp_flightrecorder import inspect
from mcp_flightrecorder._inspect_common import DEFAULT_OUTPUT_BYTES, error_envelope
from mcp_flightrecorder._inspect_text import render
from mcp_flightrecorder.testing import EvidenceReadError, assert_evidence
from .test_inspection_gate import local_answers, record


def test_forwards_policy_and_returns_gate_result_without_mutating_inputs(tmp_path, monkeypatch):
    path, run, _ = local_answers(tmp_path)
    expect = ["model", "host", "mcp"]
    minimum = {"answer_occurrences": 2}
    calls = []
    gate = inspect.gate

    def observed_gate(*args, **kwargs):
        result = gate(*args, **kwargs)
        calls.append((args, kwargs, result))
        return result

    monkeypatch.setattr(inspect, "gate", observed_gate)
    result = assert_evidence(path, run=run, expect=expect, min_activity=minimum, allow_local_tools=True)

    (args, kwargs, original), = calls
    assert args == (path,)
    assert kwargs == {"run_id": run, "expect": expect, "min_activity": minimum, "answers": "mcp"}
    assert kwargs["expect"] is expect and kwargs["min_activity"] is minimum
    assert expect == ["model", "host", "mcp"] and minimum == {"answer_occurrences": 2}
    assert result is original
    assert result["scope"]["run_id"] == run
    assert result["data"]["status"] == "pass"
    assert result["data"]["expectations"]["answers"] == "mcp"
    assert result["data"]["excluded_count"] == len(result["data"]["excluded"]) == 1


def test_defaults_are_fresh_and_preserve_all_answer_scope(tmp_path, monkeypatch):
    path, run, _ = record(tmp_path)
    gate = Mock(wraps=inspect.gate)
    monkeypatch.setattr(inspect, "gate", gate)

    for _ in range(2):
        result = assert_evidence(path)
        assert result["data"]["status"] == "pass"
        assert result["scope"]["run_id"] == run
    assert gate.call_count == 2
    first, second = gate.call_args_list
    assert first.kwargs == second.kwargs == {
        "run_id": None, "expect": ("model", "host", "mcp"),
        "min_activity": {"answer_occurrences": 1}, "answers": "all",
    }
    assert first.kwargs["min_activity"] is not second.kwargs["min_activity"]


@pytest.mark.parametrize("policy", ["false", 1, None, object()])
def test_local_scope_requires_exact_boolean(tmp_path, monkeypatch, policy):
    gate = Mock()
    monkeypatch.setattr(inspect, "gate", gate)
    with pytest.raises(EvidenceReadError, match="allow_local_tools must be a boolean") as caught:
        assert_evidence(tmp_path / "unused.sqlite", allow_local_tools=policy)
    assert caught.value.reason == "invalid_allow_local_tools"
    gate.assert_not_called()


@pytest.mark.parametrize("status", ["fail", "unknown"])
def test_completed_negative_reuses_gate_explanation(tmp_path, monkeypatch, status):
    path, run, _ = (
        record(tmp_path, submitted="changed") if status == "fail" else local_answers(tmp_path)
    )
    result = inspect.gate(path, run, expect=("model", "host", "mcp"), min_activity={"answer_occurrences": 1})
    assert result["ok"] and result["data"]["status"] == status
    gate = Mock(wraps=inspect.gate)
    monkeypatch.setattr(inspect, "gate", gate)
    with pytest.raises(AssertionError, match="VERDICT: " + status.upper()) as caught:
        assert_evidence(path, run=run)
    assert str(caught.value) == render(result)
    gate.assert_called_once()


@pytest.mark.parametrize(("options", "reason"), [
    ({}, "storage_failure"),
    ({"min_activity": {}}, "invalid_activity"),
    ({"min_activity": {"answer_occurrences": True}}, "invalid_min_activity"),
    ({"expect": ("mcp",)}, "invalid_expect"),
    ({"run": 7}, "invalid_run_id"),
])
def test_real_read_and_configuration_errors_keep_gate_reason(tmp_path, options, reason):
    path = tmp_path / "absent.sqlite"
    with pytest.raises(EvidenceReadError) as caught:
        assert_evidence(path, **options)
    assert caught.value.reason == reason
    assert list(tmp_path.iterdir()) == []


def test_directory_does_not_select_an_existing_recording(tmp_path):
    path, _, _ = record(tmp_path)
    assert assert_evidence(path)["data"]["status"] == "pass"
    with pytest.raises(EvidenceReadError):
        assert_evidence(tmp_path)


@pytest.mark.parametrize(("code", "reason"), [
    ("query_timeout", "deadline_expired"),
    ("limit_exceeded", "required_output"),
])
def test_wrapper_preserves_controlled_operational_envelope(tmp_path, monkeypatch, code, reason):
    message = "Original gate message."
    gate = Mock(return_value=error_envelope("gate", code, reason, message))
    monkeypatch.setattr(inspect, "gate", gate)
    with pytest.raises(EvidenceReadError) as caught:
        assert_evidence(tmp_path / "unused.sqlite")
    assert caught.value.reason == reason
    assert str(caught.value) == message
    gate.assert_called_once()


def test_wrapper_uses_renderer_byte_bound(tmp_path, monkeypatch):
    path, run, _ = record(tmp_path, submitted="changed")
    result = inspect.gate(path, run, expect=("model", "host", "mcp"), min_activity={"answer_occurrences": 1})
    # Exercise renderer truncation with a controlled envelope.
    result["unknowns"] = [{"code": "sample", "ids": {"event_id": "x" * DEFAULT_OUTPUT_BYTES}}]
    monkeypatch.setattr(inspect, "gate", Mock(return_value=result))
    with pytest.raises(AssertionError, match="VERDICT: FAIL") as caught:
        assert_evidence(path)
    explanation = str(caught.value)
    assert "CUTOFF: text byte budget reached" in explanation
    assert len(explanation.encode("utf-8")) <= DEFAULT_OUTPUT_BYTES


@pytest.mark.parametrize("exception", [RuntimeError("programmer error"), KeyboardInterrupt()])
def test_unrelated_exceptions_propagate(tmp_path, monkeypatch, exception):
    monkeypatch.setattr(inspect, "gate", Mock(side_effect=exception))
    with pytest.raises(type(exception)) as caught:
        assert_evidence(tmp_path / "unused.sqlite")
    assert caught.value is exception
