"""Complete-population gate controls over real recorder evidence.

SQL changes below are explicitly damaged reader inputs, not capture fixtures.
"""
from contextlib import closing, nullcontext
import json
import sqlite3
import subprocess
import sys

import pytest

from mcp_flightrecorder import Recorder, inspect


COVERAGE = {"producers": {"model": "test.model", "host": "test.host", "mcp": "test.mcp"}, "model_complete": True}
EXPECT = ["model", "host", "mcp"]
MINIMUM = {"turns": 1, "model_requests": 2, "model_responses": 2,
           "decisions": 1, "tool_calls": 1, "answer_occurrences": 1}


def observe(recorder, kind, payload, **facts):
    receipt = recorder.observe(kind, scope=recorder.capture_scope(), payload=payload, facts=facts)
    assert receipt.status == "accepted", receipt
    return receipt


def record(tmp_path, *, capture="content", prepared=False, submitted=None,
           comparisons=True, attribution="propagated", status="ok", count=1,
           bad_after=None, replay=False, discovery=False, reuse_result=False):
    path = tmp_path / "recording.sqlite"
    recorder = Recorder(path, capture=capture, coverage=COVERAGE, comparisons=comparisons,
                        limits={"ordinary_items": 10000, "ordinary_bytes": 64 * 1024 * 1024,
                                "completion_items": 1000, "completion_bytes": 8 * 1024 * 1024,
                                "committed_cache_items": 10000})
    run_id = recorder.capture_scope().run_id
    turns = []
    try:
        for index in range(count):
            with recorder.turn("A deterministic roundtrip"):
                first = observe(recorder, "model.request", {"messages": []}, messages=[])
                decision = observe(recorder, "model.tool_call", {}, name="answer", model_call_id=first.model_call_id)
                observe(recorder, "model.response", {}, model_call_id=first.model_call_id)
                if discovery:
                    listing = recorder.start_operation(scope=recorder.capture_scope(), client_id="test",
                                                       method="tools/list", arguments={})
                    assert listing.status == "accepted"
                    assert recorder.finish_operation(listing.ticket, status="ok", result={"tools": []}).status == "accepted"
                dispatch = recorder.dispatch(action="execute", name="answer", arguments={},
                                             decision_event_ids=[] if attribution == "host" else [decision.event_id]) if attribution in {"host", "dispatch"} else nullcontext()
                with dispatch:
                    start = recorder.start_operation(scope=recorder.capture_scope(), client_id="test",
                                                     method="tools/call", name="answer", arguments={},
                                                     link_basis="host_hook" if attribution in {"host", "dispatch"} else attribution,
                                                     decision_id=decision.decision_id if attribution in {"propagated", "dispatch"} else None)
                    assert start.status == "accepted", start
                    end = recorder.finish_operation(start.ticket, status=status, result="42", text_paths=[""])
                    assert end.status == "accepted", end
                prep = observe(recorder, "host.tool_result", "Tool result: 42",
                               sources=[{"event_id": end.event_id}], text_paths=[""]) if prepared else None
                text = submitted if submitted is not None else "Tool result: 42" if prepared else "42"
                if bad_after is not None and index >= bad_after:
                    text = "Changed text"
                message = {"role": "tool", "content": text}
                binding = {"bindings": [{"decision_event_id": decision.event_id, "pointer": "/content",
                                          "result_event_id": turns[0]["result"] if reuse_result and turns else end.event_id,
                                          "text_paths": [""]}]}
                witness = None
                if replay:
                    witness = observe(recorder, "model.request", {"messages": [{"role": "user", "content": "Context"}, message]},
                                      messages=[{"bindings": []}, binding],
                                      insertions=[{"source_event_id": prep.event_id, "target_pointer": "/messages/1/content", "content": text}] if prep else [])
                    observe(recorder, "model.response", {}, model_call_id=witness.model_call_id)
                insertion = [{"source_event_id": prep.event_id, "target_pointer": "/messages/0/content", "content": text}] if prep else []
                request = observe(recorder, "model.request", {"messages": [message]}, messages=[binding], insertions=insertion)
                observe(recorder, "model.response", {}, model_call_id=request.model_call_id)
                turns.append({"turn": request.turn_id, "first": first.event_id, "decision": decision.event_id,
                              "decision_id": decision.decision_id, "operation": start.operation_id,
                              "result": end.event_id, "prepared": prep.event_id if prep else None,
                              "request": request.event_id, "witness": witness.event_id if witness else request.event_id})
    finally:
        closed = recorder.close()
        assert closed.portable and closed.writer_stopped
    return path, run_id, turns


def mutate(path, sql, parameters=()):
    with closing(sqlite3.connect(path)) as connection:
        connection.execute(sql, parameters)
        connection.commit()


def details(path, event_id, update):
    with closing(sqlite3.connect(path)) as connection:
        value = json.loads(connection.execute("SELECT details_json FROM events WHERE event_id=?", (event_id,)).fetchone()[0])
        update(value)
        connection.execute("UPDATE events SET details_json=? WHERE event_id=?", (json.dumps(value), event_id))
        connection.commit()


def gate(path, run_id=None, **kwargs):
    return inspect.gate(path, run_id, expect=EXPECT, min_activity=MINIMUM, **kwargs)


def item(result, name):
    assert result["ok"], result
    return next(value for value in result["data"]["items"] if value["id"] == name)


@pytest.mark.parametrize("capture", ["content", "metadata"])
@pytest.mark.parametrize("prepared", [False, True])
def test_gate_accepts_original_evidence_preserving_raw_facts(tmp_path, capture, prepared):
    path, run, turns = record(tmp_path, capture=capture, prepared=prepared)
    before = path.read_bytes()
    raw = inspect.explain(path, run, turns[0]["turn"])
    result = gate(path)
    assert result["ok"] and result["data"]["status"] == "pass", result
    assert result["data"]["evaluation_complete"] is True
    assert result["scope"]["run_id"] == run
    assert result["data"]["expectations"] == {"boundaries": sorted(EXPECT), "min_activity": MINIMUM}
    assert result["data"]["actual_activity"] == MINIMUM
    assert result["data"]["stats"]["text_mismatch"] == int(prepared)
    assert inspect.explain(path, run, turns[0]["turn"]) == raw
    assert before == path.read_bytes()
    assert "Tool result: 42" not in json.dumps(result)


@pytest.mark.parametrize("status", ["ok", "tool_error"])
@pytest.mark.parametrize("attribution", ["propagated", "dispatch", "host"])
def test_explicit_attribution_tool_error_and_discovery_controls(tmp_path, status, attribution):
    path, run, _ = record(tmp_path, status=status, attribution=attribution, discovery=True)
    result = gate(path, run)
    assert result["ok"] and result["data"]["status"] == "pass", result
    assert item(result, "tool_attribution")["applicable"] == 1
    assert item(result, "operation_lifecycle")["applicable"] == 2


@pytest.mark.parametrize("expect", [None, [], ["mcp"], ["model"], ["host"], ["model", "host", "host"], "model,host", ["model", "host", 1]])
def test_gate_requires_explicit_host_boundaries(tmp_path, expect):
    result = inspect.gate(tmp_path / "absent.sqlite", expect=expect, min_activity={"model_requests": 1})
    assert not result["ok"] and result["error"]["reason"] == "invalid_expect"


@pytest.mark.parametrize("minimum", [None, {}, {"turns": 1}, {"model_requests": 0}, {"model_requests": True},
                                     {"model_requests": 1.0}, {"model_requests": -1}, {"model_requests": 2**63}, {"unknown": 1}])
def test_gate_rejects_vacuous_or_malformed_activity(tmp_path, minimum):
    result = inspect.gate(tmp_path / "absent.sqlite", expect=EXPECT, min_activity=minimum)
    assert not result["ok"] and result["error"]["code"] == "invalid_request"


@pytest.mark.parametrize("configured", [False, True])
def test_empty_recording_and_empty_turn_cannot_pass(tmp_path, configured):
    path = tmp_path / "empty.sqlite"
    coverage = COVERAGE if configured else {"producers": {"model": None, "host": None, "mcp": None}}
    recorder = Recorder(path, capture="content", coverage=coverage)
    if configured:
        with recorder.turn("No observed activity"):
            pass
    assert recorder.close().portable
    result = inspect.gate(path, expect=EXPECT, min_activity={"turns": 1, "model_requests": 1})
    assert result["ok"] and result["data"]["status"] == "fail", result
    assert result["data"]["actual_activity"]["turns"] == int(configured)
    assert result["data"]["actual_activity"]["model_requests"] == 0
    assert item(result, "minimum_activity")["failed"] == (1 if configured else 2)


@pytest.mark.parametrize("prepared", [False, True])
def test_true_or_changed_preparation_mismatch_fails(tmp_path, prepared):
    path, run, _ = record(tmp_path, prepared=prepared, submitted="Changed text")
    result = gate(path, run)
    assert result["data"]["status"] == "fail", result
    assert item(result, "answer_comparisons")["failed"] == 1


@pytest.mark.parametrize("damage", ["disabled", "unsupported-preparation", "missing-source", "missing-witness",
                                    "unresolved", "omitted", "original-unresolved", "original-omitted"])
def test_unavailable_comparison_or_association_never_passes(tmp_path, damage):
    path, run, turns = record(tmp_path, capture="metadata", prepared=True, comparisons=damage != "disabled", replay=True)
    last = turns[0]
    if damage == "unsupported-preparation":
        details(path, last["prepared"], lambda value: value["result_text_digest"].update(projection="unsupported/1"))
    elif damage == "missing-source":
        mutate(path, "UPDATE links SET source_event_id='absent' WHERE relation='transformed_from'")
    elif damage == "missing-witness":
        mutate(path, "DELETE FROM events WHERE event_id=?", (last["witness"],))
    elif damage in {"unresolved", "original-unresolved", "omitted", "original-omitted"}:
        original = damage.startswith("original-")
        target = last["witness"] if original else last["request"]
        pointer = "/messages/1/content" if original else "/messages/0/content"
        if damage.endswith("omitted"):
            details(path, target, lambda value: value.update(unresolved_answers_omitted_count=1))
        else:
            details(path, target, lambda value: value.update(unresolved_answers=[{"pointer": pointer, "reason": "capture_unavailable"}]))
    result = gate(path, run)
    assert result["ok"] and result["data"]["status"] != "pass", result
    assert item(result, "answer_comparisons")["unknown"] > 0 or item(result, "answer_comparisons")["failed"] > 0


@pytest.mark.parametrize("damage", ["none", "missing-decision", "non-authoritative", "wrong-model", "missing-dispatch", "dispatch-sources", "dispatch-order"])
def test_tool_call_attribution_requires_authoritative_consistent_evidence(tmp_path, damage):
    path, run, turns = record(tmp_path, attribution="dispatch" if damage.startswith("dispatch") or damage == "missing-dispatch" else "none" if damage == "none" else "propagated")
    last = turns[0]
    if damage == "missing-decision":
        mutate(path, "DELETE FROM events WHERE event_id=?", (last["decision"],))
    elif damage == "non-authoritative":
        mutate(path, "UPDATE events SET provenance='imported' WHERE event_id=?", (last["decision"],))
    elif damage == "wrong-model":
        mutate(path, "UPDATE operations SET model_call_id='another-call'")
        mutate(path, "UPDATE events SET model_call_id='another-call' WHERE kind IN ('mcp.request','mcp.response')")
    elif damage == "missing-dispatch":
        mutate(path, "DELETE FROM events WHERE kind='host.dispatch'")
    elif damage == "dispatch-sources":
        mutate(path, "UPDATE events SET details_json=json_set(details_json,'$.decision_event_ids',json('[]')) WHERE kind='host.dispatch'")
    elif damage == "dispatch-order":
        mutate(path, "UPDATE events SET sequence=99999 WHERE kind='host.dispatch'")
    result = gate(path, run)
    assert result["ok"] and result["data"]["status"] != "pass", result
    assert item(result, "tool_attribution")["status"] in {"fail", "unknown"}


@pytest.mark.parametrize("damage", ["incomplete-turn", "loss", "missing-operation", "non-authoritative"])
def test_capture_integrity_unknowns_cannot_hide_behind_activity(tmp_path, damage):
    path, run, turns = record(tmp_path)
    if damage == "incomplete-turn":
        mutate(path, "UPDATE turns SET ended_at=NULL")
    elif damage == "loss":
        mutate(path, "UPDATE runs SET dropped_events=1")
    elif damage == "missing-operation":
        mutate(path, "DELETE FROM operations")
    else:
        mutate(path, "UPDATE events SET provenance='imported' WHERE event_id=?", (turns[0]["result"],))
    result = gate(path, run)
    assert result["ok"] and result["data"]["status"] != "pass", result


@pytest.mark.parametrize("damage", ["mismatch", "unknown"])
def test_decisive_evidence_after_200th_turn_is_evaluated(tmp_path, damage):
    path, run, turns = record(tmp_path, count=201, bad_after=200 if damage == "mismatch" else None)
    if damage == "unknown":
        details(path, turns[-1]["request"], lambda value: value.update(unresolved_answers_omitted_count=1))
    result = gate(path, run)
    assert result["ok"] and result["data"]["evaluation_complete"] is True, result
    assert result["data"]["status"] == ("fail" if damage == "mismatch" else "unknown"), result
    assert result["data"]["actual_activity"]["turns"] == 201
    assert result["data"]["actual_activity"]["answer_occurrences"] == 201
    assert item(result, "answer_comparisons")["applicable"] == 201
    assert item(result, "answer_comparisons")["witnesses"][0]["ids"]["request_event_id"] == turns[-1]["request"]


def test_full_counts_survive_witness_and_output_sampling(tmp_path):
    path, run, _ = record(tmp_path, count=31, bad_after=0)
    result = gate(path, run, max_output_bytes=8192)
    comparisons = item(result, "answer_comparisons")
    assert result["data"]["status"] == "fail" and result["data"]["evaluation_complete"] is True
    assert comparisons["failed"] == comparisons["applicable"] == 31
    assert len(comparisons["witnesses"]) <= 10
    assert comparisons["witnesses_omitted"] == 31 - len(comparisons["witnesses"])
    assert len(json.dumps(result, ensure_ascii=False, separators=(",", ":")).encode()) <= 8192


def test_late_failure_dominates_unknowns_after_witness_samples_are_full(tmp_path):
    path, run, turns = record(tmp_path, count=201, bad_after=200)
    for turn in turns[:20]:
        details(path, turn["request"], lambda value: value.update(unresolved_answers_omitted_count=1))
    result = gate(path, run, max_output_bytes=8192)
    comparisons = item(result, "answer_comparisons")
    assert result["data"]["status"] == "fail" and result["data"]["evaluation_complete"] is True
    assert comparisons["applicable"] == 201
    assert comparisons["unknown"] == 20 and comparisons["failed"] == 1
    assert comparisons["witnesses_omitted"] == 21 - len(comparisons["witnesses"])
    assert all(witness["ids"]["request_event_id"] != turns[-1]["request"] for witness in comparisons["witnesses"])


def test_mcp_boundary_is_optional_but_host_model_expectations_are_explicit(tmp_path):
    path = tmp_path / "model.sqlite"
    coverage = {"producers": {"model": "test.model", "host": "test.host", "mcp": None}, "model_complete": True}
    recorder = Recorder(path, capture="metadata", coverage=coverage)
    with recorder.turn("No tool call requested"):
        request = observe(recorder, "model.request", {"messages": []}, messages=[])
        observe(recorder, "model.response", {}, model_call_id=request.model_call_id)
    assert recorder.close().portable
    result = inspect.gate(path, expect=["model", "host"], min_activity={"model_requests": 1, "model_responses": 1})
    assert result["ok"] and result["data"]["status"] == "pass", result
    assert item(result, "coverage.mcp")["status"] == "not_applicable"


def test_one_owned_snapshot_and_connection_cleanup(tmp_path, monkeypatch):
    from mcp_flightrecorder import _inspect_common, _inspect_gate

    path, run, _ = record(tmp_path)
    mutate(path, "PRAGMA journal_mode=WAL")
    opened = []
    original_open = _inspect_common.open_read
    original_check = _inspect_gate._check

    def open_read(ctx, selected_path):
        original_open(ctx, selected_path)
        opened.append(ctx.connection)

    def change_after_snapshot(ctx, expected, **kwargs):
        # open_read has already selected and pinned the schema and run snapshot.
        mutate(path, "UPDATE runs SET dropped_events=1")
        return original_check(ctx, expected, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(_inspect_common, "open_read", open_read)
        patch.setattr(_inspect_gate, "_check", change_after_snapshot)
        first = gate(path, run)
    assert first["data"]["status"] == "pass", first
    assert len(opened) == 1
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        opened[0].execute("SELECT 1")
    assert gate(path, run)["data"]["status"] == "fail"


def test_timeout_and_required_output_failure_never_return_partial_pass(tmp_path, monkeypatch):
    from mcp_flightrecorder import _inspect_gate

    path, run, _ = record(tmp_path)
    original = _inspect_gate._check

    def expire(ctx, expected, **kwargs):
        result = original(ctx, expected, **kwargs)
        ctx.deadline = 0
        return result

    with monkeypatch.context() as patch:
        patch.setattr(_inspect_gate, "_check", expire)
        timed_out = gate(path, run)
    assert not timed_out["ok"] and timed_out["error"]["code"] == "query_timeout"
    assert timed_out["data"] is None
    small = gate(path, run, max_output_bytes=1024)
    assert not small["ok"] and small["error"]["code"] == "limit_exceeded"
    assert small["data"] is None


@pytest.mark.parametrize("case,expected", [("pass", 0), ("fail", 2), ("unknown", 4), ("missing", 1), ("bounded", 1), ("timeout", 1)])
def test_actual_cli_exit_matches_gate_status(tmp_path, case, expected):
    path, run, _ = record(tmp_path, submitted="Changed" if case == "fail" else None, comparisons=case != "unknown")
    command = [sys.executable, "-I", "-B", "-m", "mcp_flightrecorder", "gate", str(path),
               "--run", run, "--expect", "model,host,mcp"]
    if case != "missing":
        command += ["--min-model-requests", "2", "--min-answer-occurrences", "1"]
    if case == "bounded":
        command += ["--max-output-bytes", "1024"]
    if case == "timeout":
        command += ["--timeout", "0.000001"]
    completed = subprocess.run(command, cwd=tmp_path, capture_output=True, text=True)
    assert completed.returncode == expected, (completed.stdout, completed.stderr)
    result = json.loads(completed.stdout)
    if case in {"pass", "fail", "unknown"}:
        assert result["data"]["status"] == case
    else:
        assert not result["ok"]
    if case == "timeout":
        assert result["error"]["code"] == "query_timeout"
        assert gate(path, run)["data"]["status"] == "pass"


def test_display_cutoff_is_counted_before_envelope_pruning(tmp_path):
    path, run, _ = record(tmp_path)
    mutate(path, "UPDATE runs SET started_at=?", ("x" * 1000,))
    result = gate(path, run)
    assert result["ok"] and result["data"]["status"] == "unknown", result
    assert item(result, "evidence_bounds")["unknown"] == 1


def test_equal_bytes_cannot_substitute_a_future_result(tmp_path):
    path, run, turns = record(tmp_path, count=2)
    first, second = turns
    mutate(path, "UPDATE links SET details_json=json_set(details_json,'$.result_event_id',?) WHERE relation='answers' AND witness_event_id=?",
           (second["result"], first["request"]))
    result = gate(path, run)
    assert result["ok"] and result["data"]["status"] == "fail", result
    assert item(result, "answer_comparisons")["failed"] == 1


@pytest.mark.parametrize("kind", ["mcp.request", "mcp.response"])
def test_extra_authoritative_endpoint_cannot_reuse_a_valid_operation(tmp_path, kind):
    path, run, _ = record(tmp_path)
    with closing(sqlite3.connect(path)) as connection:
        connection.row_factory = sqlite3.Row
        row = dict(connection.execute("SELECT * FROM events WHERE kind=?", (kind,)).fetchone())
        row["event_id"] += "-extra"
        row["sequence"] = connection.execute("SELECT max(sequence)+1 FROM events").fetchone()[0]
        connection.execute("INSERT INTO events (" + ",".join(row) + ") VALUES (" + ",".join("?" for _ in row) + ")", tuple(row.values()))
        connection.commit()
    result = gate(path, run)
    assert result["ok"] and result["data"]["status"] == "fail", result
    assert item(result, "tool_witnesses")["failed"] == 1


@pytest.mark.parametrize("kind", ["mcp.request", "mcp.response"])
def test_canonical_mcp_endpoint_requires_mcp_boundary(tmp_path, kind):
    path, run, _ = record(tmp_path)
    mutate(path, "UPDATE events SET boundary='host',source_id='test.host' WHERE kind=?", (kind,))
    result = gate(path, run)
    assert result["ok"] and result["data"]["status"] == "fail", result
    assert item(result, "tool_witnesses")["failed"] == 1


def test_original_witness_precedes_later_replay_but_still_requires_prior_result(tmp_path):
    path, run, turns = record(tmp_path, replay=True)
    with closing(sqlite3.connect(path)) as connection:
        rows = connection.execute("SELECT sequence,event_id FROM events ORDER BY sequence DESC").fetchall()
        connection.executemany("UPDATE events SET sequence=? WHERE event_id=?", [(sequence * 10, event_id) for sequence, event_id in rows])
        connection.commit()
    mutate(path, "UPDATE events SET sequence=(SELECT sequence FROM events WHERE event_id=?)+1 WHERE event_id=?",
           (turns[0]["witness"], turns[0]["result"]))
    result = gate(path, run)
    assert result["ok"] and result["data"]["status"] == "fail", result
    assert item(result, "answer_comparisons")["failed"] == 2


def test_answer_source_requires_its_authoritative_model_request(tmp_path):
    path, run, turns = record(tmp_path, attribution="host")
    mutate(path, "UPDATE events SET model_call_id='missing-call' WHERE event_id=?", (turns[0]["decision"],))
    result = gate(path, run)
    assert result["ok"] and result["data"]["status"] == "unknown", result
    assert item(result, "answer_comparisons")["unknown"] == 1


def test_explicit_result_reuse_across_turns_is_not_inferred_operation_ownership(tmp_path):
    path, run, _ = record(tmp_path, count=2, reuse_result=True)
    result = gate(path, run)
    assert result["ok"] and result["data"]["status"] == "pass", result


@pytest.mark.parametrize("capture", ["content", "metadata"])
def test_prepared_replay_has_independent_valid_original_and_current_occurrences(tmp_path, capture):
    path, run, _ = record(tmp_path, capture=capture, prepared=True, replay=True)
    result = gate(path, run)
    assert result["ok"] and result["data"]["status"] == "pass", result
    assert result["data"]["stats"]["text_mismatch"] == 2
    assert item(result, "answer_comparisons")["applicable"] == 2


@pytest.mark.parametrize("kind", ["model.request", "model.response", "model.tool_call", "host.tool_result", "mcp.request"])
def test_known_event_contract_requires_its_declared_boundary(tmp_path, kind):
    if kind.startswith("model."):
        path = tmp_path / "recording.sqlite"
        recorder = Recorder(path, capture="content", coverage=COVERAGE)
        with recorder.turn("Model-only observation"):
            request = observe(recorder, "model.request", {"messages": []}, messages=[])
            observe(recorder, "model.tool_call", {}, name="answer", model_call_id=request.model_call_id)
            observe(recorder, "model.response", {}, model_call_id=request.model_call_id)
        assert recorder.close().portable
    else:
        path, _, _ = record(tmp_path, prepared=True, discovery=True)
    expected = inspect.gate(path, expect=EXPECT, min_activity={"model_requests": 1})
    assert expected["ok"] and expected["data"]["status"] == "pass", expected
    boundary = "model" if kind.startswith("host.") else "host"
    mutate(path, "UPDATE events SET boundary=?,source_id=? WHERE kind=?" + (" AND method='tools/list'" if kind == "mcp.request" else ""),
           (boundary, "test." + boundary, kind))
    result = inspect.gate(path, expect=EXPECT, min_activity={"model_requests": 1})
    assert result["ok"] and result["data"]["status"] == "fail", result
    assert item(result, "authoritative_observations")["failed"] > 0
