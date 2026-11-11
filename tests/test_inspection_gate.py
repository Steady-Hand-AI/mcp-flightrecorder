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
    assert result["data"]["expectations"] == {"boundaries": sorted(EXPECT), "min_activity": MINIMUM, "answers": "all"}
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
    scoped = inspect.gate(path, expect=["model", "host"], min_activity={"model_requests": 1}, answers="mcp")
    assert scoped["data"]["status"] == "unknown"
    assert item(scoped, "mcp_answer_scope")["witnesses"][0]["reason"] == "no_mcp_answers_in_scope"


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


@pytest.mark.parametrize("answers", ["all", "mcp"])
def test_timeout_and_required_output_failure_never_return_partial_pass(tmp_path, monkeypatch, answers):
    from mcp_flightrecorder import _inspect_gate

    path, run, _ = record(tmp_path)
    original = _inspect_gate._check

    def expire(ctx, expected, **kwargs):
        result = original(ctx, expected, **kwargs)
        ctx.deadline = 0
        return result

    with monkeypatch.context() as patch:
        patch.setattr(_inspect_gate, "_check", expire)
        timed_out = gate(path, run, answers=answers)
    assert not timed_out["ok"] and timed_out["error"]["code"] == "query_timeout"
    assert timed_out["data"] is None
    small = gate(path, run, answers=answers, max_output_bytes=1024)
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


def local_answers(tmp_path, *, count=1, capture="metadata", competing=None,
                  replay=False, later_attempt=False, include_mcp=True, bad_mcp=False, selected_mcp=False):
    """Real explicit local dispatches; no guessed attempt or result identity."""
    path = tmp_path / "local.sqlite"
    rec = Recorder(path, capture=capture, coverage=COVERAGE, comparisons=True,
                   limits={"ordinary_items": 10000, "ordinary_bytes": 64 * 1024 * 1024,
                           "completion_items": 1000, "completion_bytes": 8 * 1024 * 1024,
                           "committed_cache_items": 10000})
    run = rec.capture_scope().run_id
    locals_ = []
    try:
        for _ in range(count):
            with rec.turn("Local selection"):
                first = observe(rec, "model.request", {"messages": []}, messages=[])
                choice = observe(rec, "model.tool_call", {}, name="local", model_call_id=first.model_call_id)
                observe(rec, "model.response", {}, model_call_id=first.model_call_id)
                dispatches = []
                for index in range(2 if competing in {"local", "execute"} else 1):
                    with rec.dispatch(action="execute" if index and competing == "execute" else "local",
                                      name="local", arguments={}, decision_event_ids=[choice.event_id]):
                        dispatches.append(rec.capture_scope().dispatch_event_id)
                        observe(rec, "host.tool_result", "local text", sources=[])
                if competing == "propagated":
                    start = rec.start_operation(scope=rec.capture_scope(), client_id="test", method="tools/call",
                        name="local", arguments={}, link_basis="propagated", decision_id=choice.decision_id)
                    assert start.status == "accepted"
                    selected = rec.finish_operation(start.ticket, result="local text", status="ok", text_paths=[""])
                    assert selected.status == "accepted"
                message = {"role": "tool", "content": "local text"}
                binding = {"bindings": [{"decision_event_id": choice.event_id, "result_event_id": selected.event_id if selected_mcp else None,
                                         "pointer": "/content", "text_paths": [""]}]}
                request = observe(rec, "model.request", {"messages": [message]}, messages=[binding])
                observe(rec, "model.response", {}, model_call_id=request.model_call_id)
                if later_attempt:
                    with rec.dispatch(action="local", name="local", arguments={}, decision_event_ids=[choice.event_id]):
                        observe(rec, "host.tool_result", "later text", sources=[])
                replay_request = request
                if replay:
                    replay_request = observe(rec, "model.request", {"messages": [{"role": "user", "content": "Context"}, message]},
                                             messages=[{"bindings": []}, binding])
                    observe(rec, "model.response", {}, model_call_id=replay_request.model_call_id)
                locals_.append({"decision": choice.event_id, "first": first.event_id,
                                "dispatch": dispatches[0], "request": request.event_id,
                                "replay": replay_request.event_id, "turn": request.turn_id})
        if include_mcp:
            with rec.turn("Independent MCP selection"):
                first = observe(rec, "model.request", {"messages": []}, messages=[])
                choice = observe(rec, "model.tool_call", {}, name="remote", model_call_id=first.model_call_id)
                observe(rec, "model.response", {}, model_call_id=first.model_call_id)
                start = rec.start_operation(scope=rec.capture_scope(), client_id="test", method="tools/call", name="remote",
                    arguments={}, link_basis="propagated", decision_id=choice.decision_id)
                assert start.status == "accepted"
                end = rec.finish_operation(start.ticket, result="42", status="ok", text_paths=[""])
                assert end.status == "accepted"
                message = {"role": "tool", "content": "changed" if bad_mcp else "42"}
                request = observe(rec, "model.request", {"messages": [message]}, messages=[{"bindings": [
                    {"decision_event_id": choice.event_id, "result_event_id": end.event_id,
                     "pointer": "/content", "text_paths": [""]}]}])
                observe(rec, "model.response", {}, model_call_id=request.model_call_id)
    finally:
        closed = rec.close()
        assert closed.portable and closed.writer_stopped
    return path, run, locals_


def test_local_origin_explanation_preserves_unknown_raw_comparison(tmp_path):
    path, run, rows = local_answers(tmp_path)
    result = inspect.explain(path, run, rows[0]["turn"], model_call_id=None)
    # Select the actual second call from the public turn response.
    result = inspect.explain(path, run, rows[0]["turn"], model_call_id=result["data"]["calls"][-1]["model_call_id"])
    answer = result["data"]["detail"]["input_answers"][0]
    assert answer.get("origin") == "local", answer
    assert answer["text_equal"] is None and answer["comparison_reason"] == "result_unavailable"


def test_gate_text_keeps_verdict_when_details_exhaust_budget(tmp_path):
    from mcp_flightrecorder._inspect_text import render
    path, run, _ = record(tmp_path, count=11, bad_after=0)
    result = gate(path, run)
    text = render(result, 512)
    assert "VERDICT: FAIL" in text
    assert "Answer comparison scope: all" in text
    assert "Excluded local answer occurrences (including replay): 0" in text
    assert "CUTOFF:" in text
    assert len(text.encode()) <= 512


@pytest.mark.parametrize("capture", ["metadata", "content"])
@pytest.mark.parametrize("replay", [False, True])
def test_explicit_mcp_scope_excludes_only_validated_local_occurrences(tmp_path, capture, replay):
    path, run, rows = local_answers(tmp_path, capture=capture, replay=replay)
    before = inspect.check(path, run, EXPECT)
    default = inspect.gate(path, run, expect=EXPECT, min_activity={"answer_occurrences": 1})
    scoped = inspect.gate(path, run, expect=EXPECT, min_activity={"answer_occurrences": 1}, answers="mcp")
    assert default["data"]["status"] == "unknown"
    assert scoped["ok"] and scoped["data"]["status"] == "pass", scoped
    data = scoped["data"]
    assert data["expectations"]["answers"] == "mcp"
    assert data["excluded_count"] == 1 + replay
    assert data["mcp_answer_count"] == 1
    assert data["stats"] == default["data"]["stats"]
    assert data["stats"]["text_unknown"] == 1 + replay
    assert all(row["ids"]["dispatch_event_id"] == rows[0]["dispatch"] for row in data["excluded"])
    assert inspect.check(path, run, EXPECT) == before


@pytest.mark.parametrize("competing", ["local", "execute", "propagated"])
def test_shared_decision_attempts_cannot_be_excluded(tmp_path, competing):
    path, run, _ = local_answers(tmp_path, competing=competing)
    result = inspect.gate(path, run, expect=EXPECT, min_activity={"answer_occurrences": 1}, answers="mcp")
    assert result["data"]["status"] == "unknown", result
    assert result["data"]["excluded_count"] == 0
    assert item(result, "answer_comparisons")["unknown"] == 1


def test_replayed_local_binding_uses_original_execution_interval(tmp_path):
    path, run, _ = local_answers(tmp_path, replay=True, later_attempt=True)
    result = inspect.gate(path, run, expect=EXPECT, min_activity={"answer_occurrences": 1}, answers="mcp")
    assert result["data"]["status"] == "pass", result
    assert result["data"]["excluded_count"] == 2


@pytest.mark.parametrize("damage", ["loss", "coverage", "relation", "omitted", "unresolved", "decision", "request", "dispatch", "sources", "result-int", "result-empty", "result-missing", "result-absent"])
def test_local_exclusion_cannot_hide_independent_or_invalid_evidence(tmp_path, damage):
    path, run, rows = local_answers(tmp_path)
    local = rows[0]
    if damage == "loss":
        mutate(path, "UPDATE runs SET dropped_events=1")
    elif damage == "coverage":
        mutate(path, "UPDATE runs SET coverage_json=json_set(coverage_json,'$.producers.host',NULL)")
    elif damage == "relation":
        mutate(path, "UPDATE links SET witness_event_id='absent' WHERE relation='answers' AND target_event_id=?", (local["request"],))
    elif damage in {"omitted", "unresolved"}:
        details(path, local["request"], lambda d: d.update(
            unresolved_answers_omitted_count=1) if damage == "omitted" else d.update(
            unresolved_answers=[{"pointer": "/messages/0/content", "reason": "capture_unavailable"}]))
    elif damage == "decision":
        mutate(path, "UPDATE events SET model_call_id='absent' WHERE event_id=?", (local["decision"],))
    elif damage == "request":
        mutate(path, "UPDATE events SET boundary='host' WHERE event_id=?", (local["request"],))
    elif damage == "dispatch":
        details(path, local["dispatch"], lambda d: d.update(complete=False))
    elif damage == "sources":
        details(path, local["dispatch"], lambda d: d.update(decision_event_ids=[]))
    elif damage == "result-absent":
        mutate(path, "UPDATE links SET details_json=json_remove(details_json,'$.result_event_id') WHERE relation='answers' AND target_event_id=?", (local["request"],))
    else:
        value = {"result-int": 5, "result-empty": "", "result-missing": "missing-mcp-result"}[damage]
        mutate(path, "UPDATE links SET details_json=json_set(details_json,'$.result_event_id',?) WHERE relation='answers' AND target_event_id=?", (value, local["request"]))
    result = inspect.gate(path, run, expect=EXPECT, min_activity={"answer_occurrences": 1}, answers="mcp")
    assert result["ok"] and result["data"]["status"] != "pass", result
    if damage not in {"loss", "coverage"}:
        assert result["data"]["excluded_count"] == 0


def test_mcp_scope_missing_selected_result_never_borrows_another_result(tmp_path):
    path, run, turns = record(tmp_path, count=2)
    mutate(path, "DELETE FROM events WHERE event_id=?", (turns[0]["result"],))
    result = gate(path, run, answers="mcp")
    assert result["data"]["status"] != "pass"
    assert result["data"]["excluded_count"] == 0
    assert result["data"]["mcp_answer_count"] == 1


def test_no_mcp_answers_is_explicit_scope_unknown_and_failure_still_wins(tmp_path):
    path, run, _ = local_answers(tmp_path, include_mcp=False)
    result = inspect.gate(path, run, expect=EXPECT, min_activity={"answer_occurrences": 1}, answers="mcp")
    assert result["data"]["status"] == "unknown"
    assert item(result, "mcp_answer_scope")["witnesses"][0]["reason"] == "no_mcp_answers_in_scope"
    mutate(path, "UPDATE runs SET dropped_events=1")
    assert inspect.gate(path, run, expect=EXPECT, min_activity={"answer_occurrences": 1}, answers="mcp")["data"]["status"] == "fail"


def test_late_failure_after_many_exclusions_and_bounded_samples(tmp_path):
    path, run, _ = local_answers(tmp_path, count=201, bad_mcp=True)
    result = inspect.gate(path, run, expect=EXPECT, min_activity={"answer_occurrences": 1}, answers="mcp", max_output_bytes=8192)
    assert result["ok"] and result["data"]["status"] == "fail", result
    assert result["data"]["excluded_count"] == 201
    assert result["data"]["actual_activity"]["answer_occurrences"] == 202
    assert result["data"]["excluded_omitted_count"] == 201 - len(result["data"]["excluded"])
    assert len(result["data"]["excluded"]) <= 10
    assert item(result, "answer_comparisons")["failed"] == 1
    assert len(json.dumps(result, ensure_ascii=False, separators=(",", ":")).encode()) <= 8192


@pytest.mark.parametrize("answers", [None, "local", "MCP", [], True])
def test_invalid_answer_scope_is_a_configuration_error(tmp_path, answers):
    result = inspect.gate(tmp_path / "absent.sqlite", expect=EXPECT, min_activity={"answer_occurrences": 1}, answers=answers)
    assert not result["ok"] and result["error"]["reason"] == "invalid_answers"


def test_actual_gate_cli_scope_text_and_useful_usage(tmp_path):
    path, run, _ = local_answers(tmp_path)
    base = [sys.executable, "-I", "-B", "-m", "mcp_flightrecorder", "gate", str(path)]
    command = base + ["--run", run, "--expect", "model,host,mcp", "--min-answer-occurrences", "1", "--answers", "mcp"]
    raw = subprocess.run(command, cwd=tmp_path, capture_output=True, text=True)
    text = subprocess.run(command + ["--text"], cwd=tmp_path, capture_output=True, text=True)
    assert raw.returncode == text.returncode == 0, (raw.stdout, raw.stderr, text.stdout, text.stderr)
    assert json.loads(raw.stdout)["data"]["excluded_count"] == 1
    assert "VERDICT: PASS" in text.stdout and "Answer comparison scope: mcp" in text.stdout
    assert "not verified" in text.stdout and "No MCP result to compare (expected" in text.stdout
    missing = subprocess.run(base, cwd=tmp_path, capture_output=True, text=True)
    assert missing.returncode == 1
    error = json.loads(missing.stdout)["error"]
    assert error["reason"] == "invalid_cli" and "--min-answer-occurrences 1" in error["message"]


def test_explicit_selected_mcp_identity_survives_shared_decision_attempts(tmp_path):
    path, run, _ = local_answers(tmp_path, competing="propagated", selected_mcp=True)
    result = inspect.gate(path, run, expect=EXPECT, min_activity={"answer_occurrences": 1}, answers="mcp")
    assert result["data"]["status"] == "pass", result
    assert result["data"]["excluded_count"] == 0
    assert result["data"]["mcp_answer_count"] == 2


def test_local_occurrence_uncertainty_is_location_specific(tmp_path):
    path, run, rows = local_answers(tmp_path, replay=True)
    details(path, rows[0]["replay"], lambda d: d.update(
        unresolved_answers=[{"pointer": "/messages/0/content", "reason": "capture_unavailable"}]))
    result = inspect.gate(path, run, expect=EXPECT, min_activity={"answer_occurrences": 1}, answers="mcp")
    # The unrelated gap remains a whole-recording unknown; both independently
    # valid local comparisons are still identified at their own locations.
    assert result["data"]["status"] != "pass"
    assert result["data"]["excluded_count"] == 2


def test_scoped_text_keeps_summary_before_long_selection_path(tmp_path):
    from mcp_flightrecorder._inspect_text import render
    path, run, _ = local_answers(tmp_path)
    result = inspect.gate(path, run, expect=EXPECT, min_activity={"answer_occurrences": 1}, answers="mcp")
    text = render(result, 512, selected_path="/" + "x" * 2000)
    assert "VERDICT: PASS" in text and "scope: mcp" in text
    assert "(including replay): 1" in text and "not verified" in text
    assert "CUTOFF:" in text and len(text.encode()) <= 512
