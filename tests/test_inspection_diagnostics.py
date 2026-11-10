"""Public diagnostics distinguish declarations, comparisons and missing facts.

SQL mutations are explicitly damaged reader inputs, never capture controls.
"""
from contextlib import closing
import json
import sqlite3
import subprocess
import sys

import pytest

from mcp_flightrecorder import Recorder, inspect


COVERAGE = {"producers": {"model": "test.model", "host": "test.host", "mcp": "test.mcp"}, "model_complete": True}


def observe(recorder, kind, payload, **facts):
    result = recorder.observe(kind, scope=recorder.capture_scope(), payload=payload, facts=facts)
    assert result.status == "accepted", result
    return result


def recording(tmp_path, capture="content", prepared="Tool result: 42", submitted="Tool result: 42", comparisons=True,
              *, replay=False, uncertainty=None):
    path = tmp_path / "recording.sqlite"
    recorder = Recorder(path, capture=capture, coverage=COVERAGE, comparisons=comparisons)
    run_id = recorder.capture_scope().run_id
    try:
        with recorder.turn("Prepare a result"):
            first = observe(recorder, "model.request", {"messages": []}, messages=[])
            decision = observe(recorder, "model.tool_call", {}, name="answer", model_call_id=first.model_call_id)
            observe(recorder, "model.response", {"answer": None}, model_call_id=first.model_call_id)
            sources = []
            for _ in range(2):
                start = recorder.start_operation(scope=recorder.capture_scope(), client_id="test", method="tools/call", name="answer", arguments={})
                assert start.status == "accepted"
                end = recorder.finish_operation(start.ticket, status="ok", result="42", text_paths=[""])
                assert end.status == "accepted"
                sources.append(end)
            prep = observe(recorder, "host.tool_result", prepared,
                           sources=[{"event_id": result.event_id} for result in sources], text_paths=[""])
            message = {"role": "tool", "content": submitted}
            binding = {"bindings": [{"decision_event_id": decision.event_id, "pointer": "/content",
                                      "result_event_id": sources[0].event_id, "text_paths": [""]}]}
            if replay:
                witness = observe(recorder, "model.request", {"messages": [{"role": "user", "content": "Context"}, message]},
                                  messages=[{"bindings": []}, binding])
                observe(recorder, "model.response", {}, model_call_id=witness.model_call_id)
            messages, annotations = [message], [binding]
            if replay:
                messages.append({"role": "user", "content": "Other context"})
                annotations.append({"bindings": []})
            unresolved = {"pointer": "/content", "reason": "capture_unavailable"}
            if uncertainty == "selected":
                binding["unresolved"] = [unresolved]
            elif uncertainty == "unrelated":
                messages.append({"role": "tool", "content": "Other unavailable result"})
                annotations.append({"bindings": None, "unresolved": [unresolved]})
            request = observe(recorder, "model.request", {"messages": messages}, messages=annotations,
                              insertions=[{"source_event_id": prep.event_id, "target_pointer": "/messages/0/content", "content": submitted}])
            observe(recorder, "model.response", {"answer": "Done"}, model_call_id=request.model_call_id)
        turn_id = request.turn_id
    finally:
        closed = recorder.close()
        assert closed.portable and closed.writer_stopped
    return path, run_id, turn_id, prep.event_id, request.event_id, sources


def mutate(path, sql, params=()):
    with closing(sqlite3.connect(path)) as connection:
        connection.execute(sql, params)
        connection.commit()


def preparation(path, run):
    result = inspect.diagnose(path, run)
    assert result["ok"], result
    assert result["format"] == "mfr.diagnose/1"
    assert result["data"]["whole_run_verdict"] == "not_evaluated"
    assert len(result["data"]["preparations"]) == 1
    return result["data"]["preparations"][0]


@pytest.mark.parametrize("capture", ["content", "metadata"])
def test_preparation_matches_original_tokens_without_excusing_raw_mismatch(tmp_path, capture):
    path, run, turn, *_ = recording(tmp_path, capture)
    before = path.read_bytes()
    original = inspect.explain(path, run, turn)
    result = preparation(path, run)
    assert result["state"] == "matches" and result["prepared_text_equal"] is True
    assert result["raw_text_equal"] is False
    assert result["source_count"] == 2 and result["unavailable_sources"] == 0
    assert result["semantic_equivalence"] == "not_verified"
    assert original == inspect.explain(path, run, turn)
    assert original["data"]["flags"]["text_mismatch"] is True
    assert before == path.read_bytes()
    codes = {item["code"] for item in inspect.diagnose(path)["data"]["diagnostics"]}
    assert ("content_omitted" in codes) is (capture == "metadata")


@pytest.mark.parametrize("capture", ["content", "metadata"])
def test_declared_insertion_is_not_prepared_equality(tmp_path, capture):
    path, run, *_ = recording(tmp_path, capture, prepared="The answer is 42.", submitted="The answer is 999.")
    result = preparation(path, run)
    assert result["state"] == "differs" and result["prepared_text_equal"] is False
    assert result["raw_text_equal"] is False


@pytest.mark.parametrize("capture", ["content", "metadata"])
@pytest.mark.parametrize("uncertainty", ["selected", "unrelated"])
def test_explicit_unresolved_annotation_only_blocks_its_selected_occurrence(tmp_path, capture, uncertainty):
    path, run, *_ = recording(tmp_path, capture, uncertainty=uncertainty)
    result = preparation(path, run)
    if uncertainty == "selected":
        assert result["state"] == "unknown" and result["prepared_text_equal"] is None
        assert result["reason"] == "unresolved_answer_diagnostics"
    else:
        assert result["state"] == "matches" and result["prepared_text_equal"] is True
    assert any(item["code"] == "unknown_binding" for item in inspect.diagnose(path)["data"]["diagnostics"])


@pytest.mark.parametrize("capture", ["content", "metadata"])
@pytest.mark.parametrize("endpoint", ["request", "witness"])
@pytest.mark.parametrize("diagnostic", ["selected", "unrelated", "omitted-answers", "omitted-identities", "identity", "malformed"])
def test_replayed_preparation_checks_current_and_original_witness_uncertainty(tmp_path, capture, endpoint, diagnostic):
    path, run, turn, prep, request, *_ = recording(tmp_path, capture, replay=True)
    with closing(sqlite3.connect(path)) as connection:
        connection.row_factory = sqlite3.Row
        link = connection.execute("SELECT * FROM links WHERE relation='answers'").fetchone()
        assert link["witness_event_id"] != request
        assert link["target_pointer"] == "/messages/1/content"
        event_id = request if endpoint == "request" else link["witness_event_id"]
        index = 0 if endpoint == "request" else 1
        details = json.loads(connection.execute("SELECT details_json FROM events WHERE event_id=?", (event_id,)).fetchone()[0])
        if diagnostic in {"selected", "unrelated"}:
            pointer = "/messages/" + str(index if diagnostic == "selected" else 1 - index) + "/content"
            details["unresolved_answers"] = [{"pointer": pointer, "reason": "capture_unavailable"}]
        elif diagnostic == "omitted-answers":
            details["unresolved_answers_omitted_count"] = 1
        elif diagnostic == "omitted-identities":
            details["message_identity_unavailable_omitted_count"] = 1
        elif diagnostic == "identity":
            details["message_identity_unavailable"] = [{"message_index": index, "reason": "unresolved_bindings"}]
        else:
            details["unresolved_answers"] = [{"reason": "missing_location"}]
        connection.execute("UPDATE events SET details_json=? WHERE event_id=?", (json.dumps(details), event_id))
        connection.commit()
    result = preparation(path, run)
    if diagnostic == "unrelated":
        assert result["state"] == "matches" and result["prepared_text_equal"] is True
    else:
        assert result["state"] == "unknown" and result["prepared_text_equal"] is None
        assert result["reason"]


@pytest.mark.parametrize("capture", ["content", "metadata"])
@pytest.mark.parametrize("damage", ["missing-source", "wrong-order", "wrong-authority", "missing-prepared", "wrong-kind", "wrong-run", "bad-pointer", "missing-artifact"])
def test_invalid_endpoint_or_any_source_never_earns_positive_explanation(tmp_path, capture, damage):
    path, run, turn, prep, request, sources = recording(tmp_path, capture)
    other = sources[1].event_id  # The unselected second source must also be checked.
    if damage == "missing-source":
        mutate(path, "UPDATE links SET source_event_id='absent' WHERE relation='transformed_from' AND source_event_id=?", (other,))
    elif damage == "wrong-order":
        mutate(path, "UPDATE events SET sequence=99999 WHERE event_id=?", (other,))
    elif damage == "wrong-authority":
        mutate(path, "UPDATE events SET provenance='imported' WHERE event_id=?", (other,))
    elif damage == "missing-prepared":
        mutate(path, "DELETE FROM events WHERE event_id=?", (prep,))
    elif damage == "wrong-kind":
        mutate(path, "UPDATE events SET kind='model.response' WHERE event_id=?", (prep,))
    elif damage == "wrong-run":
        mutate(path, "UPDATE events SET run_id='other-run' WHERE event_id=?", (other,))
    elif damage == "bad-pointer":
        mutate(path, "UPDATE links SET target_pointer='/messages/9/content' WHERE relation='inserted_into'")
    else:
        mutate(path, "DELETE FROM artifacts WHERE artifact_id=(SELECT payload_ref FROM events WHERE event_id=?)", (other,))
    result = preparation(path, run)
    assert result["state"] == "unknown" and result["prepared_text_equal"] is None
    assert result["reason"]


@pytest.mark.parametrize("field,value", [("scope", "another-run"), ("projection", "unknown/1"), ("basis", "retained"), ("value", "bad")])
def test_incompatible_prepared_tokens_are_unknown(tmp_path, field, value):
    path, run, turn, prep, *_ = recording(tmp_path, "metadata")
    with closing(sqlite3.connect(path)) as connection:
        details = json.loads(connection.execute("SELECT details_json FROM events WHERE event_id=?", (prep,)).fetchone()[0])
        details["result_text_digest"][field] = value
        connection.execute("UPDATE events SET details_json=? WHERE event_id=?", (json.dumps(details), prep))
        connection.commit()
    result = preparation(path, run)
    assert result["state"] == "unknown" and result["prepared_text_equal"] is None


def test_disabled_comparisons_cannot_be_excused_by_metadata(tmp_path):
    path, run, *_ = recording(tmp_path, "metadata", comparisons=False)
    assert preparation(path, run)["state"] == "unknown"


def test_bounded_doctor_reports_cutoffs_not_success_verdict(tmp_path):
    path, run, *_ = recording(tmp_path)
    result = inspect.diagnose(path, limit=1)
    assert result["ok"]
    assert result["data"]["evaluated_events"] == 1
    assert result["data"]["events_remaining"] is True
    assert any(item["code"] == "event_limit" for item in result["cutoffs"])
    assert result["data"]["whole_run_verdict"] == "not_evaluated"
    for budget in (1024, 2048, 4096):
        small = inspect.diagnose(path, max_output_bytes=budget)
        assert len(json.dumps(small, ensure_ascii=False, separators=(",", ":")).encode()) <= budget
        if small["ok"]:
            assert small["data"]["whole_run_verdict"] == "not_evaluated"


def test_recorded_loss_unknown_binding_and_storage_remain_distinct(tmp_path):
    path, run, turn, prep, request, *_ = recording(tmp_path)
    mutate(path, "UPDATE runs SET writer_errors=1,dropped_events=2")
    with closing(sqlite3.connect(path)) as connection:
        details = json.loads(connection.execute("SELECT details_json FROM events WHERE event_id=?", (request,)).fetchone()[0])
        details["unresolved_answers"] = [{"pointer": "/messages/0/content", "reason": "binding_key_missing"}]
        connection.execute("UPDATE events SET details_json=? WHERE event_id=?", (json.dumps(details), request))
        connection.commit()
    items = inspect.diagnose(path)["data"]["diagnostics"]
    codes = {item["code"] for item in items}
    assert {"writer_failure", "capture_rejected", "unknown_binding"} <= codes
    binding = next(item for item in items if item["code"] == "unknown_binding")
    assert binding["location"]["pointer"] == "/messages/0/content"
    assert binding["location"]["recorded_reason"] == "binding_key_missing"
    assert "same explicit message key" in binding["hint"]


def test_later_missing_source_is_not_hidden_by_a_sample_of_omissions(tmp_path):
    path, run, turn, prep, request, sources = recording(tmp_path, "metadata")
    with closing(sqlite3.connect(path)) as connection:
        connection.row_factory = sqlite3.Row
        source = dict(connection.execute("SELECT * FROM links WHERE relation='transformed_from' LIMIT 1").fetchone())
        for index in range(15):
            row = {**source, "link_id": "later-source-" + str(index), "source_event_id": "absent-" + str(index)}
            connection.execute("INSERT INTO links(" + ",".join(row) + ") VALUES(" + ",".join("?" for _ in row) + ")", tuple(row.values()))
        connection.commit()
    result = preparation(path, run)
    assert result["state"] == "unknown"
    assert result["source_count"] == 17 and result["unavailable_sources"] == 15


def test_timeout_is_not_partial_success(tmp_path, monkeypatch):
    from mcp_flightrecorder import _inspect_common
    path, *_ = recording(tmp_path)
    def expire(self):
        raise _inspect_common.InspectionError("query_timeout", "deadline_expired", "The inspection deadline expired.")
    monkeypatch.setattr(_inspect_common.Context, "check_deadline", expire)
    result = inspect.diagnose(path)
    assert not result["ok"] and result["data"] is None
    assert result["format"] == "mfr.diagnose/1" and result["error"]["code"] == "query_timeout"


def test_empty_run_and_unknown_selected_turn(tmp_path):
    path = tmp_path / "empty.sqlite"
    recorder = Recorder(path, capture="metadata", coverage=COVERAGE)
    recorder.close()
    report = inspect.diagnose(path)
    assert report["ok"] and report["data"]["evaluated_events"] == 0
    assert report["data"]["whole_run_verdict"] == "not_evaluated"
    for command in ("doctor", "explain"):
        output = cli(tmp_path, command, path, "--last")
        assert output.returncode == 1 and "no_turns" in output.stdout
    report = inspect.diagnose(path, turn_id="missing")
    assert not report["ok"] and report["error"]["code"] == "unknown_turn"


def cli(tmp_path, *arguments):
    result = subprocess.run([sys.executable, "-I", "-B", "-m", "mcp_flightrecorder", *map(str, arguments)],
                            cwd=tmp_path, capture_output=True, text=True, timeout=20)
    assert result.stderr == ""
    return result


def test_installed_text_and_doctor_routes_with_latest_turn(tmp_path):
    path, run, turn, *_ = recording(tmp_path)
    for command in (("turns", path, "--last", "--text"), ("explain", path, "--last", "--text")):
        output = cli(tmp_path, *command)
        assert output.returncode == 0 and turn in output.stdout
        assert not output.stdout.startswith("{")
    result = cli(tmp_path, "doctor", path, "--last")
    assert result.returncode == 0
    assert "prepared text matches submitted text" in result.stdout
    assert "semantic equivalence not verified" in result.stdout
    assert "not a whole-run verdict" in result.stdout
    data = cli(tmp_path, "doctor", path, "--json")
    assert data.returncode == 0 and json.loads(data.stdout) == inspect.diagnose(path)
    old = cli(tmp_path, "turns", path)
    assert json.loads(old.stdout) == inspect.find_turns(path, run)


def test_last_selection_does_not_guess_across_multiple_runs(tmp_path):
    path, run, turn, *_ = recording(tmp_path)
    mutate(path, "INSERT INTO runs(run_id,started_at,environment_json,policy_json,coverage_json) VALUES('other','later','{}','{}','{}')")
    result = inspect.diagnose(path)
    assert not result["ok"] and result["error"]["reason"] == "ambiguous_run"
    for command in ("doctor", "explain", "turns"):
        refused = cli(tmp_path, command, path, "--last")
        assert refused.returncode == 1
        assert "ambiguous_run" in refused.stdout
        explicit = cli(tmp_path, command, path, "--run", run, "--last")
        assert explicit.returncode == 0


def test_latest_matching_turn_is_explicit_and_old_order_is_preserved(tmp_path):
    path, run, turn, *_ = recording(tmp_path)
    with closing(sqlite3.connect(path)) as connection:
        connection.row_factory = sqlite3.Row
        row = dict(connection.execute("SELECT * FROM turns LIMIT 1").fetchone())
        row.update(turn_id="latest-turn", started_at="9999", host_outcome="other")
        connection.execute("INSERT INTO turns(" + ",".join(row) + ") VALUES(" + ",".join("?" for _ in row) + ")", tuple(row.values()))
        connection.commit()
    assert inspect.find_turns(path, run, last=True)["data"]["items"][0]["turn"]["turn_id"] == "latest-turn"
    assert inspect.find_turns(path, run)["data"]["items"][0]["turn"]["turn_id"] == turn
    assert inspect.find_turns(path, run, last=True, filters={"host_outcome": "returned"})["data"]["items"][0]["turn"]["turn_id"] == turn
    refused = cli(tmp_path, "turns", path, "--last", "--cursor", "invalid")
    assert refused.returncode == 1 and json.loads(refused.stdout)["error"]["reason"] == "invalid_last"


def test_text_controls_are_escaped_and_cutoffs_are_visible(tmp_path):
    path, run, turn, *_ = recording(tmp_path)
    mutate(path, "UPDATE turns SET started_at=?", ("\x1b[31mLABEL" * 200,))
    output = cli(tmp_path, "turns", path, "--last", "--text", "--max-output-bytes", "2048")
    assert output.returncode == 0
    assert "\x1b" not in output.stdout and "CUTOFF" in output.stdout
    assert len(output.stdout.encode()) <= 2048
