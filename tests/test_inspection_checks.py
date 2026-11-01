"""P4: literal check facts, public capture controls and labelled malformed copies."""
from contextlib import closing
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import time

import pytest
import pytest_check as check

from mcp_flightrecorder import Recorder
from mcp_flightrecorder import inspect as reader

from .recordings import integration_recording


COVERAGE = {"producers": {"model": "checks.model", "host": "checks.host", "mcp": "checks.mcp"}, "model_complete": True}
ITEM_IDS = ["coverage.model", "coverage.host", "coverage.mcp", "capture_health", "artifact_storage",
            "request_structure", "operation_lifecycle", "links", "answer_bindings"]


@pytest.fixture
def recording(tmp_path):
    """The generated portable file is only read; every mutation owns this copy."""
    path = tmp_path / "reader-input-copy.sqlite"
    source, symbols = integration_recording()
    shutil.copyfile(source, path)
    return path, symbols


def sql_read(path, sql, parameters=()):
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as connection:
        return connection.execute(sql, parameters).fetchall()


def successful(result):
    assert result["ok"], f"The inspection must complete before semantic assertions: {result.get('error')}"
    return result["data"]


def items(result):
    return {item["id"]: item for item in successful(result)["items"]}


def counts(item):
    return tuple(item[key] for key in ("applicable", "observed", "unknown", "failed"))


def write_json_artifact(connection, artifact_id, value):
    """Labelled semantic edits keep independent retained-byte metadata truthful."""
    text = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    encoded = text.encode("utf-8")
    connection.execute("UPDATE artifacts SET body_text=?,stored_sha256=?,stored_bytes=? WHERE artifact_id=?",
                       (text, hashlib.sha256(encoded).hexdigest(), len(encoded), artifact_id))


def manifest(connection, event_id):
    row = connection.execute("SELECT a.artifact_id,a.body_text FROM events e JOIN artifacts a ON a.artifact_id=e.payload_ref WHERE e.event_id=?", (event_id,)).fetchone()
    assert row is not None and row[1] is not None, "A manifest edit requires an available accepted manifest before corruption."
    return row[0], json.loads(row[1])


def omit(connection, artifact_id):
    connection.execute("UPDATE artifacts SET body_text=NULL,stored_sha256=NULL,stored_bytes=0,omitted_reason='reader_test_omission' WHERE artifact_id=?", (artifact_id,))


def observe(recorder, kind, payload, **facts):
    receipt = recorder.observe(kind, scope=recorder.capture_scope(), payload=payload, facts=facts)
    assert receipt.status == "accepted", f"The public capture control requires an admitted {kind}: {receipt.status}/{receipt.reason}"
    return receipt


def close(recorder):
    result = recorder.close()
    assert result.writer_stopped and result.portable, "A stopped portable public recording is required before lone-file inspection."


def test_canonical_nine_items_and_complete_stats(recording):
    path, symbols = recording
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    result = reader.check(path, symbols["R"])
    data = successful(result)
    artifact_count = sql_read(path, "SELECT count(*) FROM artifacts")[0][0]
    check.equal([item["id"] for item in data["items"]], ITEM_IDS, msg="The public check must emit exactly the nine contract items in fixed order.")
    check.equal([item["status"] for item in data["items"]], ["pass"] * 9, msg="Rewrites, text differences and unknown resolution are facts; the clean fixture has no integrity failures.")
    check.equal([counts(item) for item in data["items"]], [(1, 1, 0, 0)] * 4 + [(artifact_count, artifact_count, 0, 0), (4, 4, 0, 0), (3, 3, 0, 0), (7, 7, 0, 0), (4, 4, 0, 0)], msg="Known request populations and explicit relation populations must be counted as distinct units.")
    check.equal(data["stats"], {"turns": 2, "events": 19, "observed_events": 19, "model_calls": 4,
                "model_requests": 4, "model_responses": 4, "decisions": 2, "artifacts": artifact_count,
                "operations": 3, "links": 7, "findings": 0, "request_members": 26, "answer_occurrences": 6,
                "linked_answers": 6, "unresolved_answers": 0, "text_equal": 3, "text_mismatch": 3,
                "text_unknown": 0, "forwarded_changed": 1, "forwarded_equal": 1, "forwarded_unknown": 0},
                msg="All 21 aggregates must agree with independently specified integration facts, including unequal original comparisons.")
    check.equal((data["status"], data["required_failures"], data["required_unknowns"]), ("pass", [], []), msg="A completed clean check has no required failures or unknowns.")
    check.equal(hashlib.sha256(path.read_bytes()).hexdigest(), before, msg="Checking must not write findings, scores, repairs or any database bytes.")


@pytest.mark.parametrize("capture", ["content", "metadata"])
def test_public_empty_request_is_one_conclusive_unit(tmp_path, capture):
    path = tmp_path / "empty.sqlite"
    recorder = Recorder(path, capture=capture, coverage=COVERAGE)
    run_id = recorder.capture_scope().run_id
    try:
        with recorder.turn("empty message array"):
            request = observe(recorder, "model.request", {"messages": []}, messages=[])
            observe(recorder, "model.response", {}, model_call_id=request.model_call_id)
    finally:
        close(recorder)
    result = reader.check(path, run_id)
    data, checks = successful(result), items(result)
    check.equal(counts(checks["request_structure"]), (1, 1, 0, 0), msg="An explicit complete empty array establishes one valid request, not an unavailable population.")
    check.equal(counts(checks["answer_bindings"]), (1, 1, 0, 0), msg="Explicit empty annotations conclusively establish no answer bindings for this request.")
    check.equal((data["stats"]["request_members"], data["stats"]["answer_occurrences"]), (0, 0), msg="Known empty membership and answer populations are both exactly zero.")
    check.equal(checks["capture_health"]["status"], "pass", msg="Configured metadata retention may omit content while preserving complete capture health.")
    check.equal(checks["artifact_storage"]["status"], "pass", msg="Intentional omitted bodies remain valid storage and must not be treated as retained empty strings.")


@pytest.mark.parametrize("boundary", ["model", "host", "mcp"])
@pytest.mark.parametrize("declaration,required,expected", [
    ("configured", False, ("pass", (1, 1, 0, 0))), ("configured", True, ("pass", (1, 1, 0, 0))),
    ("null", False, ("not_applicable", (0, 0, 0, 0))), ("null", True, ("fail", (1, 1, 0, 1))),
    ("absent", False, ("unknown", (1, 0, 1, 0))), ("absent", True, ("unknown", (1, 0, 1, 0))),
    ("malformed", False, ("unknown", (1, 0, 1, 0))), ("malformed", True, ("unknown", (1, 0, 1, 0))),
], ids=["configured-default", "configured-required", "null-default", "null-required", "absent-default", "absent-required", "malformed-default", "malformed-required"])
def test_coverage_declaration_matrix_without_events(tmp_path, boundary, declaration, required, expected):
    path = tmp_path / "coverage.sqlite"
    recorder = Recorder(path, capture="metadata", coverage=COVERAGE)
    run_id = recorder.capture_scope().run_id
    close(recorder)
    # Missing/malformed declarations are explicitly labelled reader-input edits.
    with sqlite3.connect(path) as connection:
        value = json.loads(connection.execute("SELECT coverage_json FROM runs").fetchone()[0])
        if declaration == "null":
            value["producers"][boundary] = None
        elif declaration == "absent":
            del value["producers"][boundary]
        elif declaration == "malformed":
            value["producers"][boundary] = {"wrong": "type"}
        connection.execute("UPDATE runs SET coverage_json=?", (json.dumps(value),))
    item = items(reader.check(path, run_id, expect=[boundary] if required else None))["coverage." + boundary]
    check.equal((item["status"], counts(item)), expected, msg="Producer declarations have the exact default/required classification even when the recording has no events.")


def test_current_manifest_omission_keeps_other_requests_linked(recording):
    path, symbols = recording
    with sqlite3.connect(path) as connection:
        artifact, _ = manifest(connection, symbols["W3"])
        omit(connection, artifact)
    result = reader.check(path, symbols["R"])
    checks, data = items(result), successful(result)
    for name in ("request_structure", "answer_bindings"):
        check.equal((checks[name]["status"], counts(checks[name])), ("unknown", (4, 3, 1, 0)), msg="One unavailable manifest makes precisely one request unit unresolved, without guessing missing answer blocks.")
    check.equal((data["stats"]["answer_occurrences"], data["stats"]["linked_answers"]), (4, 4), msg="The other retained requests still expose four explicit linked answer occurrences.")


@pytest.mark.parametrize("no_content", [False, True], ids=["metadata", "no-content"])
def test_request_binding_failure_precedes_missing_population(recording, no_content):
    path, symbols = recording
    with closing(sqlite3.connect(path)) as connection:
        links = connection.execute("SELECT link_id FROM links WHERE relation='answers' AND source_event_id=?", (symbols["D1"],)).fetchall()
        assert len(links) == 1, "The canonical copy must have one D1 answer claim before constructing the contradictory later-witness control"
        link_id = links[0][0]
        # Keep the allocated link/source/handle/digests intact; the available
        # W1 membership contradicts only this edited first-witness claim.
        connection.execute("UPDATE links SET witness_event_id=?,target_event_id=?,target_pointer='/messages/4/content' WHERE link_id=?",
                           (symbols["W3"], symbols["W3"], link_id))
        artifact_id, _ = manifest(connection, symbols["W3"])
        omit(connection, artifact_id)
        connection.commit()
    before = path.read_bytes()
    check.equal(sql_read(path, "SELECT count(*) FROM answer_occurrences WHERE request_event_id=?", (symbols["W3"],))[0][0], 0,
                msg="The omitted W3 manifest contributes no observed answer population for the mixed-precedence control.")
    value = reader.check(path, symbols["R"], max_output_bytes=1048576, no_content=no_content)
    data = successful(value)
    checks = {item["id"]: item for item in data["items"]}
    check.equal([item["id"] for item in data["items"]], ITEM_IDS,
                msg="Mixed witnessed failure and unavailable population still complete all nine check items in their assigned order.")
    check.equal((data["status"], data["required_failures"], data["required_unknowns"]),
                ("fail", ["links", "answer_bindings"], ["request_structure"]),
                msg="An explicit failed first-witness claim outranks W3's missing answer population while its structure remains unknown.")
    check.equal(counts(checks["request_structure"]), (4, 3, 1, 0),
                msg="The unavailable W3 manifest is one unknown structural request among four authoritative requests.")
    check.equal(checks["request_structure"]["witnesses"],
                [{"ids": {"request_event_id": symbols["W3"]}, "reason": "manifest_omitted"}],
                msg="The single structural unknown names the omitted W3 manifest without a fabricated empty request.")
    check.equal(counts(checks["links"]), (7, 7, 0, 1),
                msg="All seven stored links are examined and the contradictory D1 claim is exactly one failed link.")
    check.equal(checks["links"]["witnesses"],
                [{"ids": {"link_id": link_id}, "reason": "contradictory_first_witness_order"}],
                msg="Available earlier W1 evidence supplies the precise failed-link reason despite the claimed W3 omission.")
    check.equal(counts(checks["answer_bindings"]), (4, 4, 0, 3),
                msg="W3 contributes an observed failed request unit from explicit witness evidence even with no occurrence states.")
    check.equal(checks["answer_bindings"]["witnesses"],
                [{"ids": {"request_event_id": symbols["W1"]}, "reason": "invalid_binding"},
                 {"ids": {"request_event_id": symbols["W2"]}, "reason": "invalid_binding"},
                 {"ids": {"request_event_id": symbols["W3"]}, "reason": "contradictory_first_witness_order"}],
                msg="Request witnesses keep authoritative chronology and the explicit failure reason ahead of missing-population uncertainty.")
    check.equal(data["stats"], {
        "turns": 2, "events": 19, "observed_events": 19, "artifacts": 33,
        "operations": 3, "links": 7, "findings": 0, "model_calls": 4,
        "model_requests": 4, "model_responses": 4, "decisions": 2,
        "request_members": 16, "answer_occurrences": 4, "linked_answers": 2,
        "unresolved_answers": 2, "text_equal": 0, "text_mismatch": 2,
        "text_unknown": 2, "forwarded_changed": 1, "forwarded_equal": 1,
        "forwarded_unknown": 0,
    }, msg="Complete iteration preserves every literal population and comparison count without counting state sets or inventing W3 occurrences.")
    check.equal(path.read_bytes(), before,
                msg="The mixed-precedence check and its independent population read leave the constructed recording byte-for-byte unchanged.")


def test_available_contradictory_first_membership_invalidates_all_occurrences(recording):
    path, symbols = recording
    with sqlite3.connect(path) as connection:
        artifact, value = manifest(connection, symbols["W1"])
        value["message_refs"][3], value["message_refs"][4] = value["message_refs"][4], value["message_refs"][3]
        write_json_artifact(connection, artifact, value)
    result = reader.check(path, symbols["R"])
    checks, data = items(result), successful(result)
    check.equal(counts(checks["answer_bindings"]), (4, 4, 0, 3), msg="Both disproven bindings fail every affected current request once, while the empty prior request remains valid.")
    check.equal(counts(checks["links"]), (7, 7, 0, 2), msg="The two answer links contradict their available first-witness placements; other five explicit links remain conclusive.")
    check.equal((data["stats"]["linked_answers"], data["stats"]["unresolved_answers"], data["stats"]["text_unknown"]), (0, 6, 6), msg="Reader-derived invalid bindings cannot keep the SQL view's linked label or a conclusive text comparison.")
    for name in ("W1", "W2", "W3"):
        answers = successful(reader.slice(path, symbols["R"], symbols[name], max_output_bytes=1048576))["answers"]
        check.equal([row["association_state"] for row in answers], ["invalid_binding", "invalid_binding"], msg="Each affected occurrence exposes the same disproven identity across original and shifted membership.")
        check.is_true(all(row["text_equal"] is None and row["comparison_reason"] for row in answers), msg="Every invalid binding has a null comparison and a specific explanation.")


@pytest.mark.parametrize("case,expected,reason", [
    ("malformed-json", "fail", "malformed_request_manifest"),
    ("malformed-shape", "fail", "malformed_request_manifest"),
    ("missing-reference", "unknown", "referenced_artifact_missing"),
    ("unsupported", "unknown", "request_representation_unsupported"),
    ("partial-manifest", "unknown", "manifest_partial"),
], ids=["malformed-json", "malformed-shape", "missing-reference", "unsupported", "partial-manifest"])
def test_request_structure_distinguishes_unavailability_from_contradiction(recording, case, expected, reason):
    path, symbols = recording
    with sqlite3.connect(path) as connection:
        artifact, value = manifest(connection, symbols["W3"])
        if case == "malformed-json":
            text = "{"
            connection.execute("UPDATE artifacts SET body_text=?,stored_sha256=?,stored_bytes=1 WHERE artifact_id=?", (text, hashlib.sha256(text.encode()).hexdigest(), artifact))
        elif case == "malformed-shape":
            value["message_refs"] = {"wrong": "shape"}
            write_json_artifact(connection, artifact, value)
        elif case == "missing-reference":
            value["message_refs"][-1] = "reader-input-missing-artifact"
            write_json_artifact(connection, artifact, value)
        elif case == "unsupported":
            connection.execute("UPDATE artifacts SET representation='future_request/99' WHERE artifact_id=?", (artifact,))
        else:
            connection.execute("UPDATE artifacts SET truncated=1 WHERE artifact_id=?", (artifact,))
    checks = items(reader.check(path, symbols["R"]))
    check.equal(checks["request_structure"]["status"], expected, msg="Available malformed structure and unavailable membership have distinct integrity outcomes.")
    check.is_in(reason, {row["reason"] for row in checks["request_structure"]["witnesses"]}, msg="The request witness must name the specific structural cause instead of treating zero view rows as empty history.")


def test_public_direct_unexposed_request_is_unknown_membership(tmp_path):
    path = tmp_path / "direct.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    run_id = recorder.capture_scope().run_id
    try:
        with recorder.turn("direct request"):
            request = observe(recorder, "model.request", {"opaque": "request"}, message_path=None)
            observe(recorder, "model.response", {}, model_call_id=request.model_call_id)
    finally:
        close(recorder)
    result = reader.check(path, run_id)
    for name in ("request_structure", "answer_bindings"):
        check.equal(counts(items(result)[name]), (1, 0, 1, 0), msg="A public direct request keeps one unknown request unit even when the membership view exposes zero rows.")


@pytest.mark.parametrize("case", ["unresolved", "omitted-diagnostics", "malformed-diagnostics"])
def test_binding_uncertainty_counts_requests_separately_from_occurrences(recording, case):
    path, symbols = recording
    with sqlite3.connect(path) as connection:
        details = json.loads(connection.execute("SELECT details_json FROM events WHERE event_id=?", (symbols["W3"],)).fetchone()[0])
        if case == "unresolved":
            details["unresolved_answers"] = [{"pointer": "/messages/9", "reason": "unavailable_explicit_binding"}]
        elif case == "omitted-diagnostics":
            details["unresolved_answers_omitted_count"] = 7
        else:
            details["message_identity_unavailable"] = [{"message_index": 9, "reason": {"wrong": "type"}}]
        connection.execute("UPDATE events SET details_json=? WHERE event_id=?", (json.dumps(details), symbols["W3"]))
    result = reader.check(path, symbols["R"])
    data, item = successful(result), items(result)["answer_bindings"]
    check.equal(counts(item), (4, 3, 1, 0), msg="Unknown request coverage counts each affected request once, not the unknowable number of missing answer blocks.")
    check.equal((data["stats"]["answer_occurrences"], data["stats"]["unresolved_answers"]), (6, 0), msg="Observed unresolved occurrences remain distinct from unexposed diagnostic blocks.")
    sliced = reader.slice(path, symbols["R"], symbols["W3"], max_output_bytes=1048576)
    graph = successful(sliced)
    check.is_false(graph["complete"], msg="Unresolved, omitted or malformed binding diagnostics must keep ancestry explicitly incomplete.")
    expected_code = {"unresolved": "unresolved_answer_diagnostics",
                     "omitted-diagnostics": "association_diagnostics_omitted",
                     "malformed-diagnostics": "binding_context_unavailable"}[case]
    check.is_in(expected_code, {entry["code"] for entry in sliced["unknowns"]},
                msg="Slice must expose the specific public uncertainty reason for the same affected request.")


@pytest.mark.parametrize("comparisons", [True, False], ids=["supported-digests", "disabled-digests"])
def test_public_duplicate_shifted_two_block_bindings(comparisons, tmp_path):
    path = tmp_path / "two-blocks.sqlite"
    recorder = Recorder(path, capture="content", comparisons=comparisons, coverage=COVERAGE)
    run_id = recorder.capture_scope().run_id
    try:
        with recorder.turn("two explicitly bound blocks"):
            initial = observe(recorder, "model.request", {"messages": []}, messages=[])
            decisions = [observe(recorder, "model.tool_call", {"arguments": {}}, name="tool", model_call_id=initial.model_call_id) for _ in range(2)]
            observe(recorder, "model.response", {}, model_call_id=initial.model_call_id)
            results = []
            for text in ("alpha", "beta"):
                start = recorder.start_operation(scope=recorder.capture_scope(), client_id="public", method="tools/call", arguments={})
                assert start.status == "accepted", "The public binding control requires two actual MCP operation starts."
                end = recorder.finish_operation(start.ticket, status="ok", result={"text": text}, text_paths=["/text"])
                assert end.status == "accepted", "The public binding control requires actual terminal result observations."
                results.append(end)
            message = {"content": [{"text": "alpha"}, {"text": "beta"}]}
            bindings = [{"decision_event_id": decisions[index].event_id, "result_event_id": results[index].event_id,
                         "pointer": "/content/" + str(index), "text_paths": ["/text"]} for index in range(2)]
            first = observe(recorder, "model.request", {"messages": [message, message]}, messages=[{"bindings": bindings}, {"bindings": bindings}])
            observe(recorder, "model.response", {}, model_call_id=first.model_call_id)
            later = observe(recorder, "model.request", {"messages": [{"role": "user", "content": "shift"}, message, message]},
                            messages=[{"bindings": []}, {"bindings": bindings}, {"bindings": bindings}])
            observe(recorder, "model.response", {}, model_call_id=later.model_call_id)
    finally:
        close(recorder)
    result = reader.check(path, run_id)
    data = successful(result)
    check.equal(counts(items(result)["answer_bindings"]), (3, 3, 0, 0), msg="Duplicate and shifted positions keep complete explicit binding coverage even when original digests are disabled.")
    check.equal((data["stats"]["request_members"], data["stats"]["answer_occurrences"], data["stats"]["linked_answers"]), (5, 8, 8), msg="Two distinct bound blocks in each repeated message must yield all eight occurrence positions.")
    check.equal((data["stats"]["text_equal"], data["stats"]["text_unknown"]), (8, 0) if comparisons else (0, 8), msg="Comparison opt-out changes only comparison availability, not explicit association identity.")
    graph = successful(reader.slice(path, run_id, later.event_id, max_output_bytes=1048576))
    check.equal([row["target_pointer"] for row in graph["answers"]], ["/messages/1/content/0", "/messages/1/content/1", "/messages/2/content/0", "/messages/2/content/1"], msg="Current pointers preserve every ordered duplicate position and both block locations.")
    if comparisons:
        check.equal({row["binding_witness_event_id"] for row in graph["answers"]}, {first.event_id}, msg="Reusable complete binding identity retains the original witness while rebasing later membership.")


def test_public_earlier_unknown_handle_never_acquires_later_binding(tmp_path):
    path = tmp_path / "unknown-first.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    run_id = recorder.capture_scope().run_id
    message = {"content": "same exposed text"}
    try:
        with recorder.turn("earlier binding unavailable"):
            earlier = observe(recorder, "model.request", {"messages": [message]}, messages=[{"bindings": None}])
            decision = observe(recorder, "model.tool_call", {"arguments": {}}, name="tool", model_call_id=earlier.model_call_id)
            observe(recorder, "model.response", {}, model_call_id=earlier.model_call_id)
            later = observe(recorder, "model.request", {"messages": [message]}, messages=[{"bindings": [{"decision_event_id": decision.event_id, "pointer": "/content", "text_paths": [""]}]}])
            observe(recorder, "model.response", {}, model_call_id=later.model_call_id)
    finally:
        close(recorder)
    data = successful(reader.check(path, run_id))
    check.equal((data["stats"]["answer_occurrences"], data["stats"]["linked_answers"]), (1, 1), msg="Later explicit binding exposes exactly its own occurrence and never binds the earlier unknown handle by text equality.")
    graph = successful(reader.slice(path, run_id, earlier.event_id, max_output_bytes=1048576))
    check.equal(({row["event_id"] for row in graph["events"]}, graph["answers"], graph["operations"]), ({earlier.event_id}, [], []), msg="An earlier request cannot gain future decisions or operations from a later binding of matching content.")


@pytest.mark.parametrize("case,expected", [
    ("dropped", "fail"), ("writer", "fail"), ("capacity", "fail"), ("loss-and-invalid", "fail"),
    ("open", "unknown"), ("incomplete-close", "unknown"), ("partial-observation", "unknown"),
    ("missing-health", "unknown"), ("absent-counter", "unknown"), ("negative-counter", "unknown"), ("boolean-counter", "unknown"),
    ("partial", "unknown"), ("unresolved", "unknown"), ("other", "unknown"),
    ("extra-key", "pass"), ("body-omitted", "pass"), ("finalize-busy", "pass"), ("finalize-timeout", "pass"),
])
def test_capture_health_loss_uncertainty_and_nonloss_reasons(recording, case, expected):
    path, symbols = recording
    with sqlite3.connect(path) as connection:
        policy = json.loads(connection.execute("SELECT policy_json FROM runs").fetchone()[0])
        reasons = policy["health"]["reason_counts"]
        if case in {"dropped", "writer"}:
            column = "dropped_events" if case == "dropped" else "writer_errors"
            connection.execute("UPDATE runs SET " + column + "=1")
        elif case == "open":
            connection.execute("UPDATE runs SET ended_at=NULL,close_state='open'")
        elif case == "incomplete-close":
            connection.execute("UPDATE runs SET close_state='incomplete'")
        elif case == "partial-observation":
            detail = json.loads(connection.execute("SELECT details_json FROM events WHERE event_id=?", (symbols["F3"],)).fetchone()[0])
            detail["complete"] = False
            connection.execute("UPDATE events SET details_json=? WHERE event_id=?", (json.dumps(detail), symbols["F3"]))
        elif case == "missing-health":
            del policy["health"]
        elif case == "absent-counter":
            del reasons["capacity"]
        elif case in {"negative-counter", "boolean-counter"}:
            reasons["capacity"] = -1 if case == "negative-counter" else True
        elif case == "loss-and-invalid":
            reasons["capacity"], reasons["partial"] = 1, False
        elif case == "extra-key":
            reasons["ignored_future_key"] = 900
        else:
            reasons[{"body-omitted": "body_omitted", "finalize-busy": "finalize_busy", "finalize-timeout": "finalize_timeout"}.get(case, case)] = 1
        connection.execute("UPDATE runs SET policy_json=?", (json.dumps(policy),))
    health = items(reader.check(path, symbols["R"]))["capture_health"]
    check.equal(health["status"], expected, msg="Known capture losses outrank missing diagnostics; partial evidence stays unknown and retention/portability counters alone do not erase observation integrity.")
    check.equal(counts(health), {"pass": (1, 1, 0, 0), "unknown": (1, 0, 1, 0), "fail": (1, 1, 0, 1)}[expected], msg="Capture health is exactly one run unit with failure taking precedence over uncertainty.")


def test_public_capacity_loss_with_zero_dropped_envelopes_fails_health(tmp_path):
    path = tmp_path / "capacity.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE, limits={"ordinary_bytes": 16384})
    run_id = recorder.capture_scope().run_id
    try:
        with recorder.turn("capacity"):
            observe(recorder, "host.tool_result", {"large": "x" * 50000}, sources=[])
    finally:
        close(recorder)
    result = reader.check(path, run_id)
    data = successful(result)
    check.equal(data["run"]["dropped_events"], 0, msg="Capacity omission may preserve every event envelope, so dropped-envelope count is not the complete loss signal.")
    check.greater(data["run"]["health"]["reason_counts"]["capacity"], 0, msg="This control must actually exercise persisted capacity loss rather than an unrelated incomplete run.")
    check.equal(items(result)["capture_health"]["status"], "fail", msg="Known capacity-driven evidence loss fails health even with zero dropped envelopes.")


def test_public_partial_stream_capture_remains_unknown(tmp_path):
    path = tmp_path / "partial.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    run_id = recorder.capture_scope().run_id
    try:
        with recorder.turn("partial observed stream"):
            request = observe(recorder, "model.request", {"messages": []}, messages=[])
            observe(recorder, "model.response", {"chunks": ["partial"]}, model_call_id=request.model_call_id, state="partial", complete=False)
    finally:
        close(recorder)
    check.equal(items(reader.check(path, run_id))["capture_health"]["status"], "unknown", msg="A public partial response without proven capture loss remains uncertainty, not a conclusive failure or clean pass.")


@pytest.mark.parametrize("status", ["ok", "tool_error", "protocol_error", "transport_error", "local_error", "timeout", "cancelled"])
def test_public_lifecycle_terminal_outcomes_are_valid_observations(tmp_path, status):
    path = tmp_path / "outcome.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    run_id = recorder.capture_scope().run_id
    try:
        with recorder.turn("real terminal status"):
            start = recorder.start_operation(scope=recorder.capture_scope(), client_id="public", method="tools/call", arguments={})
            assert start.status == "accepted", "A terminal-outcome control requires a real admitted start."
            end = recorder.finish_operation(start.ticket, status=status, result={"status": status}, error=None if status == "ok" else {"origin": "public", "type": status, "code": -1})
            assert end.status == "accepted", "The public recorder must admit each supported terminal status."
    finally:
        close(recorder)
    lifecycle = items(reader.check(path, run_id))["operation_lifecycle"]
    check.equal((lifecycle["status"], counts(lifecycle)), ("pass", (1, 1, 0, 0)), msg="Legitimate error, timeout and cancellation are complete witnessed outcomes, including unattributed MCP work.")


@pytest.mark.parametrize("state", ["running", "incomplete"])
def test_public_missing_terminal_is_unknown_not_failure(tmp_path, state):
    path = tmp_path / "unfinished.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    run_id = recorder.capture_scope().run_id
    try:
        start = recorder.start_operation(scope=recorder.capture_scope(), client_id="public", method="tools/call", arguments={})
        assert start.status == "accepted", "The unfinished-operation control requires its admitted start."
        if state == "incomplete":
            close(recorder)
        else:
            deadline = time.monotonic() + 5
            while recorder.health().ordinary.used_slots and time.monotonic() < deadline:
                time.sleep(0.005)
            assert recorder.health().ordinary.used_slots == 0, "The open operation must be committed before read-only inspection."
        check.equal(sql_read(path, "SELECT status FROM operations")[0][0], state, msg="The control must exercise the requested stored lifecycle branch.")
        item = items(reader.check(path, run_id))["operation_lifecycle"]
        check.equal((item["status"], counts(item)), ("unknown", (1, 0, 1, 0)), msg="An absent terminal cannot be fabricated as success or rewritten into an integrity contradiction.")
    finally:
        close(recorder)


@pytest.mark.parametrize("case", ["terminal-kind", "terminal-identity", "temporal-order", "terminal-state"])
def test_malformed_lifecycle_witnesses_fail(recording, case):
    path, symbols = recording
    with sqlite3.connect(path) as connection:
        if case == "terminal-kind":
            connection.execute("UPDATE events SET kind='host.tool_result' WHERE event_id=?", (symbols["E1"],))
        elif case == "terminal-identity":
            connection.execute("UPDATE events SET operation_id='wrong-operation' WHERE event_id=?", (symbols["E1"],))
        elif case == "temporal-order":
            connection.execute("UPDATE events SET observed_mono_ms=-1 WHERE event_id=?", (symbols["E1"],))
        else:
            connection.execute("UPDATE events SET state='cancelled' WHERE event_id=?", (symbols["E1"],))
    item = items(reader.check(path, symbols["R"]))["operation_lifecycle"]
    check.equal((item["status"], counts(item)), ("fail", (3, 3, 0, 1)), msg="A known wrong terminal identity/kind/state or contradictory temporal order is one failed operation, not a valid error outcome.")


@pytest.mark.parametrize("case", ["byte-length", "digest"])
def test_retained_storage_contradictions_fail(recording, case):
    path, symbols = recording
    with sqlite3.connect(path) as connection:
        artifact, _ = manifest(connection, symbols["W3"])
        if case == "byte-length":
            connection.execute("UPDATE artifacts SET stored_bytes=stored_bytes+1 WHERE artifact_id=?", (artifact,))
        else:
            connection.execute("UPDATE artifacts SET stored_sha256=? WHERE artifact_id=?", ("0" * 64, artifact))
    item = items(reader.check(path, symbols["R"]))["artifact_storage"]
    check.equal((item["status"], item["failed"], item["unknown"]), ("fail", 1, 0), msg="Available retained length/hash contradictions are conclusive storage failures.")


@pytest.mark.parametrize("case", ["pointer", "kind", "column", "future-witness", "source-order"])
def test_disproven_first_witness_never_keeps_linked_comparison(recording, case):
    path, symbols = recording
    with sqlite3.connect(path) as connection:
        if case == "pointer":
            connection.execute("UPDATE links SET target_pointer='/messages/0/content' WHERE relation='answers' AND source_event_id=?", (symbols["D1"],))
        elif case == "kind":
            connection.execute("UPDATE events SET kind='model.response' WHERE event_id=?", (symbols["W1"],))
        elif case in {"column", "future-witness"}:
            connection.execute("UPDATE links SET witness_event_id=? WHERE relation='answers' AND source_event_id=?", (symbols["R0"] if case == "column" else symbols["W3"], symbols["D1"]))
        else:
            first = connection.execute("SELECT sequence FROM events WHERE event_id=?", (symbols["D1"],)).fetchone()[0]
            witness = connection.execute("SELECT sequence FROM events WHERE event_id=?", (symbols["W1"],)).fetchone()[0]
            connection.execute("UPDATE events SET sequence=-100 WHERE event_id=?", (symbols["D1"],))
            connection.execute("UPDATE events SET sequence=? WHERE event_id=?", (first, symbols["W1"]))
            connection.execute("UPDATE events SET sequence=? WHERE event_id=?", (witness, symbols["D1"]))
    result = reader.check(path, symbols["R"])
    check.greater(items(result)["links"]["failed"], 0, msg="A known first-witness contradiction must fail its relation unit.")
    check.greater(items(result)["answer_bindings"]["failed"], 0, msg="Current requests that repeat a disproven binding must fail their association units.")
    answers = successful(reader.slice(path, symbols["R"], symbols["W3"], max_output_bytes=1048576))["answers"]
    affected = [row for row in answers if row["source_event_id"] == symbols["D1"]]
    assert affected, "The later explicit occurrence must remain visible while its original witness is disproven."
    check.is_true(all(row["association_state"] == "invalid_binding" and row["text_equal"] is None and row["comparison_reason"] for row in affected), msg="Reader-derived invalid_binding overrides the unchanged SQL view and explains null comparisons.")


def test_refused_decision_with_no_operation_is_not_an_integrity_failure(tmp_path):
    path = tmp_path / "refused.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    run_id = recorder.capture_scope().run_id
    try:
        with recorder.turn("refused tool"):
            request = observe(recorder, "model.request", {"messages": []}, messages=[])
            decision = observe(recorder, "model.tool_call", {"arguments": {"x": 1}}, name="tool", model_call_id=request.model_call_id)
            observe(recorder, "model.response", {}, model_call_id=request.model_call_id)
            with recorder.dispatch(action="refuse", name="tool", arguments={"x": 1}, decision_event_ids=[decision.event_id]):
                pass
    finally:
        close(recorder)
    data = successful(reader.check(path, run_id))
    check.equal((data["status"], data["stats"]["operations"], data["stats"]["forwarded_equal"]), ("pass", 0, 1), msg="An explicitly refused selected decision has no required tool operation and remains a valid dispatch fact.")


def test_witness_and_output_caps_do_not_truncate_completed_aggregates(recording):
    path, symbols = recording
    with sqlite3.connect(path) as connection:
        ids = [row[0] for row in connection.execute("SELECT artifact_id FROM artifacts WHERE body_text IS NOT NULL ORDER BY artifact_id LIMIT 12")]
        assert len(ids) == 12, "The aggregate limit control needs more than ten contradictory artifact units."
        connection.executemany("UPDATE artifacts SET stored_bytes=stored_bytes+1 WHERE artifact_id=?", [(identity,) for identity in ids])
    full = reader.check(path, symbols["R"])
    item = items(full)["artifact_storage"]
    check.equal((item["failed"], len(item["witnesses"])), (12, 10), msg="Ten visible witnesses cannot cap the completed count of twelve failed units.")
    check.is_in("witness_limit", {row["code"] for row in full["cutoffs"]}, msg="Omitted witnesses must be explicit diagnostics.")
    budget = len(json.dumps(full, ensure_ascii=False, separators=(",", ":")).encode()) - 200
    limited = reader.check(path, symbols["R"], max_output_bytes=budget)
    check.equal(successful(limited)["stats"], successful(full)["stats"], msg="Whole-envelope byte trimming must preserve every completed aggregate.")
    check.equal(counts(items(limited)["artifact_storage"]), counts(item), msg="Output caps remove optional witnesses rather than changing classification counts.")
    check.less(len(items(limited)["artifact_storage"]["witnesses"]), 10, msg="This control must actually exercise witness removal under the byte cap.")
    check.less_equal(len(json.dumps(limited, ensure_ascii=False, separators=(",", ":")).encode()), budget, msg="The entire compact UTF-8 envelope fits the requested byte budget.")


@pytest.mark.parametrize("population", [10, 11], ids=["exact-ten-witnesses", "one-witness-over"])
def test_exact_witness_cap_and_one_more_failure(recording, population):
    path, symbols = recording
    with sqlite3.connect(path) as connection:
        connection.execute("UPDATE artifacts SET stored_bytes=stored_bytes+1 WHERE artifact_id IN (SELECT artifact_id FROM artifacts WHERE body_text IS NOT NULL ORDER BY artifact_id LIMIT ?)", (population,))
    value = reader.check(path, symbols["R"])
    item = items(value)["artifact_storage"]
    check.equal((item["failed"], len(item["witnesses"])), (population, 10), msg="The exact witness boundary retains ten rows while one further failed unit changes only aggregate count and cutoff accounting.")
    check.equal(any(entry["code"] == "witness_limit" for entry in value["cutoffs"]), population == 11, msg="Exactly ten witnesses require no omission diagnostic; the eleventh does.")


def test_check_exact_encoded_cap_and_one_byte_over(recording):
    path, symbols = recording
    # Labelled malformed metadata checks escaping/UTF-8 accounting. The storage
    # contradiction supplies one optional witness that a smaller envelope trims.
    text = 'é中"\\\n\t' * 15
    with sqlite3.connect(path) as connection:
        connection.execute("UPDATE runs SET started_at=?", (text,))
        connection.execute("UPDATE artifacts SET stored_bytes=stored_bytes+1 WHERE artifact_id=(SELECT artifact_id FROM artifacts WHERE body_text IS NOT NULL LIMIT 1)")
    full = reader.check(path, symbols["R"], max_output_bytes=1048576)
    encoded = json.dumps(full, ensure_ascii=False, separators=(",", ":")).encode()
    exact = reader.check(path, symbols["R"], max_output_bytes=len(encoded))
    smaller = reader.check(path, symbols["R"], max_output_bytes=len(encoded) - 1)
    check.equal(exact, full, msg="A check envelope exactly at its compact UTF-8 byte cap must retain every whole field unchanged.")
    check.equal(successful(smaller)["stats"], successful(full)["stats"], msg="One byte over the available budget cannot change any completed check aggregate.")
    check.equal(len(successful(smaller)["items"]), 9, msg="All nine required check items survive optional witness trimming.")
    check.is_in("byte_limit", {row["code"] for row in smaller["cutoffs"]}, msg="Quotes, backslashes, control strings and multibyte characters are counted in encoded JSON, with explicit one-byte-over cutoff.")
    check.less_equal(len(json.dumps(smaller, ensure_ascii=False, separators=(",", ":")).encode()), len(encoded) - 1, msg="The trimmed complete check envelope obeys the exact reduced byte cap.")


@pytest.mark.parametrize("case,exit_code", [("pass", 0), ("fail", 2), ("unknown", 4), ("fail-and-unknown", 2)])
def test_completed_check_cli_exits_and_failure_precedence(recording, case, exit_code):
    path, symbols = recording
    with sqlite3.connect(path) as connection:
        if "fail" in case:
            connection.execute("UPDATE artifacts SET stored_bytes=stored_bytes+1 WHERE artifact_id=(SELECT artifact_id FROM artifacts WHERE body_text IS NOT NULL LIMIT 1)")
        if "unknown" in case:
            connection.execute("UPDATE runs SET close_state='open',ended_at=NULL")
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    process = subprocess.run([sys.executable, "-I", "-B", "-m", "mcp_flightrecorder", "check", str(path), "--run", symbols["R"]], cwd=path.parent, capture_output=True, text=True, timeout=20)
    assert process.stdout, f"Completed checks must emit one JSON result, including failing checks: {process.stderr}"
    value = json.loads(process.stdout)
    check.is_true(value["ok"], msg="Check result failure/unknown statuses remain successful reads with complete counts.")
    check.equal(process.returncode, exit_code, msg="Completed required failure exits 2, required unknown exits 4, and failure outranks simultaneous uncertainty.")
    check.equal(hashlib.sha256(path.read_bytes()).hexdigest(), before, msg="All check status paths remain read-only and never materialize findings or scores.")


@pytest.mark.parametrize("failure,expected", [("deadline", "query_timeout"), ("database", "recording_unreadable")])
def test_aggregate_failure_never_returns_partially_successful_items(recording, monkeypatch, failure, expected):
    from mcp_flightrecorder import _inspect_checks, _inspect_common
    path, symbols = recording
    original = _inspect_checks._storage_state
    visited = []

    def fail_after_one_complete_unit(artifact):
        visited.append(artifact["artifact_id"])
        if len(visited) == 2:
            if failure == "deadline":
                raise _inspect_common.InspectionError("query_timeout", "deadline_expired", "The inspection deadline expired.")
            raise sqlite3.OperationalError("test-owned aggregate read failure")
        return original(artifact)

    monkeypatch.setattr(_inspect_checks, "_storage_state", fail_after_one_complete_unit)
    result = reader.check(path, symbols["R"])
    check.equal(len(visited), 2, msg="The fault must occur during a real aggregate after one completed unit, not merely during opening.")
    check.equal((result["ok"], result["data"], result["error"]["code"]), (False, None, expected), msg="Deadline and database failure discard partial aggregates instead of reporting partially successful PASS items.")


# Review R1 literal oracles. Each row is an actually retained occurrence, not a
# population inferred from a failed link: request, decision, message index,
# current pointer, claimed first witness, handle owner, association, comparison.
BINDING_CONTRADICTIONS = {
    "overlapping-bindings": {
        "review_id": "R1.1", "link_counts": (7, 7, 0, 2), "request_counts": (4, 4, 0, 3),
        "failed_links": ("D1", "D2"), "failed_requests": ("W1", "W2", "W3"),
        "stats": (6, 0, 6, 0, 0, 6), "turn_inputs": (2, 4), "text_mismatch": False,
        "rows": [
            ("W1", "D1", 3, "/messages/3/content", "W1", "D1", "invalid_binding", None),
            ("W1", "D2", 3, "/messages/3/content", "W1", "D1", "invalid_binding", None),
            ("W2", "D1", 4, "/messages/4/content", "W1", "D1", "invalid_binding", None),
            ("W2", "D2", 4, "/messages/4/content", "W1", "D1", "invalid_binding", None),
            ("W3", "D1", 4, "/messages/4/content", "W1", "D1", "invalid_binding", None),
            ("W3", "D2", 4, "/messages/4/content", "W1", "D1", "invalid_binding", None),
        ],
    },
    "coherent-later-witness": {
        "review_id": "R1.2", "link_counts": (7, 7, 0, 1), "request_counts": (4, 4, 0, 3),
        "failed_links": ("D1",), "failed_requests": ("W1", "W2", "W3"),
        "stats": (6, 3, 3, 0, 3, 3), "turn_inputs": (2, 4), "text_mismatch": True,
        "rows": [
            ("W1", "D1", 3, "/messages/3/content", "W3", "D1", "invalid_binding", None),
            ("W1", "D2", 4, "/messages/4/content", "W1", "D2", "linked", False),
            ("W2", "D1", 4, "/messages/4/content", "W3", "D1", "invalid_binding", None),
            ("W2", "D2", 5, "/messages/5/content", "W1", "D2", "linked", False),
            ("W3", "D1", 4, "/messages/4/content", "W3", "D1", "invalid_binding", None),
            ("W3", "D2", 5, "/messages/5/content", "W1", "D2", "linked", False),
        ],
    },
    "disappeared-witness-membership": {
        "review_id": "R1.3a", "link_counts": (7, 7, 0, 1), "request_counts": (4, 4, 0, 3),
        "failed_links": ("D1",), "failed_requests": ("W1", "W2", "W3"),
        "stats": (5, 3, 2, 0, 3, 2), "turn_inputs": (1, 4), "text_mismatch": True,
        "rows": [
            ("W1", "D2", 4, "/messages/4/content", "W1", "D2", "linked", False),
            ("W2", "D1", 4, "/messages/4/content", "W1", "D1", "invalid_binding", None),
            ("W2", "D2", 5, "/messages/5/content", "W1", "D2", "linked", False),
            ("W3", "D1", 4, "/messages/4/content", "W1", "D1", "invalid_binding", None),
            ("W3", "D2", 5, "/messages/5/content", "W1", "D2", "linked", False),
        ],
    },
    "absent-binding-handle": {
        "review_id": "R1.3b", "link_counts": (7, 7, 0, 1), "request_counts": (4, 4, 0, 1),
        "failed_links": ("D1",), "failed_requests": ("W1",),
        "stats": (3, 3, 0, 0, 3, 0), "turn_inputs": (1, 2), "text_mismatch": True,
        "rows": [
            ("W1", "D2", 4, "/messages/4/content", "W1", "D2", "linked", False),
            ("W2", "D2", 5, "/messages/5/content", "W1", "D2", "linked", False),
            ("W3", "D2", 5, "/messages/5/content", "W1", "D2", "linked", False),
        ],
    },
}


@pytest.fixture(params=tuple(BINDING_CONTRADICTIONS), ids=[case["review_id"] for case in BINDING_CONTRADICTIONS.values()])
def contradictory_bindings(request, recording):
    """One fresh labelled copy per case; never rewrite IDs or original tokens."""
    original, symbols = recording
    case = request.param
    path = original.with_name(case + ".sqlite")
    original.rename(path)
    with closing(sqlite3.connect(path)) as connection:
        rows = connection.execute(
            "SELECT source_event_id,link_id,details_json FROM links WHERE run_id=? AND relation='answers'",
            (symbols["R"],),
        ).fetchall()
        assert len(rows) == 2, "Each malformed control starts with both canonical stored claims before one deliberate edit."
        claims = {name: {"link_id": link_id, "details": json.loads(raw)}
                  for source, link_id, raw in rows for name in ("D1", "D2") if source == symbols[name]}
        assert set(claims) == {"D1", "D2"}, "The control requires distinct original decision identities rather than inferred matching payloads."
        if case == "overlapping-bindings":
            details = dict(claims["D2"]["details"])
            details["message_artifact_id"] = claims["D1"]["details"]["message_artifact_id"]
            connection.execute("UPDATE links SET target_pointer='/messages/3/content',details_json=? WHERE link_id=?",
                               (json.dumps(details), claims["D2"]["link_id"]))
        elif case == "coherent-later-witness":
            connection.execute("UPDATE links SET witness_event_id=?,target_event_id=?,target_pointer='/messages/4/content' WHERE link_id=?",
                               (symbols["W3"], symbols["W3"], claims["D1"]["link_id"]))
        elif case == "disappeared-witness-membership":
            artifact_id, value = manifest(connection, symbols["W1"])
            assert len(value["message_refs"]) == 6, "Replacing one known membership must retain W1's six ordered positions."
            value["message_refs"][3] = value["message_refs"][0]
            write_json_artifact(connection, artifact_id, value)
        else:
            details = dict(claims["D1"]["details"])
            details["message_artifact_id"] = "absent-reader-binding-handle"
            connection.execute("UPDATE links SET details_json=? WHERE link_id=?",
                               (json.dumps(details), claims["D1"]["link_id"]))
        connection.commit()
    return path, symbols, case, claims


def check_binding_occurrences(actual, expected, symbols, claims, context):
    """Compare public rows to literal source facts, including missing-row counts."""
    fields = ("request_event_id", "source_event_id", "message_index", "target_pointer",
              "binding_witness_event_id", "binding_request_event_id", "message_artifact_id",
              "link_id", "result_event_id", "association_state", "text_equal")
    wanted = [
        (symbols[req], symbols[decision], index, pointer, symbols[witness], symbols[witness],
         claims[handle]["details"]["message_artifact_id"], claims[decision]["link_id"],
         symbols["E1" if decision == "D1" else "E2"], state, equal)
        for req, decision, index, pointer, witness, handle, state, equal in expected
    ]
    observed = [tuple(row[field] for field in fields) for row in actual]
    check.equal(len(actual), len(expected), msg=f"{context}: preserve the exact observed rows without dropping conflicting claims or fabricating a missing join.")
    check.equal(sorted(observed), sorted(wanted), msg=f"{context}: current positions, original claim IDs, explicit result IDs and claimed first witnesses follow the literal repair oracle.")
    for row in actual:
        if row["association_state"] == "invalid_binding":
            check.is_none(row["text_equal"], msg=f"{context}: a disproven binding cannot retain a conclusive original-text comparison.")
            check.is_true(isinstance(row["comparison_reason"], str) and bool(row["comparison_reason"]), msg=f"{context}: every invalid occurrence names the specific known contradiction.")
            check.is_not_in(row["comparison_reason"], ("missing_decision", "missing_or_unselected_witness"), msg=f"{context}: available contradictory evidence must not be relabelled as an unavailable endpoint or witness.")
        else:
            check.is_none(row["comparison_reason"], msg=f"{context}: the unaffected complete D2 binding keeps its conclusive comparison without invented uncertainty.")


@pytest.mark.parametrize("no_content", [False, True], ids=["metadata", "no-content"])
def test_known_binding_contradictions_exact_units_and_witnesses(contradictory_bindings, no_content):
    path, symbols, case, claims = contradictory_bindings
    expected = BINDING_CONTRADICTIONS[case]
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    value = reader.check(path, symbols["R"], max_output_bytes=1048576, no_content=no_content)
    data, checks = successful(value), items(value)
    artifact_count = sql_read(path, "SELECT count(*) FROM artifacts")[0][0]
    check.equal([row["id"] for row in data["items"]], ITEM_IDS, msg="Known binding contradictions still complete every one of the nine check items.")
    check.equal((data["status"], data["required_failures"], data["required_unknowns"]), ("fail", ["links", "answer_bindings"], []), msg="These four fully available contradictions are failed evidence units, not operational errors or missing capture.")
    check.equal(counts(checks["links"]), expected["link_counts"], msg="R1 counts each stored conflicting link once and never selects a winning claim.")
    check.equal(counts(checks["answer_bindings"]), expected["request_counts"], msg="Request failure includes its explicit invalid witness even when that request's occurrence join disappears.")
    check.equal((checks["request_structure"]["status"], counts(checks["request_structure"])), ("pass", (4, 4, 0, 0)), msg="All four mutations retain complete structurally valid authoritative requests.")
    check.equal((checks["artifact_storage"]["status"], counts(checks["artifact_storage"])), ("pass", (artifact_count, artifact_count, 0, 0)), msg="The manifest edit updates only retained SHA/length; no repair case is a storage corruption test.")
    for item in data["items"]:
        if item["id"] not in {"links", "answer_bindings"}:
            check.equal(item["status"], "pass", msg="Unchanged coverage, capture, storage and operation facts do not fail merely because answer attribution contradicts evidence.")
    occurrences, linked, unresolved, equal, mismatch, unknown = expected["stats"]
    check.equal(data["stats"], {"turns": 2, "events": 19, "observed_events": 19, "model_calls": 4,
                "model_requests": 4, "model_responses": 4, "decisions": 2, "artifacts": artifact_count,
                "operations": 3, "links": 7, "findings": 0, "request_members": 26,
                "answer_occurrences": occurrences, "linked_answers": linked, "unresolved_answers": unresolved,
                "text_equal": equal, "text_mismatch": mismatch, "text_unknown": unknown,
                "forwarded_changed": 1, "forwarded_equal": 1, "forwarded_unknown": 0},
                msg="Literal 6/6/5/3 occurrence populations partition into valid/mismatched/unknown comparisons without changing unrelated aggregate facts.")
    request_witnesses = [row["ids"].get("request_event_id") for row in checks["answer_bindings"]["witnesses"]]
    check.equal(sorted(request_witnesses), sorted(symbols[name] for name in expected["failed_requests"]), msg="Every exact failed W1/W2/W3 request appears once, including an explicit W1 claim whose occurrence was lost.")
    link_witnesses = [row["ids"].get("link_id") for row in checks["links"]["witnesses"]]
    check.equal(sorted(link_witnesses), sorted(claims[name]["link_id"] for name in expected["failed_links"]), msg="Failed-link witnesses retain all original offending claim IDs and no unrelated relation.")
    check.is_true(all(row["reason"] for name in ("links", "answer_bindings") for row in checks[name]["witnesses"]), msg="Each failed link/request unit carries an explanation instead of a bare count.")
    check.equal(hashlib.sha256(path.read_bytes()).hexdigest(), before, msg="Both metadata policies must classify contradictions without rewriting database bytes or adding findings.")


@pytest.mark.parametrize("no_content", [False, True], ids=["metadata", "no-content"])
def test_known_binding_contradictions_explain_arrays_and_turn_flags(contradictory_bindings, no_content):
    path, symbols, case, claims = contradictory_bindings
    expected = BINDING_CONTRADICTIONS[case]
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    produced = successful(reader.explain(path, symbols["R"], symbols["T1"], symbols["M0"], max_output_bytes=1048576, no_content=no_content))
    check_binding_occurrences(produced["detail"]["produced_answers"], expected["rows"], symbols, claims, "M0 produced answers")
    check.equal((produced["detail"]["input_answers"], produced["detail"]["totals"]["input_answers"]), ([], 0), msg="The producing call's empty prior request does not acquire later submitted answers.")
    check.equal(produced["detail"]["totals"]["produced_answers"], expected["stats"][0], msg="Produced-answer totals retain exactly the surviving observed rows, including later turns.")
    for request_name, call, turn, member_count in (("W1", "M1", "T1", 6), ("W2", "M2", "T2", 8), ("W3", "M3", "T2", 10)):
        expected_rows = [row for row in expected["rows"] if row[0] == request_name]
        explained = reader.explain(path, symbols["R"], symbols[turn], symbols[call], max_output_bytes=1048576, no_content=no_content)
        detail = successful(explained)["detail"]
        check_binding_occurrences(detail["input_answers"], expected_rows, symbols, claims, request_name + " input answers")
        check.equal((detail["totals"]["input_answers"], detail["totals"]["members"]), (len(expected_rows), member_count), msg="Dropping a disproven occurrence join cannot shorten the complete manifest membership population.")
        check.equal((detail["produced_answers"], detail["totals"]["produced_answers"]), ([], 0), msg="Submitting existing answers does not turn later calls into new decision producers.")
        if request_name == "W1" and case in {"disappeared-witness-membership", "absent-binding-handle"}:
            check.is_true(any(row["ids"].get("request_event_id") == symbols["W1"] and row["ids"].get("link_id") == claims["D1"]["link_id"] and row["code"] for row in explained["unknowns"]), msg="W1 explanation must name its invalid explicit D1 claim even when no D1 input occurrence joins that request.")
    discovered = reader.find_turns(path, symbols["R"], max_output_bytes=1048576, no_content=no_content)
    turns = successful(discovered)["items"]
    check.equal([row["turn"]["turn_id"] for row in turns], [symbols["T1"], symbols["T2"]], msg="Known contradictory answers do not hide either recorded turn.")
    if case in {"disappeared-witness-membership", "absent-binding-handle"}:
        check.is_true(any(row["ids"].get("request_event_id") == symbols["W1"] and row["ids"].get("link_id") == claims["D1"]["link_id"] and row["code"] for row in discovered["unknowns"]), msg="Discovery retains the explicit failed W1/link diagnostic independently of its missing occurrence population.")
    for index, row in enumerate(turns):
        turn_name = "T1" if index == 0 else "T2"
        expected_counts = ({"events": 15, "observed_events": 15, "model_calls": 2, "model_requests": 2,
                            "model_responses": 2, "decisions": 2, "operations": 3, "request_members": 8,
                            "input_answers": expected["turn_inputs"][0], "produced_answers": expected["stats"][0]}
                           if index == 0 else
                           {"events": 4, "observed_events": 4, "model_calls": 2, "model_requests": 2,
                            "model_responses": 2, "decisions": 0, "operations": 0, "request_members": 18,
                            "input_answers": expected["turn_inputs"][1], "produced_answers": 0})
        expected_flags = {"tool_error": False, "text_mismatch": expected["text_mismatch"],
                          "unattributed_operation": False, "forwarded_change": index == 0}
        check.equal(row["counts"], expected_counts, msg="Discovery counts actual current/produced occurrences, never missing rows inferred from invalid links.")
        check.equal(row["flags"], expected_flags, msg="Text-mismatch flags require a valid false comparison; conflicting claims cannot contribute one.")
        overview = successful(reader.explain(path, symbols["R"], symbols[turn_name], max_output_bytes=1048576, no_content=no_content))
        check.equal((overview["counts"], overview["flags"]), (expected_counts, expected_flags), msg="Explanation and discovery share classification for all turn-wide counts and positive-witness flags.")
    for flag_value in (False, True):
        filtered = successful(reader.find_turns(path, symbols["R"], filters={"text_mismatch": flag_value}, max_output_bytes=1048576, no_content=no_content))
        wanted = [symbols["T1"], symbols["T2"]] if flag_value == expected["text_mismatch"] else []
        check.equal([row["turn"]["turn_id"] for row in filtered["items"]], wanted, msg="Filtering consumes corrected positive comparison facts rather than the unchanged SQL view's misleading linked label.")
    check.equal(hashlib.sha256(path.read_bytes()).hexdigest(), before, msg="All explanation and discovery routes retain the malformed input bytes for independent review.")


@pytest.mark.parametrize("no_content", [False, True], ids=["metadata", "no-content"])
def test_known_binding_contradictions_slice_preserves_claims_and_missing_rows(contradictory_bindings, no_content):
    path, symbols, case, claims = contradictory_bindings
    expected = BINDING_CONTRADICTIONS[case]
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    for request_name in ("W1", "W2", "W3"):
        value = reader.slice(path, symbols["R"], symbols[request_name], limit=1000, max_depth=12,
                             max_output_bytes=1048576, no_content=no_content)
        data = successful(value)
        expected_rows = [row for row in expected["rows"] if row[0] == request_name]
        check.equal(data["root_request_event_id"], symbols[request_name], msg="An incomplete binding graph retains the explicitly selected root request.")
        check.is_in(symbols[request_name], {row["event_id"] for row in data["events"]}, msg="A successful slice must include its root even when available attribution is contradictory.")
        check_binding_occurrences(data["answers"], expected_rows, symbols, claims, request_name + " slice")
        check.is_true(all(row["request_event_id"] == symbols[request_name] for row in data["answers"]), msg="Context-only witness visits must not add historical answer occurrences or fabricate the disappeared W1 row.")
        missing_witness_join = request_name == "W1" and case in {"disappeared-witness-membership", "absent-binding-handle"}
        invalid_rows = any(row[6] == "invalid_binding" for row in expected_rows)
        if invalid_rows or missing_witness_join:
            check.is_false(data["complete"], msg="Every expanded request using a known invalid binding is an incomplete ancestry result.")
            check.is_true(bool(value["unknowns"] or data["frontier"]), msg="An incomplete invalid-binding slice must expose its contradiction diagnostics.")
            answer_links = {row["link_id"] for row in data["links"] if row["relation"] == "answers"}
            check.equal(answer_links, {claims["D1"]["link_id"], claims["D2"]["link_id"]}, msg="Both explicit answer claims remain inspectable; the reader must not select one to conceal overlap or a later first witness.")
            check.is_true({symbols["D1"], symbols["D2"]}.issubset({row["event_id"] for row in data["events"]}), msg="Invalid association does not erase the explicit recorded decision endpoints.")
        if missing_witness_join:
            check.is_true(any(row["ids"].get("link_id") == claims["D1"]["link_id"] and row["code"] for row in value["unknowns"]), msg="The explicit invalid W1 claim remains an identified link diagnostic even after its handle-to-member join disappears.")
            check.is_true(any(row["kind"] == "link" and row["id"] == claims["D1"]["link_id"] and row["reason"] for row in data["frontier"]), msg="The incomplete W1 slice names the unresolvable stored claim in its frontier rather than manufacturing a D1 occurrence.")
        check.is_false(any(row["event_id"] in {symbols["L0"], symbols["L1"], symbols["F1"], symbols["F2"], symbols["F3"]} for row in data["events"]), msg="Contradiction handling must not broaden ancestry to unrelated discovery or future model responses.")
    check.equal(hashlib.sha256(path.read_bytes()).hexdigest(), before, msg="Slice diagnostics leave the original malformed rows and production link IDs untouched.")


def test_known_binding_contradictions_installed_cli_parity(contradictory_bindings):
    path, symbols, case, _ = contradictory_bindings
    entry, no_content = {
        "overlapping-bindings": ("fr", False),
        "coherent-later-witness": ("module", True),
        "disappeared-witness-membership": ("fr", False),
        "absent-binding-handle": ("module", True),
    }[case]
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    cli = Path(sys.executable).with_name("fr")
    assert entry != "fr" or cli.is_file(), "The entry-point regression must execute this worker's installed fr, never a source-tree substitute."
    prefix = [str(cli)] if entry == "fr" else [sys.executable, "-I", "-B", "-m", "mcp_flightrecorder"]
    outside = path.parent / "outside-checkout"
    outside.mkdir()
    common = ["--run", symbols["R"], "--max-output-bytes", "1048576"] + (["--no-content"] if no_content else [])
    routes = [
        (["check", str(path)], reader.check(path, symbols["R"], max_output_bytes=1048576, no_content=no_content), 2),
        (["explain", str(path), "--turn", symbols["T1"], "--model-call", symbols["M0"]], reader.explain(path, symbols["R"], symbols["T1"], symbols["M0"], max_output_bytes=1048576, no_content=no_content), 0),
        (["explain", str(path), "--turn", symbols["T2"], "--model-call", symbols["M3"]], reader.explain(path, symbols["R"], symbols["T2"], symbols["M3"], max_output_bytes=1048576, no_content=no_content), 0),
        (["turns", str(path), "--text-mismatch", "true"], reader.find_turns(path, symbols["R"], filters={"text_mismatch": True}, max_output_bytes=1048576, no_content=no_content), 0),
        (["slice", str(path), symbols["W3"]], reader.slice(path, symbols["R"], symbols["W3"], max_output_bytes=1048576, no_content=no_content), 0),
    ]
    for arguments, expected, exit_code in routes:
        successful(expected)
        process = subprocess.run(prefix + arguments + common, cwd=outside, capture_output=True, text=True, timeout=20)
        assert process.stdout, f"The installed {entry} {arguments[0]} must emit an envelope before parity checks: {process.stderr}"
        value = json.loads(process.stdout)
        check.equal(process.returncode, exit_code, msg="A successfully completed contradictory check exits 2; successful explanation/discovery/slice reads exit 0.")
        check.equal(value, expected, msg="The selected installed CLI route and public Python retain identical corrected rows, witnesses, flags, counts and diagnostics.")
        check.equal((process.stdout.count("\n"), process.stdout.endswith("\n"), process.stderr), (1, True, ""), msg="Each installed route emits exactly one compact JSON line without progress output or tracebacks.")
    check.equal(successful(routes[0][1])["status"], "fail", msg="Entry parity must be anchored to a failed known-binding oracle, not equal copies of an incorrect successful check.")
    check.equal(hashlib.sha256(path.read_bytes()).hexdigest(), before, msg="Installed command execution remains read-only under the assigned metadata policy.")


@pytest.mark.parametrize("control", ["canonical", "omitted-witness", "omitted-bodies", "missing-decision"])
@pytest.mark.parametrize("no_content", [False, True], ids=["metadata", "no-content"])
def test_binding_contradiction_neighboring_available_and_unknown_controls(recording, control, no_content):
    path, symbols = recording
    with closing(sqlite3.connect(path)) as connection:
        if control == "omitted-witness":
            artifact_id, _ = manifest(connection, symbols["W1"])
            omit(connection, artifact_id)
        elif control == "omitted-bodies":
            _, value = manifest(connection, symbols["W1"])
            for artifact_id in value["message_refs"][3:5]:
                omit(connection, artifact_id)
        elif control == "missing-decision":
            connection.execute("UPDATE links SET source_event_id='symbolic-missing-decision' WHERE relation='answers' AND source_event_id=?", (symbols["D1"],))
        connection.commit()
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    value = reader.check(path, symbols["R"], max_output_bytes=1048576, no_content=no_content)
    data, checks = successful(value), items(value)
    expected = {
        "canonical": ("pass", (4, 4, 0, 0), (4, 4, 0, 0), (6, 6, 0, 3, 3, 0)),
        "omitted-witness": ("unknown", (4, 3, 1, 0), (4, 3, 1, 0), (4, 4, 0, 2, 2, 0)),
        "omitted-bodies": ("pass", (4, 4, 0, 0), (4, 4, 0, 0), (6, 6, 0, 3, 3, 0)),
        "missing-decision": ("unknown", (4, 4, 0, 0), (4, 1, 3, 0), (6, 3, 3, 0, 3, 3)),
    }[control]
    check.equal((data["status"], counts(checks["request_structure"]), counts(checks["answer_bindings"])), expected[:3], msg="Known valid identity, unavailable first-witness membership and a symbolic endpoint keep their distinct request-unit classifications.")
    if control == "omitted-witness":
        check.equal((checks["request_structure"]["status"], checks["answer_bindings"]["status"]), ("unknown", "unknown"), msg="Each unavailable-manifest request unit retains its own unknown classification under both output policies.")
    check.equal(tuple(data["stats"][key] for key in ("answer_occurrences", "linked_answers", "unresolved_answers", "text_equal", "text_mismatch", "text_unknown")), expected[3], msg="The repair preserves canonical comparisons, body-independent tokens and explicit later occurrences without inventing missing populations.")
    check.equal(data["required_failures"], [], msg="Neither unavailable evidence nor legitimate repeated bindings are observed contradictions.")
    if control in {"canonical", "omitted-bodies"}:
        check.equal([row["status"] for row in data["items"]], ["pass"] * 9, msg="Both the canonical and body-only omission recordings retain all nine successful checks.")
    else:
        check.equal(checks["links"]["status"], "unknown", msg="Unavailable witness shape or an allowed symbolic decision makes link evidence unknown, never invalid.")
    all_inputs = []
    for request_name, call, turn in (("W1", "M1", "T1"), ("W2", "M2", "T2"), ("W3", "M3", "T2")):
        rows = successful(reader.explain(path, symbols["R"], symbols[turn], symbols[call], max_output_bytes=1048576, no_content=no_content))["detail"]["input_answers"]
        expected_count = 0 if control == "omitted-witness" and request_name == "W1" else 2
        check.equal(len(rows), expected_count, msg="An unavailable W1 manifest removes only its own two occurrence rows, while every complete later request retains both explicit inputs.")
        all_inputs.extend(rows)
        check.is_true(all(row["binding_witness_event_id"] == symbols["W1"] for row in rows), msg="Repeated and shifted valid occurrences retain the original first witness rather than choosing a later request.")
        expected_pointers = [] if expected_count == 0 else (["/messages/3/content", "/messages/4/content"] if request_name == "W1" else ["/messages/4/content", "/messages/5/content"])
        check.equal([row["target_pointer"] for row in rows], expected_pointers, msg="Neighbor controls preserve the exact original and rebased submitted positions.")
    check.is_false(any(row["association_state"] == "invalid_binding" for row in all_inputs), msg="The new contradiction validator must not manufacture invalid bindings from absent evidence or retained-body omission.")
    if control == "missing-decision":
        check.equal([row["association_state"] for row in all_inputs], ["missing_decision", "linked"] * 3, msg="A symbolic missing D1 endpoint stays visible in every current request without content-based replacement.")
    else:
        check.is_true(all(row["association_state"] == "linked" for row in all_inputs), msg="All available canonical/body-only/later-witness occurrences remain explicitly linked.")
    graph = successful(reader.slice(path, symbols["R"], symbols["W3"], max_output_bytes=1048576, no_content=no_content))
    check.equal([row["association_state"] for row in graph["answers"]], ["missing_decision", "linked"] if control == "missing-decision" else ["linked", "linked"], msg="W3 slice preserves the same explicit association classification under both output policies.")
    check.equal(hashlib.sha256(path.read_bytes()).hexdigest(), before, msg="Positive and unavailable control reads preserve the independently prepared database bytes.")
