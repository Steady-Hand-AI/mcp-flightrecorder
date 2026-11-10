"""Installed navigation behavior against literal inspection fixture oracles.

Modified copies below are reader-input controls, not producer recordings.
The generated fixture and all expected associations remain independent of the
candidate formatter, comparison helpers and traversal implementation.
"""
import base64
from contextlib import closing
import hashlib
import json
import shutil
import sqlite3
import threading
import time

import pytest
from pytest_check import check

from mcp_flightrecorder import Recorder, inspect

from .recordings import core_recording, integration_recording


INTEGRATION, SYMBOLS = integration_recording()
CORE, CORE_SYMBOLS = core_recording()
MAX_OUTPUT = 1_048_576
COUNTS = {
    "T1": {"events": 15, "observed_events": 15, "model_calls": 2, "model_requests": 2,
           "model_responses": 2, "decisions": 2, "operations": 3, "request_members": 8,
           "input_answers": 2, "produced_answers": 6},
    "T2": {"events": 4, "observed_events": 4, "model_calls": 2, "model_requests": 2,
           "model_responses": 2, "decisions": 0, "operations": 0, "request_members": 18,
           "input_answers": 4, "produced_answers": 0},
}
FLAGS = {
    "T1": {"tool_error": False, "text_mismatch": True, "unattributed_operation": False, "forwarded_change": True},
    "T2": {"tool_error": False, "text_mismatch": True, "unattributed_operation": False, "forwarded_change": False},
}
COVERAGE = {"producers": {"model": "reader.model", "host": "reader.host", "mcp": "reader.mcp"}, "model_complete": True}


def compact(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")


def success(result, operation):
    assert result["ok"] is True, f"{operation} must complete before its dependent data are inspected: {result['error']}"
    check.equal(set(result), {"format", "operation", "ok", "scope", "data", "unknowns", "cutoffs", "diagnostics_omitted", "error", "untrusted_content"}, msg="Every reader uses the exact common envelope keys")
    check.equal(result["format"], "mfr.inspect/1", msg="The installed envelope format is versioned independently of the package")
    check.equal(result["operation"], operation, msg="The envelope identifies the Python operation spelling")
    check.is_none(result["error"], msg="A completed read has no operational error")
    check.is_true(result["untrusted_content"], msg="Recorded metadata remains explicitly untrusted host content")
    return result["data"]


def refused(result, code, reason=None):
    assert result["ok"] is False, f"The expected {code} refusal must precede dependent error-field checks"
    check.equal(result["error"]["code"], code, msg="The refusal preserves the contractual error category")
    if reason is not None:
        check.equal(result["error"]["reason"], reason, msg="The refusal distinguishes the specified operational cause")
    check.is_none(result["data"], msg="An operational failure never exposes a partial successful data object")
    check.less_equal(len(compact(result)), 1024, msg="Expected errors fit the small bounded envelope without arbitrary input echo")


def copied(tmp_path, name="reader-input.sqlite"):
    destination = tmp_path / name
    shutil.copyfile(INTEGRATION, destination)
    return destination


def mutate(path, statement, parameters=()):
    with closing(sqlite3.connect(path)) as connection:
        connection.execute(statement, parameters)
        connection.commit()


def clone_rows(path, table, id_column, identifiers):
    """Make labelled independent reader-input rows with tied ordering values."""
    with closing(sqlite3.connect(path)) as connection:
        connection.row_factory = sqlite3.Row
        template = dict(connection.execute("SELECT * FROM " + table + " LIMIT 1").fetchone())
        columns = tuple(template)
        for identifier in identifiers:
            values = {**template, id_column: identifier}
            if table == "turns":
                values["prompt_ref"] = None
            connection.execute("INSERT INTO " + table + " (" + ",".join(columns) + ") VALUES (" + ",".join("?" for _ in columns) + ")", [values[column] for column in columns])
        connection.commit()


def accepted(receipt):
    assert receipt.status == "accepted", f"The public producer must admit this control before its evidence is tested: {receipt.reason}"
    return receipt


def model_pair(recorder, *, decision=False):
    request = accepted(recorder.observe("model.request", scope=recorder.capture_scope(), payload={"messages": []},
                                        facts={"messages": []}))
    choice = None
    if decision:
        choice = accepted(recorder.observe("model.tool_call", scope=recorder.capture_scope(), payload={"arguments": {"value": 1}},
                                           facts={"name": "same-tool", "model_call_id": request.model_call_id, "provider_ref": "same-provider-id"}))
    response = accepted(recorder.observe("model.response", scope=recorder.capture_scope(), payload={"answer": "done"},
                                         facts={"model_call_id": request.model_call_id, "state": "complete"}))
    return request, choice, response


def test_schema_exact_discovery_contract():
    data = success(inspect.schema(), "schema")
    check.equal(data["package_version"], "0.5.19", msg="Schema names this release version")
    check.equal(data["database_schema_version"], 6, msg="Inspection preserves the accepted database schema")
    check.equal(data["api_format"], "mfr.inspect/1", msg="Discovery advertises the common public envelope format")
    expected = {"check": "check", "find_turns": "turns", "explain": "explain", "payload": "payload", "slice": "slice", "query": "sql", "runs": "runs", "schema": "schema", "snapshot": "snapshot"}
    check.equal(set(data["operations"]), set(expected), msg="Discovery promises exactly the assigned readers, helpers and local copy command")
    for name, cli in expected.items():
        item = data["operations"][name]
        check.equal(item["cli"], cli, msg=f"{name} advertises its assigned CLI spelling")
        check.equal(item["python"], None if name == "snapshot" else name, msg=f"{name} distinguishes public Python readers from CLI-only snapshot")
        if name != "snapshot":
            check.is_true(callable(getattr(inspect, name)), msg=f"The advertised public {name} function is installed")
    check.equal(data["operations"]["payload"]["optional_defaults"], {"offset": 0, "max_bytes": 4096, "max_output_bytes": 65536, "no_content": False, "timeout": 10.0}, msg="Payload defaults are explicit and match the public signature")
    check.equal(data["operations"]["find_turns"]["optional_defaults"], {"filters": None, "limit": 50, "cursor": None, "max_output_bytes": 65536, "no_content": False, "timeout": 10.0}, msg="Turn pagination defaults agree with the public API")
    check.equal(data["limits"]["max_output_bytes"], {"default": 65536, "min": 1024, "max": 1048576}, msg="Encoded output limits include exact minimum/default/maximum")
    check.equal(data["limits"]["payload_bytes"], {"default": 4096, "min": 4, "max": 65536}, msg="Content budgets are separate from display-string limits")
    check.equal(data["limits"]["display_string_bytes"], 512, msg="Metadata strings expose the pinned UTF-8 display cap")
    check.equal(data["limits"]["query_value_bytes"], 8388608, msg="Arbitrary SQL exposes its fixed native value and encoded-row limit")
    check.equal(data["limits"]["cursor_chars"], 8192, msg="Discovery exposes one shared ceiling for complete incoming and generated cursor tokens")
    check.equal(data["limits"]["explain_calls"], 50, msg="Overview call cap is discoverable")
    check.equal(data["limits"]["explain_detail_records"], 200, msg="Selected detail cap counts all array records")
    check.equal(data["check_statuses"], {"values": ["pass", "fail", "unknown", "not_applicable"], "reserved": ["warn"], "exits": {"pass": 0, "not_applicable": 0, "unknown": 4, "fail": 2}}, msg="Check discovery reports completion exits and keeps reserved warn distinct from emitted statuses")
    check.equal(data["expect_boundaries"], ["model", "host", "mcp"], msg="Only actual capture boundaries are accepted expectations")
    check.equal(data["content_policy"]["no_content_disables"], ["payload", "query"], msg="No-content rejects only the explicit content operations")
    check.equal(data["errors"]["incompatible_schema"], 3, msg="Schema incompatibility has the pinned storage-class CLI exit")
    check.equal(data["errors"]["unknown_event"], 1, msg="Unknown selected model call retains the request-class exit")
    check.less_equal(len(compact(inspect.schema())), 65536, msg="Static schema respects the ordinary whole-envelope budget")


@pytest.mark.parametrize("population", [0, 1, 2], ids=["empty", "single", "two"])
def test_runs_population(tmp_path, population):
    path = copied(tmp_path)
    if population == 0:
        with closing(sqlite3.connect(path)) as connection:
            for table in ("links", "operations", "findings", "events", "turns", "artifacts", "runs"):
                connection.execute("DELETE FROM " + table)
            connection.commit()
    elif population == 2:
        clone_rows(path, "runs", "run_id", ["r_additional"])
    data = success(inspect.runs(path, max_output_bytes=MAX_OUTPUT), "runs")
    expected = [] if population == 0 else [SYMBOLS["R"]]
    if population == 2:
        expected = sorted([*expected, "r_additional"])
    check.equal([row["run_id"] for row in data["items"]], expected, msg="Run discovery exposes exactly the stored run population in timestamp/ID order")
    check.is_false(data["has_more"], msg="A complete small run population does not invent a continuation")
    check.is_none(data["next_cursor"], msg="An exhausted run page has no cursor")


def test_canonical_turn_counts_flags_and_overviews():
    turns = success(inspect.find_turns(INTEGRATION, SYMBOLS["R"], max_output_bytes=MAX_OUTPUT), "find_turns")
    check.equal([row["turn"]["turn_id"] for row in turns["items"]], [SYMBOLS["T1"], SYMBOLS["T2"]], msg="Both observed turns remain discoverable, including zero-new-decision T2")
    for symbol, calls, unassigned in (("T1", ["M0", "M1"], ["O0"]), ("T2", ["M2", "M3"], [])):
        row = next(item for item in turns["items"] if item["turn"]["turn_id"] == SYMBOLS[symbol])
        check.equal(row["counts"], COUNTS[symbol], msg=f"{symbol} has the literal retained counts in the acceptance oracle")
        check.equal(row["flags"], FLAGS[symbol], msg=f"{symbol} issues belong to their submitting/observing turn")
        overview = success(inspect.explain(INTEGRATION, SYMBOLS["R"], SYMBOLS[symbol], max_output_bytes=MAX_OUTPUT), "explain")
        check.equal(overview["counts"], COUNTS[symbol], msg="Explanation and discovery agree on exact turn-wide counts")
        check.equal([call["model_call_id"] for call in overview["calls"]], [SYMBOLS[name] for name in calls], msg="Call summaries follow their earliest authoritative observation order")
        check.equal([call["complete"] for call in overview["calls"]], [True, True], msg="All four canonical calls have complete request/response observations")
        check.equal([operation["operation_id"] for operation in overview["unassigned_operations"]], [SYMBOLS[name] for name in unassigned], msg="Unattributed discovery is visible without assigning it to a model decision")
        check.is_none(overview["detail"], msg="Overview does not fabricate a selected-call detail object")
        check.equal(row["turn"]["host_outcome"], "returned", msg="The canonical host returned normally")
        check.equal(row["turn"]["capture_state"], "complete", msg="The accepted closed fixture records complete turn capture")


def test_turn_aggregates_consume_late_comparison_diagnostics(tmp_path):
    """True issue flags must not suppress later occurrence or dispatch diagnostics."""
    path = copied(tmp_path, "late-comparison.sqlite")
    with closing(sqlite3.connect(path)) as connection:
        link_id, raw = connection.execute(
            "SELECT link_id,details_json FROM links WHERE run_id=? AND relation='answers' AND source_event_id=?",
            (SYMBOLS["R"], SYMBOLS["D1"]),
        ).fetchone()
        details = json.loads(raw)
        details["submitted_text_digest"].update(value=None, reason="disabled")
        connection.execute("UPDATE links SET details_json=? WHERE run_id=? AND link_id=?",
                           (json.dumps(details), SYMBOLS["R"], link_id))
        raw = connection.execute("SELECT details_json FROM events WHERE run_id=? AND event_id=?",
                                 (SYMBOLS["R"], SYMBOLS["D2"])).fetchone()[0]
        details = json.loads(raw)
        details["args_digest"].update(value=None, reason="disabled")
        connection.execute("UPDATE events SET details_json=? WHERE run_id=? AND event_id=?",
                           (json.dumps(details), SYMBOLS["R"], SYMBOLS["D2"]))
        connection.commit()
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    expected_unknowns = {
        "T1": [
            {"code": "disabled", "path": "", "ids": {"request_event_id": SYMBOLS["W1"], "link_id": link_id}, "count": 1},
            {"code": "disabled", "path": "", "ids": {"request_event_id": SYMBOLS["W1"], "link_id": link_id}, "count": 1},
            {"code": "disabled", "path": "", "ids": {"request_event_id": SYMBOLS["W2"], "link_id": link_id}, "count": 1},
            {"code": "disabled", "path": "", "ids": {"request_event_id": SYMBOLS["W3"], "link_id": link_id}, "count": 1},
            {"code": "disabled", "path": "", "ids": {"decision_event_id": SYMBOLS["D2"], "dispatch_event_id": SYMBOLS["H2"]}, "count": 1},
            {"code": "disabled", "path": "", "ids": {"decision_event_id": SYMBOLS["D2"], "dispatch_event_id": SYMBOLS["H2"]}, "count": 1},
        ],
        "T2": [
            {"code": "disabled", "path": "", "ids": {"request_event_id": SYMBOLS["W2"], "link_id": link_id}, "count": 1},
            {"code": "disabled", "path": "", "ids": {"request_event_id": SYMBOLS["W3"], "link_id": link_id}, "count": 1},
        ],
    }
    for turn in ("T1", "T2"):
        result = inspect.explain(path, SYMBOLS["R"], SYMBOLS[turn], max_output_bytes=MAX_OUTPUT)
        data = success(result, "explain")
        check.equal(data["counts"], COUNTS[turn], msg="Disabled comparisons do not change any retained occurrence count")
        check.equal(data["flags"], FLAGS[turn], msg="Earlier conclusive mismatches and dispatch changes retain their literal turn flags")
        check.is_none(data["detail"], msg="The preservation guard exercises turn aggregation without selected-call detail arrays")
        check.equal(result["unknowns"], expected_unknowns[turn], msg="Every iterator is exhausted in order, preserving late diagnostics and intentional repeated notices")
        check.equal(result["cutoffs"], [], msg="The complete small overview needs no record or byte cutoff")
        check.equal(result["diagnostics_omitted"], {"unknowns": 0, "cutoffs": 0}, msg="No comparison notice disappears behind an omitted-diagnostic count")
    check.equal(hashlib.sha256(path.read_bytes()).hexdigest(), before, msg="Both public overview reads preserve the controlled source bytes")


@pytest.mark.parametrize("turn,timeline", [("T1", "L0 L1 R0 D1 D2 Z0 H1 S1 H2 S2 E2 E1 P W1 F1"), ("T2", "W2 F2 W3 F3")], ids=["T1", "T2"])
def test_canonical_timeline_through_public_query(turn, timeline):
    data = success(inspect.query(INTEGRATION, "SELECT event_id FROM events WHERE run_id=? AND turn_id=? ORDER BY sequence,event_id", [SYMBOLS["R"], SYMBOLS[turn]]), "query")
    check.equal(data["rows"], [[SYMBOLS[symbol]] for symbol in timeline.split()], msg="The fixed event timeline preserves overlapping operation completion order")


@pytest.mark.parametrize("call,turn,request_symbol,response,decisions,operations,members,inputs,produced", [
    ("M0", "T1", "R0", "Z0", ["D1", "D2"], ["O1", "O2"], 2, 0, 6),
    ("M1", "T1", "W1", "F1", [], [], 6, 2, 0),
    ("M2", "T2", "W2", "F2", [], [], 8, 2, 0),
    ("M3", "T2", "W3", "F3", [], [], 10, 2, 0),
], ids=["M0", "M1", "M2", "M3"])
def test_canonical_selected_call(call, turn, request_symbol, response, decisions, operations, members, inputs, produced):
    data = success(inspect.explain(INTEGRATION, SYMBOLS["R"], SYMBOLS[turn], SYMBOLS[call], max_output_bytes=MAX_OUTPUT), "explain")
    assert len(data["calls"]) == 1, "Selected-call mode must return one summary before its fields are compared"
    check.equal(data["counts"], COUNTS[turn], msg="Selecting a call never narrows the top-level turn aggregate")
    check.equal(data["calls"][0], {"model_call_id": SYMBOLS[call], "request_event_ids": [SYMBOLS[request_symbol]], "response_event_ids": [SYMBOLS[response]], "decision_event_ids": [SYMBOLS[symbol] for symbol in decisions], "operation_ids": [SYMBOLS[symbol] for symbol in operations], "complete": True}, msg="Call membership follows explicit request, response, decision and operation identities")
    detail = data["detail"]
    for name, expected in (("members", members), ("input_answers", inputs), ("produced_answers", produced), ("operations", len(operations))):
        check.equal(detail["totals"][name], expected, msg=f"{call} exact {name} count follows the independent fixture oracle")
        check.equal(len(detail[name]), expected, msg=f"The generous budget retains every {call} {name} row")
    check.equal([row["operation_id"] for row in detail["operations"]], [SYMBOLS[symbol] for symbol in operations], msg="Selected detail does not include sibling or discovery operations")
    for operation in detail["operations"]:
        check.equal(operation["status"], "ok", msg="A witnessed successful MCP operation remains a success")
        check.equal(operation["resolution"], "unknown", msg="Missing transport/cache knowledge is not inferred as a cache hit")
        check.is_none(operation["input_rounds"], msg="Unobserved input rounds remain null rather than zero")


def test_canonical_answers_dispatch_and_context_insertion():
    first = success(inspect.explain(INTEGRATION, SYMBOLS["R"], SYMBOLS["T1"], SYMBOLS["M0"], max_output_bytes=MAX_OUTPUT), "explain")["detail"]
    expected_dispatches = [{"decision_event_id": SYMBOLS["D1"], "dispatch_event_id": SYMBOLS["H1"], "forwarded_changed": True, "comparison_reason": None}, {"decision_event_id": SYMBOLS["D2"], "dispatch_event_id": SYMBOLS["H2"], "forwarded_changed": False, "comparison_reason": None}]
    check.equal(first["dispatches"], expected_dispatches, msg="Forwarded argument comparisons use the two explicit decision/dispatch relationships")
    answers = first["produced_answers"]
    assert len(answers) == 6, "Six retained occurrences are required before comparing repeated binding identities"
    expected_rows = [("W1", "D1", "/messages/3/content", True), ("W1", "D2", "/messages/4/content", False), ("W2", "D1", "/messages/4/content", True), ("W2", "D2", "/messages/5/content", False), ("W3", "D1", "/messages/4/content", True), ("W3", "D2", "/messages/5/content", False)]
    check.equal([(row["request_event_id"], row["source_event_id"], row["target_pointer"], row["text_equal"]) for row in answers], [(SYMBOLS[request], SYMBOLS[decision], pointer, equal) for request, decision, pointer, equal in expected_rows], msg="Actual submitted pointers shift with history while original text comparisons remain explicit")
    for row in answers:
        check.equal(row["binding_witness_event_id"], SYMBOLS["W1"], msg="Repeated history retains the original W1 binding witness")
        check.equal(row["binding_request_event_id"], SYMBOLS["W1"], msg="Binding request is distinct from the current occurrence request")
        check.equal(row["association_state"], "linked", msg="Each canonical explicit answer association is conclusive")
    check.equal(len({row["message_artifact_id"] for row in answers}), 2, msg="The two distinct answer handles are reused without collapsing their six ordered occurrences")
    check.equal(len({row["link_id"] for row in answers}), 2, msg="The first-witness binding link is reused for later explicit submissions")
    transformed = {(row["source_event_id"], row["target_event_id"]) for row in first["links"] if row["relation"] == "transformed_from"}
    check.equal(transformed, {(SYMBOLS["E1"], SYMBOLS["P"]), (SYMBOLS["E2"], SYMBOLS["P"])}, msg="Prepared context P has both explicit result sources")
    later = success(inspect.explain(INTEGRATION, SYMBOLS["R"], SYMBOLS["T1"], SYMBOLS["M1"], max_output_bytes=MAX_OUTPUT), "explain")["detail"]
    insertion = [row for row in later["links"] if row["relation"] == "inserted_into"]
    check.equal([(row["source_event_id"], row["target_event_id"], row["target_pointer"]) for row in insertion], [(SYMBOLS["P"], SYMBOLS["W1"], "/messages/5/content")], msg="Only W1 has the explicit prepared-context insertion")


def test_core_repeated_answers_and_zero_decision_turn():
    symbols = CORE_SYMBOLS
    data = success(inspect.explain(CORE, symbols["run_id"], symbols["turns"]["T1"], symbols["model_calls"]["M0"], max_output_bytes=MAX_OUTPUT), "explain")
    answers = data["detail"]["produced_answers"]
    check.equal([row["target_pointer"] for row in answers], ["/messages/2/content", "/messages/3/content", "/messages/4/content"], msg="Core repeated history retains each actual shifted occurrence pointer")
    check.equal([row["text_equal"] for row in answers], [False, False, False], msg="Core's three original-versus-submitted comparisons are all mismatches")
    check.equal([row["binding_witness_event_id"] for row in answers], [symbols["events"]["W0"]] * 3, msg="Core W0 remains the original binding witness in later turns")
    later = success(inspect.explain(CORE, symbols["run_id"], symbols["turns"]["T2"], max_output_bytes=MAX_OUTPUT), "explain")
    check.equal(later["counts"]["decisions"], 0, msg="Complete core T2 has zero new model decisions")
    check.equal([row["model_call_id"] for row in later["calls"]], [symbols["model_calls"]["M2"], symbols["model_calls"]["M3"]], msg="Zero new decisions does not erase T2's two observed model calls")


@pytest.mark.parametrize("filters,expected", [
    ({"forwarded_change": True}, ["T1"]), ({"forwarded_change": False}, ["T2"]),
    ({"text_mismatch": True}, ["T1", "T2"]), ({"text_mismatch": False}, []),
    ({"tool_error": True}, []), ({"tool_error": False}, ["T1", "T2"]),
    ({"unattributed_operation": True}, []), ({"unattributed_operation": False}, ["T1", "T2"]),
    ({"host_outcome": "returned", "capture_state": "complete", "forwarded_change": True}, ["T1"]),
    ({"host_outcome": "not-observed"}, []),
], ids=["forwarded-true", "forwarded-false", "mismatch-true", "mismatch-false", "error-true", "error-false", "unattributed-true", "unattributed-false", "and", "empty"])
def test_turn_filter_matrix(filters, expected):
    data = success(inspect.find_turns(INTEGRATION, SYMBOLS["R"], filters=filters), "find_turns")
    check.equal([item["turn"]["turn_id"] for item in data["items"]], [SYMBOLS[name] for name in expected], msg="Closed filters use exact witnessed flags and combine with AND, including explicit false")


@pytest.mark.parametrize("kind", ["runs", "turns"])
def test_keyset_pages_preserve_equal_timestamp_ties(tmp_path, kind):
    path = copied(tmp_path)
    if kind == "runs":
        clone_rows(path, "runs", "run_id", ["r_000", "r_zzz"])
        call, expected, identity = lambda **kw: inspect.runs(path, **kw), sorted(["r_000", SYMBOLS["R"], "r_zzz"]), lambda row: row["run_id"]
    else:
        mutate(path, "UPDATE turns SET started_at='2000-01-01T00:00:00Z'")
        clone_rows(path, "turns", "turn_id", ["t_000", "t_zzz"])
        call, expected, identity = lambda **kw: inspect.find_turns(path, SYMBOLS["R"], **kw), sorted(["t_000", SYMBOLS["T1"], SYMBOLS["T2"], "t_zzz"]), lambda row: row["turn"]["turn_id"]
    collected, cursor = [], None
    for index in range(len(expected) + 1):
        result = call(limit=1 if index == 0 else 2, cursor=cursor, max_output_bytes=MAX_OUTPUT)
        data = success(result, "runs" if kind == "runs" else "find_turns")
        collected.extend(identity(row) for row in data["items"])
        cursor = data["next_cursor"]
        if index == 0:
            mutate(path, "UPDATE runs SET ended_at=ended_at WHERE run_id=?", (SYMBOLS["R"],))
        if cursor is None:
            break
    check.equal(collected, expected, msg="Changing a page limit preserves every tied full ordering tuple exactly once")
    check.is_none(cursor, msg="Keyset continuation exhausts without retaining a cross-page transaction")
    mutate(path, "CREATE INDEX inspection_after_paging ON runs(ended_at)")
    check.is_true(inspect.runs(path)["ok"], msg="A benign index remains compatible and completed pages release read ownership")


@pytest.mark.parametrize("change", ["path", "run", "filters", "no-content", "version", "order", "operation", "malformed"], ids=["path", "run", "filters", "no-content", "version", "order", "operation", "malformed"])
def test_cursor_scope_mismatch(tmp_path, change):
    path = copied(tmp_path)
    clone_rows(path, "runs", "run_id", ["r_other"])
    page = success(inspect.find_turns(path, SYMBOLS["R"], limit=1), "find_turns")
    assert page["next_cursor"], "The first of two turns must supply a continuation token for mismatch controls"
    token, run, kwargs = page["next_cursor"], SYMBOLS["R"], {}
    if change == "path":
        second = tmp_path / "copied-again.sqlite"
        shutil.copyfile(path, second)
        path = second
    elif change == "run":
        run = "r_other"
    elif change == "filters":
        kwargs["filters"] = {"text_mismatch": True}
    elif change == "no-content":
        kwargs["no_content"] = True
    elif change == "malformed":
        token = "not a base64 cursor!"
    else:
        decoded = json.loads(base64.urlsafe_b64decode(token + "=" * (-len(token) % 4)))
        decoded[change] = {"version": 999, "order": ["turn_id", "started_at"], "operation": "runs"}[change]
        token = base64.urlsafe_b64encode(compact(decoded)).rstrip(b"=").decode()
    refused(inspect.find_turns(path, run, cursor=token, **kwargs), "cursor_mismatch")


@pytest.mark.parametrize("operation,kwargs", [
    ("runs", {"limit": True}), ("runs", {"limit": 0}), ("runs", {"limit": 201}),
    ("runs", {"max_output_bytes": True}), ("runs", {"max_output_bytes": 1023}), ("runs", {"max_output_bytes": 1048577}),
    ("runs", {"no_content": 1}), ("find_turns", {"filters": {"unknown": True}}),
    ("find_turns", {"filters": {"tool_error": 1}}), ("find_turns", {"filters": {"condition_key": ""}}),
    ("find_turns", {"filters": {"capture_state": "invented"}}), ("find_turns", {"filters": []}),
], ids=["bool-limit", "zero-limit", "over-limit", "bool-budget", "small-budget", "large-budget", "nonbool-policy", "unknown-filter", "nonbool-filter", "empty-filter", "bad-capture-state", "nonmapping-filter"])
def test_invalid_navigation_arguments_precede_open(tmp_path, operation, kwargs):
    path = tmp_path / "absent.sqlite"
    args = [path] if operation == "runs" else [path, SYMBOLS["R"]]
    refused(getattr(inspect, operation)(*args, **kwargs), "invalid_request")
    check.is_false(path.exists(), msg="Invalid requests do not create a recording while checking argument shape")


@pytest.mark.parametrize("field,value", [("run_id", ""), ("run_id", True), ("turn_id", 7), ("turn_id", []), ("model_call_id", False)], ids=["empty-run", "bool-run", "integer-turn", "list-turn", "bool-call"])
def test_invalid_identity_shapes(field, value):
    arguments = {"run_id": SYMBOLS["R"], "turn_id": SYMBOLS["T1"]}
    arguments[field] = value
    refused(inspect.explain(INTEGRATION, **arguments), "invalid_request")


@pytest.mark.parametrize("selection,expected,reason", [
    ("run", "unknown_run", "unknown_run"), ("turn", "unknown_turn", "unknown_turn"),
    ("foreign-turn", "unknown_turn", "unknown_turn"), ("call", "unknown_event", "unknown_model_call"),
    ("foreign-call", "unknown_event", "unknown_model_call"), ("artifact", "unknown_artifact", "unknown_artifact"),
    ("foreign-artifact", "unknown_artifact", "unknown_artifact"),
], ids=["run", "turn", "foreign-turn", "call", "foreign-call", "artifact", "foreign-artifact"])
def test_unknown_and_foreign_scope_identities(tmp_path, selection, expected, reason):
    path = copied(tmp_path)
    clone_rows(path, "runs", "run_id", ["r_other"])
    if selection == "run":
        result = inspect.explain(path, "absent", "also-absent", "also-absent")
    elif selection in {"artifact", "foreign-artifact"}:
        with closing(sqlite3.connect(path)) as connection:
            artifact = connection.execute("SELECT payload_ref FROM events WHERE event_id=?", (SYMBOLS["R0"],)).fetchone()[0]
        result = inspect.payload(path, "r_other" if selection == "foreign-artifact" else SYMBOLS["R"], artifact if selection == "foreign-artifact" else "absent")
    else:
        run = "r_other" if selection == "foreign-turn" else SYMBOLS["R"]
        turn = "absent" if selection == "turn" else SYMBOLS["T1"]
        call = SYMBOLS["M2"] if selection == "foreign-call" else "absent" if selection == "call" else None
        result = inspect.explain(path, run, turn, call)
    refused(result, expected, reason)


@pytest.mark.parametrize("column,value", [("provenance", "imported"), ("source_id", "other-producer"), ("event_version", 99)], ids=["imported", "wrong-source", "unsupported-version"])
def test_non_authoritative_rows_stay_visible_without_counting(tmp_path, column, value):
    path = copied(tmp_path)
    mutate(path, "UPDATE events SET " + column + "=? WHERE event_id=?", (value, SYMBOLS["D1"]))
    result = inspect.explain(path, SYMBOLS["R"], SYMBOLS["T1"], SYMBOLS["M0"], max_output_bytes=MAX_OUTPUT)
    data = success(result, "explain")
    check.equal(data["counts"]["events"], 15, msg="Unsupported/imported evidence remains a stored event")
    check.equal(data["counts"]["observed_events"], 14, msg="Only the configured version-one observed source contributes authoritative events")
    check.equal(data["counts"]["decisions"], 1, msg="A non-authoritative tool-call row is not silently counted as an observed decision")
    found = [row for row in data["detail"]["events"] if row["event_id"] == SYMBOLS["D1"]]
    assert len(found) == 1, "The explicitly selected call still exposes its non-authoritative stored row"
    check.is_false(found[0]["authoritative"], msg="The event metadata labels excluded authority explicitly")
    check.is_true(any(item["code"] == "non_authoritative" for item in result["unknowns"]), msg="Excluded authority is disclosed as uncertainty")


@pytest.mark.parametrize("change,expected", [("request-false", False), ("response-false", False), ("decision-false", False), ("request-missing", None), ("response-missing", None)], ids=["request-false", "response-false", "decision-false", "request-missing", "response-missing"])
def test_call_completeness_needs_all_authoritative_model_observations(tmp_path, change, expected):
    path = copied(tmp_path)
    symbol = {"request": "R0", "response": "Z0", "decision": "D1"}[change.split("-")[0]]
    if change.endswith("missing"):
        mutate(path, "UPDATE events SET provenance='imported' WHERE event_id=?", (SYMBOLS[symbol],))
    else:
        mutate(path, "UPDATE events SET details_json=json_set(details_json,'$.complete',json('false')) WHERE event_id=?", (SYMBOLS[symbol],))
    data = success(inspect.explain(path, SYMBOLS["R"], SYMBOLS["T1"], SYMBOLS["M0"], max_output_bytes=MAX_OUTPUT), "explain")
    check.equal(data["calls"][0]["complete"], expected, msg="False observations remain false and missing request/response remains unknown, never complete")


@pytest.mark.parametrize("identity", ["decision", "dispatch"], ids=["decision-identity", "dispatch-identity"])
def test_explicit_operation_fallback_identity(tmp_path, identity):
    path = copied(tmp_path)
    statement = "UPDATE operations SET model_call_id=NULL" + (",decision_id=NULL" if identity == "dispatch" else "") + " WHERE operation_id=?"
    mutate(path, statement, (SYMBOLS["O1"],))
    data = success(inspect.explain(path, SYMBOLS["R"], SYMBOLS["T1"], SYMBOLS["M0"], max_output_bytes=MAX_OUTPUT), "explain")
    check.equal([row["operation_id"] for row in data["detail"]["operations"]], [SYMBOLS["O1"], SYMBOLS["O2"]], msg="A stored operation joins through its explicit decision or dispatch even without a model-call column")
    check.equal(data["calls"][0]["operation_ids"], [SYMBOLS["O1"], SYMBOLS["O2"]], msg="Summary and detail use the same explicit attribution rule")
    check.equal([row["operation_id"] for row in data["unassigned_operations"]], [SYMBOLS["O0"]], msg="Explicit fallback attribution does not leave an operation incorrectly unassigned")


def test_missing_decision_answer_is_not_inner_joined_away(tmp_path):
    path = copied(tmp_path)
    mutate(path, "DELETE FROM events WHERE event_id=?", (SYMBOLS["D1"],))
    result = inspect.explain(path, SYMBOLS["R"], SYMBOLS["T1"], SYMBOLS["M1"], max_output_bytes=MAX_OUTPUT)
    answers = success(result, "explain")["detail"]["input_answers"]
    assert len(answers) == 2, "A missing symbolic decision endpoint must not remove its observed answer occurrence"
    first = next(row for row in answers if row["source_event_id"] == SYMBOLS["D1"])
    check.equal(first["association_state"], "missing_decision", msg="Unavailable decision identity remains explicit on the occurrence")
    check.is_none(first["text_equal"], msg="Missing decision authority cannot produce a comparison conclusion")


@pytest.mark.parametrize("status,expected", [("tool_error", True), ("protocol_error", True), ("transport_error", True), ("timeout", True), ("local_error", True), ("cancelled", False), ("running", False), ("incomplete", False)], ids=["tool-error", "protocol-error", "transport-error", "timeout", "local-error", "cancelled", "running", "incomplete"])
def test_public_operation_outcome_flags(tmp_path, status, expected):
    path = tmp_path / "producer.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    run = recorder.capture_scope().run_id
    try:
        with recorder.turn("Outcome control") as turn:
            accepted(turn.receipt)
            start = accepted(recorder.start_operation(scope=recorder.capture_scope(), client_id="client", method="tools/call", arguments={}, name="tool"))
            if status not in {"running", "incomplete"}:
                accepted(recorder.finish_operation(start.ticket, status=status, result={"outcome": status}))
        if status == "running":
            deadline = time.monotonic() + 5
            while True:
                with closing(sqlite3.connect(path)) as connection:
                    stored = connection.execute("SELECT status FROM operations WHERE operation_id=?", (start.operation_id,)).fetchone()
                if stored is not None:
                    break
                assert time.monotonic() < deadline, "The admitted running operation must commit within the test watchdog"
                threading.Event().wait(0.01)
        else:
            closed = recorder.close()
            assert closed.writer_stopped, "The producer writer must join before testing a terminal or close-marked operation"
        data = success(inspect.explain(path, run, turn.receipt.turn_id), "explain")
        check.equal(data["flags"]["tool_error"], expected, msg="Only the five documented error statuses set the tool_error discovery flag")
        check.is_true(data["flags"]["unattributed_operation"], msg="An explicitly unassociated tools/call is distinguished from discovery")
        check.equal(data["unassigned_operations"][0]["status"], status, msg="Explanation preserves the producer's terminal/open/close-marked status")
        check.equal(data["calls"], [], msg="An unattributed operation never invents a model-call identity")
    finally:
        recorder.close()


def test_public_refusal_unattributed_identity_and_positive_host_cache(tmp_path):
    path = tmp_path / "producer.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    run = recorder.capture_scope().run_id
    try:
        with recorder.turn("Explicit refusal and cache control") as turn:
            accepted(turn.receipt)
            request, decision, response = model_pair(recorder, decision=True)
            with recorder.dispatch(action="refuse", name="same-tool", arguments={"value": 1}, decision_event_ids=[decision.event_id]) as dispatch:
                accepted(dispatch.receipt)
            start = accepted(recorder.start_operation(scope=recorder.capture_scope(), client_id="client", method="tools/call", arguments={"value": 1}, name="same-tool"))
            served = accepted(recorder.observe("cache.served", scope=recorder.capture_scope(), payload="cached-value", facts={"owner": "host", "operation_id": start.operation_id, "cache_id": "positive-cache"}))
            accepted(recorder.finish_operation(start.ticket, status="ok", result="cached-value", resolution="host_cache"))
    finally:
        closed = recorder.close()
    assert closed.writer_stopped and closed.portable, "The public positive-control recording must close portably before inspection"
    data = success(inspect.explain(path, run, turn.receipt.turn_id, request.model_call_id, max_output_bytes=MAX_OUTPUT), "explain")
    check.equal(data["calls"][0]["decision_event_ids"], [decision.event_id], msg="A refused decision is still an observed model decision")
    check.equal(data["calls"][0]["operation_ids"], [], msg="Matching name/arguments/provider labels never attribute the unrelated MCP operation to a refusal")
    check.equal(data["detail"]["totals"]["operations"], 0, msg="A refused decision remains valid with no attributed operation or follow-up request")
    check.equal(data["calls"][0]["complete"], True, msg="The completed request/response pair remains complete without a follow-up")
    check.equal(data["unassigned_operations"][0]["resolution"], "host_cache", msg="An explicit positive cache observation remains distinguishable from unknown resolution")
    check.equal(data["detail"]["dispatches"][0]["dispatch_event_id"], dispatch.receipt.event_id, msg="A refused dispatch is retained even when no operation follows")
    cache = success(inspect.query(path, "SELECT event_id,kind,boundary FROM observed_events WHERE event_id=?", [served.event_id]), "query")
    check.equal(cache["rows"], [[served.event_id, "cache.served", "host"]], msg="Positive host cache service is an observed event, not an inference from absent network work")


def test_public_empty_complete_turn_remains_explainable(tmp_path):
    path = tmp_path / "empty-turn.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    run = recorder.capture_scope().run_id
    try:
        with recorder.turn("No work selected") as turn:
            accepted(turn.receipt)
    finally:
        closed = recorder.close()
    assert closed.writer_stopped and closed.portable, "The public empty turn must close before its absence facts are inspected"
    data = success(inspect.explain(path, run, turn.receipt.turn_id), "explain")
    check.equal(data["counts"], {key: 0 for key in COUNTS["T1"]}, msg="A complete no-work turn has known zero retained event, call and operation populations")
    check.equal(data["calls"], [], msg="An existing zero-call turn is a successful explanation")
    check.equal(data["unassigned_operations"], [], msg="An empty turn acquires no inferred operation")
    check.equal(data["flags"], {key: False for key in FLAGS["T1"]}, msg="Known absence alone does not become an error, rewrite or attribution flag")
    check.equal(data["turn"]["capture_state"], "complete", msg="Declared complete empty capture is preserved independently of model-call count")


def test_reused_provider_labels_do_not_merge_decisions_or_assign_operation(tmp_path):
    path = tmp_path / "same-labels.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    run = recorder.capture_scope().run_id
    try:
        with recorder.turn("Repeated labels") as turn:
            accepted(turn.receipt)
            request = accepted(recorder.observe("model.request", scope=recorder.capture_scope(), payload={"messages": []}, facts={"messages": []}))
            decisions = [accepted(recorder.observe("model.tool_call", scope=recorder.capture_scope(), payload={"arguments": {"same": True}}, facts={"model_call_id": request.model_call_id, "name": "same-name", "provider_ref": "same-provider-id"})) for _ in range(2)]
            accepted(recorder.observe("model.response", scope=recorder.capture_scope(), payload="done", facts={"model_call_id": request.model_call_id}))
            operation = accepted(recorder.start_operation(scope=recorder.capture_scope(), client_id="client", method="tools/call", name="same-name", arguments={"same": True}))
            accepted(recorder.finish_operation(operation.ticket, status="ok", result="done"))
    finally:
        closed = recorder.close()
    assert closed.writer_stopped and closed.portable, "The repeated-label producer must close before explicit identities are inspected"
    data = success(inspect.explain(path, run, turn.receipt.turn_id, request.model_call_id, max_output_bytes=MAX_OUTPUT), "explain")
    check.equal(data["calls"][0]["decision_event_ids"], [row.event_id for row in decisions], msg="Repeated provider labels preserve two distinct observed decision identities")
    check.equal(len(set(data["calls"][0]["decision_event_ids"])), 2, msg="Equal names, arguments and provider IDs never merge decision occurrences")
    check.equal(data["detail"]["operations"], [], msg="A matching unassociated operation cannot be assigned through value equality or timing")
    check.equal([row["operation_id"] for row in data["unassigned_operations"]], [operation.operation_id], msg="The unmatched operation remains visible with its original explicit identity")


@pytest.mark.parametrize("change,code", [("version", "incompatible_schema"), ("view", "incompatible_schema"), ("column", "incompatible_schema"), ("missing", "recording_unreadable"), ("corrupt", "recording_unreadable")], ids=["version", "missing-view", "missing-column", "missing-file", "corrupt-file"])
def test_recording_schema_and_storage_errors(tmp_path, change, code):
    path = copied(tmp_path)
    if change == "version":
        mutate(path, "PRAGMA user_version=5")
    elif change == "view":
        mutate(path, "DROP VIEW answer_occurrences")
    elif change == "column":
        mutate(path, "ALTER TABLE runs RENAME COLUMN ended_at TO obsolete_ended_at")
    elif change == "missing":
        path.unlink()
    else:
        path.write_bytes(b"not a SQLite recording")
    refused(inspect.runs(path), code)


def test_read_only_special_path_and_accepted_file_identity(tmp_path):
    path = copied(tmp_path, "escaped ? # % recording.sqlite")
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    accepted_before = hashlib.sha256(INTEGRATION.read_bytes()).hexdigest()
    success(inspect.runs(path), "runs")
    success(inspect.find_turns(path, SYMBOLS["R"]), "find_turns")
    success(inspect.explain(path, SYMBOLS["R"], SYMBOLS["T1"], SYMBOLS["M0"], max_output_bytes=MAX_OUTPUT), "explain")
    refused(inspect.runs(path.as_uri() + "?mode=rw"), "invalid_request")
    check.equal(hashlib.sha256(path.read_bytes()).hexdigest(), before, msg="Escaped read-only access leaves the complete recording byte-identical")
    check.equal(hashlib.sha256(INTEGRATION.read_bytes()).hexdigest(), accepted_before, msg="Accepted original fixtures are never rewritten by reader controls")
