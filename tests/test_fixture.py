"""Installed-core behavior against the controlled trace and independent SQL oracle."""

from contextlib import closing
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys

from mcp_flightrecorder import Recorder
from pytest_check import check

from .fixture_checks import body, check_token, query, reconstruct, same_typed_value

ROOT = Path(__file__).resolve().parents[1]
COVERAGE = {"producers": {"model": "test.model", "host": "test.host", "mcp": "test.mcp"},
            "model_complete": True}


EXPECTED_CATALOG = [{
    "name": "echo", "description": "Returns the supplied text.",
    "inputSchema": {"type": "object", "properties": {"text": {"type": "string"}},
                    "required": ["text"], "additionalProperties": False},
}]
EXPECTED_MESSAGES = {
    "U": {"role": "user", "content": "Echo the selected text."},
    "A": {"role": "assistant", "tool_calls": [{"id": "provider-call-1", "type": "function",
          "function": {"name": "echo", "arguments": '{"text":"selected"}'}}]},
    "B": {"role": "tool", "tool_call_id": "provider-call-1", "content": "submitted"},
    "P1": {"role": "system", "content": "Synthetic retained history A."},
    "P2": {"role": "system", "content": "Synthetic retained history B."},
    "F0m": {"role": "assistant", "content": "First follow-up complete."},
    "U2": {"role": "user", "content": "Use retained context; do not select another tool."},
    "F1m": {"role": "assistant", "content": "No new tool call."},
    "F2m": {"role": "assistant", "content": "Retained context checked."},
}
TRACE = ["R0", "D", "Z0", "H", "S", "E", "W0", "F0", "W1", "F1", "W2", "F2"]
REQUESTS = {"R0": ["U"], "W0": ["U", "A", "B"],
            "W1": ["P1", "U", "A", "B", "F0m", "U2"],
            "W2": ["P1", "P2", "U", "A", "B", "F0m", "U2", "F1m"]}



def decoded(row, column):
    value = row[column]
    return json.loads(value) if value is not None else None


def assert_timelines(connection, sql_dir, summary):
    events, run = summary["events"], summary["run_id"]
    timeline = []
    for turn_symbol, trace in (("T1", TRACE[:8]), ("T2", TRACE[8:])):
        rows = query(connection, sql_dir, "q1", run_id=run, turn_id=summary["turns"][turn_symbol])
        assert same_typed_value([r['event_id'] for r in rows], [events[s] for s in trace]), f'Q1 {turn_symbol} preserves precisely the observed event order'
        for row in rows:
            assert same_typed_value(row['authoritative'], 1), f"Q1 event {row['event_id']} has the configured observed authority"
        timeline.extend(rows)
    assert len(timeline) == 12, 'all twelve event rows are required for dependent fixture checks'
    sequence = [row["sequence"] for row in timeline]
    assert all((left < right for left, right in zip(sequence, sequence[1:]))), 'writer sequence strictly increases across the complete trace'
    by_id = {row["event_id"]: row for row in timeline}
    kinds = ["model.request", "model.tool_call", "model.response", "host.dispatch", "mcp.request", "mcp.response",
             "model.request", "model.response", "model.request", "model.response", "model.request", "model.response"]
    assert same_typed_value([row['kind'] for row in timeline], kinds), 'the twelve timeline observations keep their specified kinds'
    for symbol, message_symbol in (("Z0", "A"), ("F0", "F0m"), ("F1", "F1m"), ("F2", "F2m")):
        row = by_id[events[symbol]]
        assert same_typed_value(decoded(row, 'payload'), EXPECTED_MESSAGES[message_symbol]), f'{symbol} retains the actually consumed complete response'
        assert same_typed_value(row['state'], 'complete'), f'{symbol} records observed completion state'
    assert same_typed_value(decoded(by_id[events['E']], 'payload'), {'content': [{'type': 'text', 'text': 'executed'}], 'isError': False}), 'terminal result preserves exposed text and tool error flag'
    for symbol, members in REQUESTS.items():
        row = by_id[events[symbol]]
        assert same_typed_value(row['payload_representation'], 'request_refs/2'), f'{symbol} retains the request_refs/2 representation'
        manifest = decoded(row, "payload")
        assert isinstance(manifest, dict), f'{symbol} manifest must be available'
        assert same_typed_value(manifest.get('format'), 'request_refs/2'), f'{symbol} manifest identifies contract 2'
        assert same_typed_value(len(manifest['message_refs']), len(members)), f'{symbol} membership keeps every original occurrence'
        expected = {"model": "fixture-model", "system": "Synthetic evidence fixture.",
                    "tools": EXPECTED_CATALOG, "messages": [EXPECTED_MESSAGES[key] for key in members]}
        assert same_typed_value(reconstruct(connection, manifest), expected), f'{symbol} reconstructs the original submitted request'
        assert same_typed_value(decoded(row, 'catalog'), EXPECTED_CATALOG), f'{symbol} preserves the per-request catalog'
        assert same_typed_value(row['catalog_ref'], next((field['artifact_id'] for field in manifest['field_refs'] if field['pointer'] == '/tools'))), f'{symbol} catalog_ref points at its declared tools field'
        member_rows = decoded(row, "request_members_json")
        assert same_typed_value([m['index'] for m in member_rows], list(range(len(members)))), f'{symbol} Q1 indexes retain ordered array positions'
        for member in member_rows:
            assert same_typed_value([member['artifact_missing'], member['omitted_reason'], member['redacted'], member['truncated']], [0, None, 0, 0]), f"{symbol}/{member['index']} loss flags preserve full fixture evidence"
    return by_id


def assert_association(connection, sql_dir, summary, by_id):
    events, run = summary["events"], summary["run_id"]
    rows = query(connection, sql_dir, "q2", run_id=run)
    assert len(rows) == 1, 'Q2 must have exactly one association before inspecting it'
    row = rows[0]
    expected = {"decision_event_id": events["D"], "decision_id": summary["decision_id"],
                "dispatch_event_id": events["H"], "operation_id": summary["operation_id"],
                "name": "echo", "dispatched_tool": "echo", "mcp_tool": "echo", "host_action": "execute",
                "status": "ok", "resolution": "remote", "link_basis": "host_hook", "forwarded_changed": 1}
    for key, value in expected.items():
        assert same_typed_value(row[key], value), f'Q2 explicit association {key}'
    for key, value in (("model_arguments", "selected"), ("forwarded_arguments", "executed"), ("mcp_arguments", "executed")):
        assert same_typed_value(decoded(row, key), {'text': value}), f'Q2 {key} preserves its separately observed arguments'
    tokens = [decoded(row, key) for key in ("model_args_digest_json", "host_args_digest_json", "mcp_args_digest_json")]
    for name, token in zip(("model", "host", "mcp"), tokens):
        check_token(token, run, 'args_json_python/1', f'{name} argument token')
    assert same_typed_value(tokens[1]['value'], tokens[2]['value']), 'host and MCP argument originals agree'
    assert tokens[0]['value'] != tokens[1]['value'], 'model and forwarded argument originals differ'
    answers = decoded(row, "answers_json")
    assert len(answers) == 3, 'Q2 must keep all three answer occurrences'
    for answer, symbol, index in zip(answers, ("W0", "W1", "W2"), (2, 3, 4)):
        expected_answer = {"request_event_id": events[symbol], "pointer": f"/messages/{index}/content",
                           "binding_witness_event_id": events["W0"], "comparison_basis": "observed_original", "text_equal": 0}
        for key, value in expected_answer.items():
            assert same_typed_value(answer[key], value), f'{symbol} answer {key}'
        check_token(answer['comparison']['submitted_text_digest'], run, 'text_blocks_json/1', f'{symbol} submitted token')
        assert same_typed_value(answer['comparison']['result_event_id'], events['E']), f'{symbol} result attribution names the observed terminal'
    assert same_typed_value(len({a['message_artifact_id'] for a in answers}), 1), 'all answer occurrences share one immutable message handle'
    assert same_typed_value(len({a['binding_link_id'] for a in answers}), 1), 'all answer occurrences share one first-witness binding'
    result_token = decoded(by_id[events["E"]], "details_json")["result_text_digest"]
    check_token(result_token, run, 'text_blocks_json/1', 'observed result token')
    return answers[0]["message_artifact_id"]


def assert_totals(connection, sql_dir, summary, by_id, handle):
    events, run = summary["events"], summary["run_id"]
    for table, expected in (("runs", 1), ("turns", 2), ("operations", 1), ("events", 12), ("links", 2), ("findings", 0)):
        assert same_typed_value(connection.execute(f'SELECT count(*) FROM {table}').fetchone()[0], expected), f'fixed trace has exactly {expected} {table} rows'
    assert same_typed_value(connection.execute('SELECT count(*) FROM request_members').fetchone()[0], 18), 'request_members counts occurrences, not unique messages'
    occurrences = list(connection.execute("SELECT * FROM answer_occurrences ORDER BY request_sequence,message_index"))
    assert same_typed_value([(r['message_index'], r['association_state']) for r in occurrences], [(2, 'linked'), (3, 'linked'), (4, 'linked')]), 'answer view keeps linked shifted occurrences'
    operation = connection.execute("SELECT * FROM operations WHERE operation_id=?", (summary["operation_id"],)).fetchone()
    assert operation is not None, 'operation row must exist for lifecycle checks'
    expected_op = {"decision_id": summary["decision_id"], "dispatch_event_id": events["H"], "model_call_id": summary["model_calls"]["M0"],
                   "start_event_id": events["S"], "end_event_id": events["E"], "input_rounds": 0, "client_id": "fixture-client"}
    for key, value in expected_op.items():
        assert same_typed_value(operation[key], value), f'operation lifecycle {key}'
    duration = operation["duration_ms"]
    assert duration is not None, 'a finished operation exposes a measured duration'
    assert duration >= 0, 'operation duration cannot be negative'
    assert same_typed_value(duration, by_id[events['E']]['observed_mono_ms'] - by_id[events['S']]['observed_mono_ms']), 'duration is the observed monotonic interval'
    for symbol in ("S", "E"):
        for key in ("decision_id", "model_call_id", "operation_id"):
            assert same_typed_value(by_id[events[symbol]][key], operation[key]), f"{symbol} carries the operation's frozen {key}"
    q3 = query(connection, sql_dir, "q3", run_id=run)
    rounded = connection.execute("SELECT ROUND(?,1)", (duration,)).fetchone()[0]
    assert same_typed_value(q3, [{'server_key': 'fixture-server', 'name': 'echo', 'status': 'ok', 'resolution': 'remote', 'operations': 1, 'measured_durations': 1, 'mean_ms': rounded}]), 'Q3 counts one measured remote echo outcome'
    assert same_typed_value(query(connection, sql_dir, 'q5', run_id=run), [{'method': 'tools/call', 'resolution': 'remote', 'operations': 1}]), 'Q5 counts the logical tools/call resolution once'
    conditions = {"case": "core_acceptance", "variant": "no_tool"}
    key = hashlib.sha256(json.dumps(conditions, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()).hexdigest()
    assert same_typed_value(query(connection, sql_dir, 'q6', run_id=run), [{'turn_id': summary['turns']['T2'], 'condition_key': key, 'capture_state': 'complete', 'host_outcome': 'returned', 'observed_tool_calls': 0, 'count_coverage': 'complete'}]), 'Q6 identifies only the complete no-new-decision turn'
    links = {row["relation"]: row for row in connection.execute("SELECT * FROM links")}
    assert set(links) == {'dispatch_source', 'answers'}, 'both relation rows are required for witness checks'
    dispatch = links["dispatch_source"]
    assert same_typed_value([dispatch['source_event_id'], dispatch['target_event_id'], dispatch['witness_event_id']], [events['D'], events['H'], events['H']]), 'dispatch source and witness preserve explicit host action'
    answer = links["answers"]
    assert same_typed_value([answer['source_event_id'], answer['target_event_id'], answer['witness_event_id'], answer['target_slot'], answer['target_pointer']], [events['D'], events['W0'], events['W0'], 'payload', '/messages/2/content']), 'binding witness is its earliest committed request'
    details = json.loads(answer["details_json"])
    assert same_typed_value([details['binding_format'], details['message_artifact_id'], details['relative_pointer']], ['message_binding/2', handle, '/content']), 'binding fields join relative message evidence'
    run_row = connection.execute("SELECT * FROM runs WHERE run_id=?", (run,)).fetchone()
    assert run_row is not None, 'the run row must exist'
    assert same_typed_value([run_row['close_state'], run_row['dropped_events'], run_row['writer_errors']], ['clean', 0, 0]), 'loss-free completed fixture has a clean run'
    expected_coverage = {"producers": {"model": "fixture.model", "host": "fixture.host", "mcp": "fixture.mcp"}, "model_complete": True}
    assert same_typed_value(json.loads(run_row['environment_json']), {'synthetic': True, 'fixture': 'core_acceptance'}), 'run declares the exact synthetic environment'
    assert same_typed_value(json.loads(run_row['coverage_json']), expected_coverage), 'run fixes its observed producer groups'
    assert run_row['ended_at'] is not None, 'logical run closure retains an end timestamp'
    for turn in connection.execute("SELECT * FROM turns"):
        first_turn = turn["turn_id"] == summary["turns"]["T1"]
        conditions = {"case": "core_acceptance", "variant": "rewrite" if first_turn else "no_tool"}
        prompt = "Echo the selected text." if first_turn else "Use retained context; do not select another tool."
        condition_key = hashlib.sha256(json.dumps(conditions, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()).hexdigest()
        assert same_typed_value(turn['conversation_id'], 'fixture-conversation'), f"turn {turn['turn_id']} preserves its declared conversation"
        assert same_typed_value(body(connection, turn['prompt_ref']), prompt), f"turn {turn['turn_id']} preserves its original prompt"
        assert same_typed_value(json.loads(turn['conditions_json']), conditions), f"turn {turn['turn_id']} preserves its declared conditions"
        assert same_typed_value(turn['condition_key'], condition_key), f"turn {turn['turn_id']} hashes canonical conditions"
        assert same_typed_value(json.loads(turn['coverage_json']), expected_coverage), f"turn {turn['turn_id']} copies fixed coverage"
        assert same_typed_value([turn['capture_state'], turn['host_outcome']], ['complete', 'returned']), f"ended turn {turn['turn_id']} is complete"
        assert turn['ended_at'] is not None, f"turn {turn['turn_id']} has an observed end time"
    assert same_typed_value(summary['close'], {'writer_stopped': True, 'run_close_state': 'clean', 'portable': True, 'reason': None}), 'close reports both logical and portable completion'
    assert same_typed_value(connection.execute('PRAGMA journal_mode').fetchone()[0].lower(), 'delete'), 'portable fixture uses DELETE journal mode'
    assert same_typed_value(connection.execute('PRAGMA user_version').fetchone()[0], 6), 'canonical schema version remains six'
    assert same_typed_value(connection.execute("SELECT count(*) FROM sqlite_master WHERE type='table'").fetchone()[0], 7), 'canonical schema has seven tables'
    assert same_typed_value(connection.execute('PRAGMA foreign_key_check').fetchall(), []), 'no foreign-key violation survives closure'


def accepted(receipt, requirement):
    assert receipt.status == "accepted", f"{requirement}: accepted prerequisite required, got {receipt.status}/{receipt.reason}"
    return receipt


def observe(recorder, kind, payload, **facts):
    return accepted(recorder.observe(kind, scope=recorder.capture_scope(), payload=payload, facts=facts),
                    f"fixture must admit {kind}")


def request(recorder, payload, **facts):
    receipt = observe(recorder, "model.request", payload, **facts)
    observe(recorder, "model.response", {"complete": True}, model_call_id=receipt.model_call_id)
    return receipt


def operation(recorder, **options):
    start = accepted(recorder.start_operation(scope=recorder.capture_scope(), client_id="client",
                     method="tools/call", name="echo", arguments={"text": "same"}, **options),
                     "the logical operation start must exist before completion")
    end = accepted(recorder.finish_operation(start.ticket, status="ok", result="result", text_paths=[""]),
                   "the operation must expose a terminal source for relation checks")
    return start, end


def test_quickstart_runs_from_outside_the_checkout(tmp_path):
    script = tmp_path / "quickstart.py"
    shutil.copyfile(ROOT / "examples/quickstart.py", script)
    database = tmp_path / "new.sqlite"
    process = subprocess.run(
        [sys.executable, "-I", "-B", str(script), str(database)],
        cwd=tmp_path, text=True, capture_output=True, timeout=20,
    )
    assert process.returncode == 0, process.stderr
    assert process.stdout.strip() == str(database)
    with closing(sqlite3.connect(database)) as connection:
        assert connection.execute("SELECT close_state FROM runs").fetchone() == ("clean",)
        assert connection.execute("SELECT count(*) FROM turns").fetchone() == (1,)
        assert connection.execute("SELECT kind FROM events").fetchall() == [("host.tool_result",)]


def test_external_core_example_matches_independent_sql_contract(tmp_path):
    outside = tmp_path / "outside-source"
    outside.mkdir()
    script = outside / "record_core.py"
    shutil.copyfile(ROOT / "tests/producers/record_core.py", script)
    database = outside / "core.sqlite"
    process = subprocess.run([sys.executable, "-I", "-B", str(script), "--output", str(database)],
                             cwd=outside, text=True, capture_output=True)
    assert process.returncode == 0, f"the copied example must execute the installed API outside source: {process.stderr}"
    summary = json.loads(process.stdout)
    check.equal(set(summary["events"]), {"R0", "D", "Z0", "H", "S", "E", "W0", "F0", "W1", "F1", "W2", "F2"},
                msg="the external example must return every specified event symbol")
    # Move only the closed main file into a new directory: the consumer cannot rely on old sidecars.
    lone = tmp_path / "lone-file"
    lone.mkdir()
    copied = lone / "core.sqlite"
    shutil.copyfile(database, copied)
    before = copied.read_bytes()
    entries = sorted(path.name for path in lone.iterdir())
    with closing(sqlite3.connect(copied.resolve().as_uri() + "?mode=ro", uri=True)) as connection:
        connection.row_factory = sqlite3.Row
        by_id = assert_timelines(connection, ROOT / "sql", summary)
        handle = assert_association(connection, ROOT / "sql", summary, by_id)
        assert_totals(connection, ROOT / "sql", summary, by_id, handle)
    assert copied.read_bytes() == before, "Independent SQL checks must not change the recording bytes."
    assert sorted(path.name for path in lone.iterdir()) == entries, "Read-only checks must not create sidecars or other files."


def test_two_source_preparation_and_request_bound_insertions(tmp_path):
    path = tmp_path / "relations.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    try:
        with recorder.turn("combine"):
            _, first = operation(recorder)
            _, second = operation(recorder)
            prepared = observe(recorder, "host.tool_result", "summary", sources=[
                {"event_id": first.event_id}, {"event_id": second.event_id}], text_paths=[""])
            actual = request(recorder, {"messages": [{"role": "user", "content": "summary"}]},
                             messages=[{"bindings": []}], insertions=[{
                                 "source_event_id": prepared.event_id, "target_pointer": "/messages/0/content", "content": "summary"}])
            mismatch = request(recorder, {"messages": [{"content": 1}]}, messages=[{"bindings": []}],
                               insertions=[{"source_event_id": prepared.event_id, "target_pointer": "/messages/0/content", "content": True}])
            absent = request(recorder, {"messages": []}, messages=[], insertions=[{
                "source_event_id": prepared.event_id, "target_pointer": "/missing", "content": "summary"}])
    finally:
        recorder.close()
    with closing(sqlite3.connect(path)) as db:
        db.row_factory = sqlite3.Row
        links = db.execute("SELECT * FROM links ORDER BY relation,source_event_id").fetchall()
        transformed = [row for row in links if row["relation"] == "transformed_from"]
        inserted = [row for row in links if row["relation"] == "inserted_into"]
        check.equal({row["source_event_id"] for row in transformed}, {first.event_id, second.event_id},
                    msg="prepared material preserves both explicitly declared source results")
        check.equal(len(transformed), 2, msg="two source observations create exactly two transformed_from relations")
        check.equal({(row["target_event_id"], row["witness_event_id"]) for row in transformed},
                    {(prepared.event_id, prepared.event_id)}, msg="preparation itself witnesses each source relation")
        assert len(inserted) == 1, "only the actual matching request destination may establish an insertion"
        check.equal((inserted[0]["source_event_id"], inserted[0]["target_event_id"], inserted[0]["witness_event_id"],
                     inserted[0]["target_slot"], inserted[0]["target_pointer"]),
                    (prepared.event_id, actual.event_id, actual.event_id, "payload", "/messages/0/content"),
                    msg="inserted_into is bound to the current submitted request and exact destination")
        for receipt in (mismatch, absent):
            details = json.loads(db.execute("SELECT details_json FROM events WHERE event_id=?", (receipt.event_id,)).fetchone()[0])
            check.is_true(bool(receipt.losses) or any("mismatch" in json.dumps(value) or "absent" in json.dumps(value) or "unresolved" in key
                                                     for key, value in details.items()),
                          msg="mismatched typed value or absent destination must retain an explicit limitation diagnostic")


def test_cache_observations_use_declared_owners_without_inventing_resolution(tmp_path):
    path = tmp_path / "cache.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    try:
        start = accepted(recorder.start_operation(scope=recorder.capture_scope(), client_id="c", method="tools/list", arguments={}),
                         "cache scenario requires an observed logical operation")
        client = observe(recorder, "cache.served", {"tools": []}, owner="client", operation_id=start.operation_id,
                         client_id="c", cache_id="discovery", cache_scope="fixture", ttl_ms=0)
        host = observe(recorder, "cache.served", "host value", owner="host", operation_id=start.operation_id)
        bad_owner = recorder.observe("cache.served", scope=recorder.capture_scope(), payload={}, facts={"owner": "transport"})
        missing_owner = recorder.observe("cache.served", scope=recorder.capture_scope(), payload={})
        accepted(recorder.finish_operation(start.ticket, status="ok", result={"tools": []}),
                 "operation completion remains independent of served-hit observations")
    finally:
        recorder.close()
    for receipt in (bad_owner, missing_owner):
        check.equal((receipt.status, receipt.reason), ("rejected", "invalid_input"),
                    msg="a served hit needs a valid explicit host/client owner")
        check.is_none(receipt.event_id, msg="rejected cache observations cannot publish event identities")
    with closing(sqlite3.connect(path)) as db:
        rows = {row[0]: row[1:] for row in db.execute("SELECT event_id,boundary,source_id FROM events WHERE kind='cache.served'")}
        check.equal(rows, {client.event_id: ("mcp", "test.mcp"), host.event_id: ("host", "test.host")},
                    msg="cache owner maps to its fixed declared producer group")
        check.equal(db.execute("SELECT resolution FROM operations").fetchone()[0], "unknown",
                    msg="a cache observation must not silently rewrite the operation's declared resolution")


def test_propagated_and_unassigned_operations_preserve_explicit_identity(tmp_path):
    path = tmp_path / "attribution.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    try:
        with recorder.turn("two calls"):
            source = observe(recorder, "model.request", {"messages": []}, messages=[])
            decision = observe(recorder, "model.tool_call", {"arguments": {"text": "same"}}, name="echo",
                               model_call_id=source.model_call_id)
            observe(recorder, "model.response", {}, model_call_id=source.model_call_id)
            propagated, _ = operation(recorder, decision_id=decision.decision_id, link_basis="propagated")
            unassigned, _ = operation(recorder, link_basis="none")
            invalid = recorder.start_operation(scope=recorder.capture_scope(), client_id="client", method="tools/call",
                        arguments={"text": "same"}, name="echo", decision_id=decision.decision_id, link_basis="none")
    finally:
        recorder.close()
    check.equal((invalid.status, invalid.reason), ("rejected", "invalid_input"),
                msg="none attribution rejects an explicit decision rather than guessing a correlation")
    check.is_none(invalid.ticket, msg="a contradictory start cannot own a completion reservation")
    with closing(sqlite3.connect(path)) as db:
        db.row_factory = sqlite3.Row
        rows = db.execute((ROOT / "sql/q2.sql").read_text(), {"run_id": recorder.capture_scope().run_id}).fetchall()
        assert len(rows) == 1, "one explicit propagated decision produces one Q2 association"
        check.equal((rows[0]["operation_id"], rows[0]["dispatch_event_id"], rows[0]["link_basis"]),
                    (propagated.operation_id, None, "propagated"), msg="propagated correlation needs no invented dispatch")
        operations = {row["operation_id"]: row for row in db.execute("SELECT * FROM operations")}
        check.equal(len(operations), 2, msg="the invalid third start must leave no operation row")
        check.equal((operations[unassigned.operation_id]["decision_id"], operations[unassigned.operation_id]["dispatch_event_id"]),
                    (None, None), msg="matching tool names and arguments cannot attribute an unassigned operation")
        check.equal(operations[propagated.operation_id]["model_call_id"], source.model_call_id,
                    msg="known decision occurrence supplies its observed model-call attribution")
        for query_name in ("q3", "q5"):
            totals = db.execute((ROOT / f"sql/{query_name}.sql").read_text(), {"run_id": recorder.capture_scope().run_id}).fetchall()
            check.equal(sum(row["operations"] for row in totals), 2,
                        msg=f"{query_name} counts both logical operations regardless of decision assignment")
