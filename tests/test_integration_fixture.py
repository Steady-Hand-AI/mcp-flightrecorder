"""Installed SDK examples against independent SQL and native host expectations."""
from collections import Counter
from contextlib import closing
import hashlib
import importlib.util
from importlib.metadata import version
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys

import pytest
import pytest_check as check

from .fixture_checks import body, check_token, query, reconstruct, same_typed_value


ROOT = Path(__file__).resolve().parents[1]


TRACE = ["L0", "L1", "R0", "D1", "D2", "Z0", "H1", "S1", "H2", "S2",
         "E2", "E1", "P", "W1", "F1", "W2", "F2", "W3", "F3"]
KINDS = ["mcp.request", "mcp.response", "model.request", "model.tool_call", "model.tool_call",
         "model.response", "host.dispatch", "mcp.request", "host.dispatch", "mcp.request",
         "mcp.response", "mcp.response", "host.tool_result", "model.request", "model.response",
         "model.request", "model.response", "model.request", "model.response"]
COVERAGE = {"producers": {"model": "fixture.model", "host": "fixture.host", "mcp": "fixture.mcp"},
            "model_complete": True}
CONDITION_KEYS = {
    "T1": "350e533bc07966f3aa72b68318c590452d306846b121f2cf29c2587ef50faec4",
    "T2": "dcc3b019e5dfcaf7e23456fd6568840868f825d9b93d617bcf0f5b034c704ef4",
}
DEFAULT_LIMITS = {"ordinary_items": 256, "ordinary_bytes": 8388608,
                  "completion_items": 64, "completion_bytes": 262144,
                  "batch_items": 64, "batch_bytes": 1048576, "batch_delay_ms": 20,
                  "committed_cache_items": 512, "busy_timeout_ms": 1000}


def decode(value):
    return None if value is None else json.loads(value)


def expected_request(inputs, name):
    expected = {"model": "fixture-model", "temperature": 0, "n": 1,
                "messages": [inputs["messages.json"][symbol] for symbol in inputs["request-order.json"][name]],
                "tools": inputs["provider-tools.json"], "stream": name == "R0"}
    if name == "R0":
        expected["stream_options"] = {"include_usage": True}
    return expected


def assert_schema_and_totals(connection, inputs):
    expected_tables = ["artifacts", "events", "findings", "links", "operations", "runs", "turns"]
    tables = [row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
    assert tables == expected_tables, 'schema preserves exactly the seven contract tables'
    assert same_typed_value(connection.execute('PRAGMA user_version').fetchone()[0], 6), 'schema user version remains six'
    assert same_typed_value(connection.execute('PRAGMA journal_mode').fetchone()[0].lower(), 'delete'), 'portable database uses DELETE journal mode'
    assert same_typed_value(connection.execute('PRAGMA foreign_key_check').fetchall(), []), 'closed recording has no foreign-key violations'
    assert same_typed_value([row[0] for row in connection.execute('PRAGMA integrity_check')], ['ok']), 'closed recording passes SQLite integrity check'
    counts = inputs["expected-counts.json"]
    for table in ("runs", "turns", "events", "operations", "findings", "request_members", "answer_occurrences"):
        assert same_typed_value(connection.execute(f'SELECT count(*) FROM {table}').fetchone()[0], counts[table]), f'fixed fixture has exactly {counts[table]} {table}'
    actual_kinds = {row[0]: row[1] for row in connection.execute("SELECT kind,count(*) FROM events GROUP BY kind")}
    assert same_typed_value(actual_kinds, dict(Counter(KINDS))), 'all nineteen observations have exactly the specified kind counts'
    actual_links = {row[0]: row[1] for row in connection.execute("SELECT relation,count(*) FROM links GROUP BY relation")}
    assert same_typed_value(actual_links, counts['links']), 'seven links preserve all four exact relation counts'


def assert_artifact_integrity(connection):
    for index, row in enumerate(connection.execute("SELECT * FROM artifacts ORDER BY artifact_id")):
        name = f"retained artifact {index}"
        assert same_typed_value([row['omitted_reason'], row['redacted'], row['truncated']], [None, 0, 0]), f'{name} has no missing, redacted or truncated evidence'
        assert row['body_text'] is not None, f'{name} has a body under content capture'
        encoded = row["body_text"].encode("utf-8")
        assert same_typed_value(row['stored_bytes'], len(encoded)), f'{name} stored byte count matches its body'
        assert same_typed_value(row['stored_sha256'], hashlib.sha256(encoded).hexdigest()), f'{name} stored SHA-256 matches its body'


def assert_timeline(connection, sql, symbols):
    by_symbol = {}
    for turn, names in (("T1", TRACE[:15]), ("T2", TRACE[15:])):
        rows = query(connection, sql, "q1", run_id=symbols["R"], turn_id=symbols[turn])
        assert same_typed_value([row['event_id'] for row in rows], [symbols[name] for name in names]), f'Q1 {turn} has the exact ordered event trace'
        actual = {row["event_id"]: row for row in rows}
        for name in names:
            assert symbols[name] in actual, f'Q1 includes prerequisite event {name}'
            row = actual[symbols[name]]
            by_symbol[name] = row
            kind = KINDS[TRACE.index(name)]
            boundary = kind.split(".")[0]
            assert same_typed_value(row['kind'], kind), f'{name} preserves its specified kind'
            assert same_typed_value([row['authoritative'], row['provenance'], row['event_version']], [1, 'observed', 1]), f'{name} is authoritative observed version-one evidence'
            assert same_typed_value([row['boundary'], row['source_id']], [boundary, COVERAGE['producers'][boundary]]), f'{name} retains configured producer authority'
            assert same_typed_value([row['run_id'], row['turn_id']], [symbols['R'], symbols[turn]]), f'{name} retains its run and turn'
            assert same_typed_value(decode(row['details_json']).get('complete'), True), f'{name} declares complete capture'
    sequence = [by_symbol[name]["sequence"] for name in TRACE]
    assert all((a < b for a, b in zip(sequence, sequence[1:]))), 'writer sequence establishes S1 before S2 before E2 before E1 within the full trace'
    assert same_typed_value(len({symbols[name] for name in TRACE}), 19), 'nineteen event symbols are distinct'
    for names, count in ((["T1", "T2"], 2), (["M0", "M1", "M2", "M3"], 4), (["O0", "O1", "O2"], 3), (["delta1", "delta2"], 2)):
        assert same_typed_value(len({symbols[name] for name in names}), count), f"{','.join(names)} retain distinct occurrence identities"
    return by_symbol


def assert_requests(connection, inputs, symbols, events):
    handles = {}
    for index, name in enumerate(("R0", "W1", "W2", "W3")):
        row = events[name]
        assert same_typed_value(row['model_call_id'], symbols[f'M{index}']), f'{name} retains its model-call identity'
        assert same_typed_value(decode(row['details_json']).get('instance_id'), 'fixture-openai'), f'{name} records the configured model instance'
        assert same_typed_value(row['payload_representation'], 'request_refs/2'), f'{name} uses request_refs/2'
        manifest = decode(row["payload"])
        assert isinstance(manifest, dict), f'{name} manifest is present'
        assert same_typed_value([manifest.get('format'), manifest.get('message_path')], ['request_refs/2', '/messages']), f'{name} manifest format and message path are explicit'
        assert same_typed_value(reconstruct(connection, manifest, name), expected_request(inputs, name)), f'{name} reconstructs exactly the submitted SDK body'
        assert same_typed_value(decode(row['catalog']), inputs['provider-tools.json']), f'{name} retains the exact per-request provider catalog'
        fields = manifest["field_refs"]
        assert same_typed_value([field['pointer'] for field in fields], ['/tools']), f'{name} extracts only its supplied tools field'
        assert same_typed_value(row['catalog_ref'], fields[0]['artifact_id']), f'{name} catalog_ref identifies its tools field'
        order = inputs["request-order.json"][name]
        members = decode(row["request_members_json"])
        assert same_typed_value([member['index'] for member in members], list(range(len(order)))), f'{name} preserves all ordered message indices'
        assert same_typed_value(len(manifest['message_refs']), len(order)), f'{name} manifest preserves every supplied membership'
        for member, message_name in zip(members, order):
            assert same_typed_value(member['pointer'], f"/messages/{member['index']}"), f"{name}/{member['index']} preserves its absolute message pointer"
            assert same_typed_value([member['artifact_missing'], member['omitted_reason'], member['redacted'], member['truncated']], [0, None, 0, 0]), f"{name}/{member['index']} keeps loss-free membership flags"
            if message_name in ("B1", "B2"):
                handles.setdefault(message_name, []).append(member["artifact_id"])
    for name in ("B1", "B2"):
        assert same_typed_value(len(handles.get(name, [])), 3), f'{name} has three submitted memberships'
        assert same_typed_value(len(set(handles[name])), 1), f'{name} reuses one immutable complete message handle'
    assert handles['B1'][0] != handles['B2'][0], 'HB1 and HB2 remain distinct handles'
    return {name: values[0] for name, values in handles.items()}


def assert_responses(inputs, symbols, events):
    assert same_typed_value(decode(events['Z0']['payload']), {'format': 'openai_chat_stream/1', 'chunks': inputs['provider-stream.json']}), 'Z0 preserves precisely the four consumed SDK chunks'
    for index, name in enumerate(("Z0", "F1", "F2", "F3")):
        assert same_typed_value([events[name]['model_call_id'], events[name]['state']], [symbols[f'M{index}'], 'complete']), f'{name} closes its own complete model call'
        if index:
            assert same_typed_value(decode(events[name]['payload']), inputs['provider-normal.json'][f'W{index}']), f'{name} retains its exact normal SDK response'
    for index, reference in ((1, "call-alpha"), (2, "call-beta")):
        row = events[f"D{index}"]
        payload = decode(row["payload"])
        assert isinstance(payload, dict), f'D{index} retains a decision payload'
        expected_raw = inputs["messages.json"]["A"]["tool_calls"][index - 1]["function"]["arguments"]
        assert same_typed_value(payload.get('arguments_raw'), expected_raw), f'D{index} keeps the exact raw selected arguments'
        assert 'arguments' not in payload, f'D{index} does not invent SDK-parsed arguments'
        assert same_typed_value([row['name'], row['model_call_id'], row['decision_id']], ['echo', symbols['M0'], symbols[f'delta{index}']]), f'D{index} keeps its explicit name and occurrence attribution'
        assert same_typed_value(decode(row['details_json']).get('provider_ref'), reference), f'D{index} retains the exposed provider reference'
        assert symbols[f'D{index}'] != symbols[f'delta{index}'], f'D{index} event and decision IDs are not interchanged'
        assert same_typed_value(decode(events[f'H{index}']['details_json']).get('decision_event_ids'), [symbols[f'D{index}']]), f'H{index} declares exactly its one decision-event source'
    discovery = decode(events["L1"]["payload"])
    assert isinstance(discovery, dict) and isinstance(discovery.get('tools'), list), 'L1 retains the real discovery tool list'
    assert same_typed_value(len(discovery['tools']), 1), 'L1 contains exactly the returned single declaration'
    for field, expected in inputs["mcp-tool.json"].items():
        assert same_typed_value(discovery['tools'][0].get(field), expected), f'L1 preserves actual discovery {field}'
    for name, text in (("E1", "alpha-executed"), ("E2", "beta")):
        result = decode(events[name]["payload"])
        assert isinstance(result, dict), f'{name} retains its real returned result'
        assert same_typed_value(result.get('content'), [{'type': 'text', 'text': text}]), f'{name} retains one explicit text block'
        assert same_typed_value(result.get('isError'), False), f'{name} preserves the successful isError flag'
    assert same_typed_value(decode(events['P']['payload']), inputs['messages.json']['S']['content']), 'P retains the explicit two-result prepared string'
    check_token(decode(events['P']['details_json']).get('result_text_digest'), symbols['R'], 'text_blocks_json/1', 'P complete prepared string')


def assert_operations(connection, sql, symbols, events):
    for index in range(3):
        name = f"O{index}"
        operation = connection.execute("SELECT * FROM operations WHERE run_id=? AND operation_id=?", (symbols["R"], symbols[name])).fetchone()
        assert operation is not None, f'{name} has its logical operation row'
        start, end = ("L0", "L1") if index == 0 else (f"S{index}", f"E{index}")
        expected = {"turn_id": symbols["T1"], "model_call_id": None if index == 0 else symbols["M0"],
                    "decision_id": None if index == 0 else symbols[f"delta{index}"],
                    "dispatch_event_id": None if index == 0 else symbols[f"H{index}"],
                    "link_basis": "none" if index == 0 else "host_hook", "client_id": "fixture-client",
                    "server_key": "fixture-server", "method": "tools/list" if index == 0 else "tools/call",
                    "name": None if index == 0 else "echo", "status": "ok", "resolution": "unknown",
                    "input_rounds": None, "error_code": None, "start_event_id": symbols[start], "end_event_id": symbols[end]}
        for field, value in expected.items():
            assert same_typed_value(operation[field], value), f'{name} preserves logical lifecycle {field}'
        arguments = {"cursor": None, "meta": None, "cache_mode": "use"} if index == 0 else {"text": "alpha-executed" if index == 1 else "beta"}
        assert same_typed_value(decode(events[start]['payload']).get('arguments'), arguments), f'{name} retains its exact exposed invocation arguments'
        for event in (start, end):
            for field in ("turn_id", "model_call_id", "decision_id", "operation_id", "client_id", "server_key", "method", "name"):
                assert same_typed_value(events[event][field], operation[field]), f"{event} preserves the operation's frozen {field}"
        duration = operation["duration_ms"]
        assert isinstance(duration, (int, float)) and (not isinstance(duration, bool)), f'{name} has an observed numeric duration'
        assert duration >= 0, f'{name} duration is nonnegative'
        assert same_typed_value(duration, events[end]['observed_mono_ms'] - events[start]['observed_mono_ms']), f'{name} duration is its observed monotonic interval'
        assert operation['started_at'] is not None and operation['ended_at'] is not None, f'{name} has observed start and end timestamps'
    average = connection.execute("SELECT ROUND(AVG(duration_ms),1) FROM operations WHERE method='tools/call'").fetchone()[0]
    assert same_typed_value(query(connection, sql, 'q3', run_id=symbols['R']), [{'server_key': 'fixture-server', 'name': 'echo', 'status': 'ok', 'resolution': 'unknown', 'operations': 2, 'measured_durations': 2, 'mean_ms': average}]), 'Q3 counts exactly the two measured tool calls and excludes discovery'
    assert same_typed_value(query(connection, sql, 'q5', run_id=symbols['R']), [{'method': 'tools/call', 'resolution': 'unknown', 'operations': 2}, {'method': 'tools/list', 'resolution': 'unknown', 'operations': 1}]), 'Q5 separately counts discovery and tool-call unknown resolution'


def assert_associations(connection, sql, symbols, events, handles):
    rows = query(connection, sql, "q2", run_id=symbols["R"])
    assert len(rows) == 2, 'Q2 has exactly two decision-dispatch-operation association rows'
    assert same_typed_value([row['decision_event_id'] for row in rows], [symbols['D1'], symbols['D2']]), 'Q2 orders the two distinct decisions by observation'
    for index, row in enumerate(rows, 1):
        label = f"D{index}"
        expected = {"decision_event_id": symbols[label], "decision_id": symbols[f"delta{index}"],
                    "dispatch_event_id": symbols[f"H{index}"], "operation_id": symbols[f"O{index}"],
                    "name": "echo", "dispatched_tool": "echo", "mcp_tool": "echo", "host_action": "execute",
                    "status": "ok", "resolution": "unknown", "link_basis": "host_hook", "model_arguments": None,
                    "forwarded_changed": 1 if index == 1 else 0}
        for field, value in expected.items():
            assert same_typed_value(row[field], value), f'Q2 {label} explicit association {field}'
        actual_arguments = {"text": "alpha-executed" if index == 1 else "beta"}
        for field in ("forwarded_arguments", "mcp_arguments"):
            assert same_typed_value(decode(row[field]), actual_arguments), f'Q2 {label} {field} is the decoded argument object'
        for prefix in ("model", "host", "mcp"):
            assert same_typed_value([row[f'{prefix}_omitted_reason'], row[f'{prefix}_redacted'], row[f'{prefix}_truncated']], [None, 0, 0]), f'Q2 {label} {prefix} arguments have no retention loss'
        tokens = [check_token(decode(row[field]), symbols['R'], 'args_json_python/1', f'{label} {name} arguments')
                  for field, name in (("model_args_digest_json", "model"), ("host_args_digest_json", "host"), ("mcp_args_digest_json", "MCP"))]
        assert same_typed_value(tokens[1]['value'], tokens[2]['value']), f'{label} host and MCP original argument tokens agree'
        assert same_typed_value(tokens[0]['value'] == tokens[1]['value'], index == 2), f'{label} original model-to-host argument equality is applicable'
        answers = decode(row["answers_json"])
        assert isinstance(answers, list) and len(answers) == 3, f'Q2 {label} keeps all three answer occurrences'
        result_token = check_token(decode(events[f'E{index}']['details_json']).get('result_text_digest'), symbols['R'], 'text_blocks_json/1', f'{label} MCP result')
        for request, position, answer in zip(("W1", "W2", "W3"), ((3, 4, 4) if index == 1 else (4, 5, 5)), answers):
            expected_answer = {"request_event_id": symbols[request], "pointer": f"/messages/{position}/content",
                               "message_artifact_id": handles[f"B{index}"], "binding_witness_event_id": symbols["W1"],
                               "comparison_basis": "observed_original", "text_equal": 1 if index == 1 else 0}
            for field, value in expected_answer.items():
                assert same_typed_value(answer[field], value), f'Q2 {label}/{request} answer {field}'
            comparison = answer["comparison"]
            assert same_typed_value(comparison.get('result_event_id'), symbols[f'E{index}']), f'{label}/{request} explicitly identifies the actual MCP terminal'
            token = check_token(comparison.get('submitted_text_digest'), symbols['R'], 'text_blocks_json/1', f'{label}/{request} submitted text')
            assert same_typed_value(result_token['value'] == token['value'], index == 1), f'{label}/{request} actual compatible text tokens establish the expected equality'
        assert same_typed_value(len({answer['binding_link_id'] for answer in answers}), 1), f'{label} reuses one first-witness answers link'


def assert_links_and_occurrences(connection, symbols, handles):
    rows = [dict(row) for row in connection.execute("SELECT * FROM links")]
    expected = [("dispatch_source", "D1", "H1", "H1"), ("dispatch_source", "D2", "H2", "H2"),
                ("answers", "D1", "W1", "W1"), ("answers", "D2", "W1", "W1"),
                ("transformed_from", "E1", "P", "P"), ("transformed_from", "E2", "P", "P"),
                ("inserted_into", "P", "W1", "W1")]
    actual_keys = sorted((row["relation"], row["source_event_id"], row["target_event_id"], row["witness_event_id"]) for row in rows)
    assert same_typed_value(actual_keys, sorted(((kind, symbols[source], symbols[target], symbols[witness]) for kind, source, target, witness in expected))), 'all seven links have exactly the explicit sources, targets and witnesses'
    by_key = {(row["relation"], row["source_event_id"]): row for row in rows}
    for index in (1, 2):
        key = ("answers", symbols[f"D{index}"])
        assert key in by_key, f'D{index} has its original answers binding'
        row = by_key[key]
        details = decode(row["details_json"])
        assert same_typed_value([row['target_slot'], row['target_pointer']], ['payload', f'/messages/{index + 2}/content']), f'D{index} binding names its first submitted destination'
        assert same_typed_value([details.get('binding_format'), details.get('message_artifact_id'), details.get('relative_pointer')], ['message_binding/2', handles[f'B{index}'], '/content']), f'D{index} binding keeps immutable message-relative identity'
        key = ("transformed_from", symbols[f"E{index}"])
        assert key in by_key, f'P has its explicit E{index} source link'
        source = by_key[key]
        assert same_typed_value([source['source_slot'], source['source_pointer']], ['payload', '/content/0/text']), f'P source E{index} selects the original result text'
    key = ("inserted_into", symbols["P"])
    assert key in by_key, 'W1 has the explicitly witnessed prepared-summary insertion'
    insertion = by_key[key]
    assert same_typed_value([insertion['source_slot'], insertion['source_pointer'], insertion['target_slot'], insertion['target_pointer']], ['payload', '', 'payload', '/messages/5/content']), 'summary insertion connects the actual source root and submitted destination'
    occurrences = [dict(row) for row in connection.execute("SELECT * FROM answer_occurrences ORDER BY request_sequence,message_index,target_pointer")]
    expected_rows = []
    for request, positions in (("W1", (3, 4)), ("W2", (4, 5)), ("W3", (4, 5))):
        for index, position in enumerate(positions, 1):
            expected_rows.append((symbols[request], symbols[f"D{index}"], position, f"/messages/{position}/content", handles[f"B{index}"], "linked"))
    assert same_typed_value([(row['request_event_id'], row['source_event_id'], row['message_index'], row['target_pointer'], row['message_artifact_id'], row['association_state']) for row in occurrences], expected_rows), 'answer_occurrences expands all six linked retained-history positions'


def assert_run_and_turns(connection, sql, inputs, summary, sdk_versions):
    symbols = summary["symbols"]
    run = connection.execute("SELECT * FROM runs WHERE run_id=?", (symbols["R"],)).fetchone()
    assert run is not None, 'summary R identifies the recorded run'
    assert same_typed_value([run['close_state'], run['dropped_events'], run['writer_errors']], ['clean', 0, 0]), 'run closure is clean with no dropped events or writer errors'
    assert run['ended_at'] is not None, 'run has an observed closing timestamp'
    assert same_typed_value(decode(run['coverage_json']), COVERAGE), 'run declares all three fixed producers and complete model visibility'
    policy = decode(run["policy_json"])
    assert same_typed_value([policy.get('capture'), policy.get('comparisons')], ['content', True]), 'run explicitly uses content capture and original comparisons'
    assert same_typed_value(policy.get('limits'), DEFAULT_LIMITS), 'canonical run uses the unchanged default capture limits'
    assert same_typed_value(policy.get('projection_max_bytes'), 1048576), 'canonical run retains the original 1 MiB projection cap'
    metadata = decode(run["environment_json"])
    for key, expected in (("fixture", "phase2_integration"), ("provider_backend", "scripted_sdk_transport"), ("mcp_backend", "in_process_sdk_server"), ("sdk_versions", sdk_versions)):
        assert same_typed_value(metadata.get(key), expected), f'run environment declares observed {key}'
    assert same_typed_value(metadata.get('capture_boundaries'), summary['boundaries']), 'run and summary declare the same capture boundaries'
    for name, prompt_symbol, variant in (("T1", "U", "parallel_rewrite"), ("T2", "U2", "history_no_tool")):
        turn = connection.execute("SELECT * FROM turns WHERE run_id=? AND turn_id=?", (symbols["R"], symbols[name])).fetchone()
        assert turn is not None, f'{name} exists for original ingress checks'
        assert same_typed_value(body(connection, turn['prompt_ref'], f'{name} prompt'), inputs['messages.json'][prompt_symbol]['content']), f'{name} retains its original host prompt'
        assert same_typed_value(turn['conversation_id'], 'fixture-conversation'), f'{name} preserves its conversation ID'
        assert same_typed_value(decode(turn['conditions_json']), {'case': 'phase2_integration', 'variant': variant}), f'{name} preserves its declared conditions'
        assert same_typed_value(turn['condition_key'], CONDITION_KEYS[name]), f'{name} preserves its independent fixed condition key'
        assert same_typed_value(decode(turn['coverage_json']), COVERAGE), f'{name} copies complete declared producer coverage'
        assert same_typed_value([turn['host_outcome'], turn['capture_state']], ['returned', 'complete']), f'{name} ends normally with complete capture'
        assert turn['ended_at'] is not None, f'{name} has its observed end timestamp'
    assert same_typed_value(query(connection, sql, 'q6', run_id=symbols['R']), [{'turn_id': symbols['T2'], 'condition_key': CONDITION_KEYS['T2'], 'capture_state': 'complete', 'host_outcome': 'returned', 'observed_tool_calls': 0, 'count_coverage': 'complete'}]), 'Q6 returns exactly the complete zero-new-decision T2'
    assert same_typed_value(summary['close'], {'writer_stopped': True, 'run_close_state': 'clean', 'portable': True, 'reason': None}), 'producer close reports joined owner and portable clean recording'


def assert_host_observations(inputs, summary, events, sdk_versions):
    host, backend = summary["host"], summary["backend"]
    assert same_typed_value(summary['sdk_versions'], sdk_versions), 'summary reports the actually installed SDK versions'
    for key, expected in (("model", "submitted SDK body arguments and consumed SDK results/chunks"),
                          ("mcp", "logical Client.list_tools and Client.call_tool arguments/results"),
                          ("provider_backend", "scripted_sdk_transport"), ("mcp_backend", "in_process_sdk_server")):
        assert same_typed_value(summary['boundaries'].get(key), expected), f'summary identifies the supported {key} boundary'
    assert same_typed_value(summary['boundaries'].get('unobserved'), ['wire requests', 'hidden SDK retries/rounds', 'notifications', 'cache source']), 'summary leaves hidden SDK observations unclaimed'
    assert same_typed_value(summary['boundaries'].get('outside_coverage'), ['live provider', 'HTTP/stdio MCP transports', 'other SDKs/platforms']), 'summary separates unsupported live and remote transports'
    assert same_typed_value(host.get('assembled_assistant'), inputs['messages.json']['A']), 'host independently assembled A from the consumed tool fragments'
    assert same_typed_value(host.get('stream_chunks'), inputs['provider-stream.json']), 'host observed the exact ordered native chunks'
    assert same_typed_value(host.get('provider_catalog'), inputs['provider-tools.json']), 'host built the provider catalog from real discovery'
    assert same_typed_value(decode(events['L1']['payload']), host.get('discovery')), 'retained discovery equals the actual host-visible SDK result'
    assert same_typed_value([decode(events[name]['payload']) for name in ('E1', 'E2')], host.get('tool_results')), 'retained MCP terminals equal the two actual host-visible returns'
    assert same_typed_value(host.get('prepared_summary'), inputs['messages.json']['S']['content']), 'host prepared the explicit two-result summary'
    assert same_typed_value(host.get('summary_source_order'), ['alpha', 'beta']), 'host supplies E1 then E2 as the prepared-summary source order'
    assert same_typed_value(host.get('responses'), inputs['provider-normal.json']), 'host observed the three exact ordinary completions'
    assert same_typed_value(host.get('overlap_before_release'), {'entered': ['alpha', 'beta'], 'released': []}), 'both actual tool handlers entered while neither result gate was released'
    assert same_typed_value(backend.get('provider_requests'), [expected_request(inputs, name) for name in ('R0', 'W1', 'W2', 'W3')]), 'backend observed four exact submitted provider bodies'
    assert same_typed_value(backend.get('mcp_list_calls'), 1), 'backend handled one real discovery'
    assert same_typed_value(backend.get('mcp_tool_calls'), [{'name': 'echo', 'arguments': {'text': 'alpha-executed'}}, {'name': 'echo', 'arguments': {'text': 'beta'}}]), 'backend received the rewritten alpha and unchanged beta calls'
    assert same_typed_value(backend.get('operation_control_order'), ['enter:alpha', 'enter:beta', 'release:beta', 'return:beta', 'release:alpha', 'return:alpha']), 'real gates establish overlapping starts and beta-before-alpha returns'


def file_hash(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture(scope="module")
def integration_recording(tmp_path_factory):
    """Each worker creates its own immutable native-SDK recording outside the checkout."""
    directory = tmp_path_factory.mktemp("external-integration")
    script = directory / "minimal_host.py"
    shutil.copyfile(ROOT / "tests/producers/minimal_host.py", script)
    shutil.copytree(ROOT / "tests/fixtures/integration", directory / "inputs")
    database = directory / "integration.sqlite"
    process = subprocess.run(
        [sys.executable, "-I", "-B", str(script), "--output", str(database), "--inputs", str(directory / "inputs")],
        cwd=directory, capture_output=True, text=True, timeout=50,
    )
    assert process.returncode == 0, f"The external installed-SDK example must complete before its persisted fixture can be inspected: {process.stderr}"
    try:
        summary = json.loads(process.stdout)
    except json.JSONDecodeError as error:
        pytest.fail(f"The external example must emit its real symbol/host/closure summary as JSON: {error}")
    assert summary.get("close", {}).get("portable") is True, "The canonical recording must be portable before tests copy only its main database."
    assert summary["close"].get("writer_stopped") is True, "The producer must join the owner before any test consumes or copies the recording."
    return database, summary


def copy_recording(source, destination):
    database = destination / "fixture.sqlite"
    shutil.copyfile(source, database)
    return database


def test_external_sdk_example_matches_independent_sql_contract(integration_recording, tmp_path):
    sdk_versions = {
        name: version(name)
        for name in ("openai", "mcp", "mcp-types", "httpx2")
    }
    source, summary = integration_recording
    database = copy_recording(source, tmp_path)
    before = {path.name: file_hash(path) for path in tmp_path.iterdir()}
    input_directory = ROOT / "tests/fixtures/integration"
    inputs = {
        name: json.loads((input_directory / name).read_text(encoding="utf-8"))
        for name in (
            "expected-counts.json", "mcp-tool.json", "messages.json", "provider-normal.json",
            "provider-stream.json", "provider-tools.json", "request-order.json",
        )
    }
    sql = ROOT / "sql"
    with closing(sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True)) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute("BEGIN")
        assert_schema_and_totals(connection, inputs)
        assert_artifact_integrity(connection)
        events = assert_timeline(connection, sql, summary["symbols"])
        handles = assert_requests(connection, inputs, summary["symbols"], events)
        assert_responses(inputs, summary["symbols"], events)
        assert_operations(connection, sql, summary["symbols"], events)
        assert_associations(connection, sql, summary["symbols"], events, handles)
        assert_links_and_occurrences(connection, summary["symbols"], handles)
        assert_run_and_turns(connection, sql, inputs, summary, sdk_versions)
        assert_host_observations(inputs, summary, events, sdk_versions)
    after = {path.name: file_hash(path) for path in tmp_path.iterdir()}
    assert after == before, "Fresh read-only checks must leave the lone database unchanged and create no sidecars."


def test_canonical_host_and_backends_match_native_no_recorder_control(integration_recording, run_sdk_async):
    spec = importlib.util.spec_from_file_location("canonical_host_control", ROOT / "tests/producers/minimal_host.py")
    assert spec is not None and spec.loader is not None, "The shipped example must be loadable by its file path without changing the installed package import boundary."
    example = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(example)
    native = run_sdk_async(example.run_host(ROOT / "tests/fixtures/integration", recorder=None), timeout=20)
    _, recorded = integration_recording
    check.equal(native["host"], recorded["host"], msg="Recording must preserve the actual native host's assembled choices, returned SDK values, catalog and overlap controls.")
    check.equal(native["backend"], recorded["backend"], msg="Recording must preserve provider JSON, real MCP call counts/arguments and the controlled backend order.")
    check.equal(native["sdk_versions"], recorded["sdk_versions"], msg="The native/decorated comparison must exercise the same installed SDK profile.")
    check.equal(native["symbols"], {}, msg="The no-recorder control must not manufacture capture identities or use recorded rows to run the host.")


def test_external_example_refuses_existing_recording(integration_recording, tmp_path):
    source, _ = integration_recording
    database = copy_recording(source, tmp_path)
    before = file_hash(database)
    process = subprocess.run(
        [sys.executable, "-I", "-B", str(ROOT / "tests/producers/minimal_host.py"), "--output", str(database),
         "--inputs", str(ROOT / "tests/fixtures/integration")],
        cwd=tmp_path, capture_output=True, text=True, timeout=20,
    )
    check.not_equal(process.returncode, 0, msg="The example must honor the core's refusal to overwrite an existing recording.")
    check.is_in("FileExistsError", process.stderr, msg="The refusal must originate from the fresh-path contract, not an unrelated SDK or fixture failure.")
    check.equal(file_hash(database), before, msg="A repeated example invocation must preserve every byte of the existing recording.")
