"""Installed query contract: SQLite values, authorization, bounds and ownership."""
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

from mcp_flightrecorder import Recorder, inspect

from .recordings import core_recording


@pytest.fixture
def query_recording(tmp_path):
    path = tmp_path / "query ? #.sqlite"
    source, _ = core_recording()
    shutil.copyfile(source, path)
    return path


def _encoded(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")


def _data(result):
    assert result["ok"], "The selected valid query must succeed before its result can be inspected."
    check.equal(result["operation"], "query", msg="SQL uses the public Python operation name in every envelope.")
    check.equal(result["scope"], {}, msg="Arbitrary SELECT does not imply a selected run scope.")
    check.is_true(result["untrusted_content"], msg="Explicit SQL output remains untrusted recorded content.")
    return result["data"]


def _error(result, code, reason=None):
    assert isinstance(result.get("error"), dict), "An expected refusal must expose its structured machine-readable error."
    check.is_false(result["ok"], msg="Rejected SQL must never masquerade as a completed query.")
    check.is_none(result["data"], msg="Failures and timeouts must not return partially fetched data.")
    check.equal(result["error"]["code"], code, msg="The refusal must distinguish argument, SQL, storage and deadline errors.")
    if reason is not None:
        check.equal(result["error"]["reason"], reason, msg="The named negative control must reach its intended refusal branch.")
    check.less_equal(len(_encoded(result)), 1024, msg="Every expected error fits the independent 1024-byte error cap.")


def _fingerprint(path):
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as connection:
        populations = {name: connection.execute("SELECT count(*) FROM " + name).fetchone()[0]
                       for name in ("runs", "turns", "events", "operations", "links", "artifacts", "findings")}
        bodies = connection.execute("SELECT artifact_id,body_text FROM artifacts ORDER BY artifact_id").fetchall()
    return hashlib.sha256(path.read_bytes()).hexdigest(), populations, hashlib.sha256(_encoded(bodies)).hexdigest()


@pytest.mark.parametrize("sql,params,columns,rows", [
    ("SELECT NULL AS n,'Aé中😀Z' AS t,42 AS i,1.25 AS f", None,
     ["n", "t", "i", "f"], [[None, "Aé中😀Z", 42, 1.25]]),
    ("SELECT ? AS v,? AS v,? AS v", [None, "bound", 3], ["v", "v", "v"], [[None, "bound", 3]]),
    ("SELECT ?,?,?,?,?", (True, False, -(2 ** 63), 2 ** 63 - 1, 2.5),
     ["?", "?", "?", "?", "?"], [[1, 0, -(2 ** 63), 2 ** 63 - 1, 2.5]]),
    ("SELECT :text AS a,:integer AS b", {"text": "named", "integer": 7}, ["a", "b"], [["named", 7]]),
    (" \n-- leading comment\n/* SELECT is below */ WITH x(v) AS (SELECT 9) SELECT v FROM x", None,
     ["v"], [[9]]),
    ("SELECT 1 AS repeated,2 AS repeated WHERE 0", None, ["repeated", "repeated"], []),
    ("SELECT ? AS literal", ["x'; DROP TABLE runs; --"], ["literal"], [["x'; DROP TABLE runs; --"]]),
    ("SELECT hex(?) AS h,typeof(?) AS t,length(?) AS n", [b"\x00\xff", b"\x00\xff", b"\x00\xff"],
     ["h", "t", "n"], [["00FF", "blob", 2]]),
], ids=["sqlite-scalars", "positional-duplicates", "tuple-bool-int64-real", "named", "commented-cte",
        "empty-columns", "bound-sql-looking-text", "explicit-blob-conversion"])
def test_query_scalar_and_binding_values(query_recording, sql, params, columns, rows):
    before = _fingerprint(query_recording)
    data = _data(inspect.query(query_recording, sql, params))
    check.equal(data, {"columns": columns, "rows": rows, "returned_rows": len(rows), "has_more": False},
                msg="The independent SQL oracle preserves labels, row arrays and native finite scalar types.")
    check.equal(_fingerprint(query_recording), before, msg="Even SQL-looking bound text cannot modify source rows or content.")


def test_documented_response_usage_sql_api_and_installed_cli(tmp_path):
    documented_sql = """
SELECT sequence, event_id, model_call_id,
       json_extract(details_json, '$.usage.prompt_tokens') AS prompt_tokens,
       json_extract(details_json, '$.usage.cached_tokens') AS cached_tokens,
       json_extract(details_json, '$.usage.cache_write_tokens') AS cache_write_tokens,
       CASE
         WHEN json_type(details_json, '$.usage.prompt_tokens') = 'integer'
          AND json_type(details_json, '$.usage.cached_tokens') = 'integer'
          AND json_extract(details_json, '$.usage.prompt_tokens') > 0
          AND json_extract(details_json, '$.usage.cached_tokens') >= 0
          AND json_extract(details_json, '$.usage.cached_tokens')
              <= json_extract(details_json, '$.usage.prompt_tokens')
         THEN 1.0 * json_extract(details_json, '$.usage.cached_tokens')
              / json_extract(details_json, '$.usage.prompt_tokens')
       END AS read_fraction
FROM observed_events
WHERE run_id = ? AND kind = 'model.response' AND sequence > ?
ORDER BY sequence
"""
    guide = (Path(__file__).resolve().parents[1] / "docs" / "integrations.md").read_text(encoding="utf-8")
    assert 'fr sql "run.sqlite" "' + documented_sql + '" --params \'["RUN_ID", -1]\' --limit 100' in guide, \
        "Exercise the literal user-facing recipe, including its existing type and nonnegative guards."

    path = tmp_path / "response usage ? #.sqlite"
    recorder = Recorder(path, capture="metadata",
        coverage={"producers": {"model": "query.usage", "host": "query.host", "mcp": None}, "model_complete": True})
    run_id = recorder.capture_scope().run_id
    responses = []
    cases = [
        ({"prompt_tokens": 10000, "cached_tokens": 8000, "cache_write_tokens": 1000}, [10000, 8000, 1000, 0.8]),
        ({"prompt_tokens": 10, "cached_tokens": 8}, [10, 8, None, 0.8]),
        ({"prompt_tokens": 10, "cached_tokens": 0, "cache_write_tokens": 0}, [10, 0, 0, 0.0]),
        (None, [None, None, None, None]),
        ({"prompt_tokens": 0, "cached_tokens": 0, "cache_write_tokens": 0}, [0, 0, 0, None]),
        ({"prompt_tokens": 10, "cached_tokens": 11, "cache_write_tokens": 1}, [10, 11, 1, None]),
        ({"prompt_tokens": 20, "cache_write_tokens": 3}, [20, None, 3, None]),
        ({"cached_tokens": 4, "cache_write_tokens": 2}, [None, 4, 2, None]),
    ]
    try:
        with recorder.turn("response usage query") as turn:
            assert turn.receipt.status == "accepted"
            requests = []
            for _ in range(2):
                request = recorder.observe("model.request", scope=recorder.capture_scope(),
                    payload={"messages": []}, facts={"messages": []})
                assert request.status == "accepted"
                requests.append(request)
            # Responses finish in reverse request order; two distinct responses share the first call ID.
            calls = [requests[1].model_call_id, requests[0].model_call_id, requests[0].model_call_id]
            for index, (usage, _) in enumerate(cases):
                if index < len(calls):
                    call_id = calls[index]
                else:
                    request = recorder.observe("model.request", scope=recorder.capture_scope(),
                        payload={"messages": []}, facts={"messages": []})
                    assert request.status == "accepted"
                    call_id = request.model_call_id
                facts = {"model_call_id": call_id}
                if usage is not None:
                    facts["usage"] = usage
                response = recorder.observe("model.response", scope=recorder.capture_scope(),
                    payload={"content": "unretained usage response"}, facts=facts)
                assert response.status == "accepted"
                responses.append(response)
    finally:
        closed = recorder.close()
    assert closed.writer_stopped and closed.portable
    assert closed.run_close_state == "clean"

    before = _fingerprint(path)
    full = inspect.query(path, documented_sql, [run_id, -1], limit=100)
    data = _data(full)
    check.is_none(full["error"], msg="The literal query must complete successfully on finalized public capture.")
    check.equal(full["cutoffs"], [], msg="All eight observed responses fit the documented first-page limit.")
    check.equal(data["columns"], ["sequence", "event_id", "model_call_id", "prompt_tokens", "cached_tokens",
                                  "cache_write_tokens", "read_fraction"],
                msg="The recipe exposes the three independent counts and only the guarded derived fraction.")
    check.equal([row[1:] for row in data["rows"]],
                [[response.event_id, response.model_call_id, *values]
                 for response, (_, values) in zip(responses, cases)],
                msg="Response order, repeated call IDs, absent usage/writes, zero and invalid fractions remain distinct.")
    sequences = [row[0] for row in data["rows"]]
    assert len(sequences) == len(cases) and sequences == sorted(set(sequences)) and sequences[0] > -1
    check.equal((data["returned_rows"], data["has_more"]), (len(cases), False),
                msg="The unbounded fixture page retains even a response with no usage facts.")
    check.equal(_data(inspect.query(path, documented_sql, ["other-run", -1]))["rows"], [],
                msg="Run selection is an explicit binding, not inferred from whichever response is latest.")

    fr_executable = Path(sys.executable).with_name("fr")
    assert fr_executable.is_file(), "Install the candidate in the test environment first"
    cwd = tmp_path / "usage-cli-cwd"
    cwd.mkdir()
    after_sequence = -1
    for expected_rows in (data["rows"][:2], data["rows"][2:4]):
        params = [run_id, after_sequence]
        page = inspect.query(path, documented_sql, params, limit=2)
        page_data = _data(page)
        check.is_none(page["error"], msg="Only a successful page supplies its continuation sequence.")
        check.equal(page_data["rows"], expected_rows, msg="The continuation selects the next observed response events.")
        check.equal((page_data["returned_rows"], page_data["has_more"]), (2, True),
                    msg="The two-row pages report the existing bounded lookahead.")
        check.equal([cutoff["code"] for cutoff in page["cutoffs"]], ["item_limit"],
                    msg="A successful limited page retains the existing item-limit cutoff.")
        argv = [str(fr_executable), "sql", str(path), documented_sql,
                "--params", json.dumps(params), "--limit", "2"]
        process = subprocess.run(argv, cwd=cwd, capture_output=True, text=True, timeout=20)
        assert process.returncode == 0, process.stderr
        assert process.stdout, "Installed fr sql must return the successful page's JSON envelope."
        cli = json.loads(process.stdout)
        check.equal(process.stderr, "", msg="The successful recipe writes no diagnostic traceback.")
        check.equal(process.stdout, _encoded(cli).decode() + "\n", msg="The recipe retains the existing single JSON CLI envelope.")
        check.equal(cli, page, msg="The installed CLI executes the literal recipe with identical values and continuation.")
        assert page_data["rows"], "Continuation requires an actual last returned sequence."
        after_sequence = page_data["rows"][-1][0]
    check.equal(_fingerprint(path), before, msg="Public and CLI usage inspection leave the finalized recording unchanged.")


@pytest.mark.parametrize("expression,expected", [
    ("abs(-2)", 2), ("avg(2)", 2.0), ("count(*)", 1), ("sum(3)", 3), ("total(3)", 3.0),
    ("min(2,3)", 2), ("max(2,3)", 3), ("round(1.25,1)", 1.3), ("coalesce(NULL,2)", 2),
    ("ifnull(NULL,3)", 3), ("nullif(3,3)", None), ("length('abc')", 3), ("lower('ABC')", "abc"),
    ("upper('abc')", "ABC"), ("substr('abc',2,1)", "b"), ("substring('abc',2,1)", "b"),
    ("trim(' x ')", "x"), ("ltrim(' x ')", "x "), ("rtrim(' x ')", " x"),
    ("replace('aba','a','z')", "zbz"), ("instr('abc','b')", 2), ("printf('v=%d',7)", "v=7"),
    ("format('v=%d',7)", "v=7"), ("typeof(NULL)", "null"), ("hex(X'00FF')", "00FF"),
    ("quote('it''s')", "'it''s'"), ("unicode('é')", 233), ("char(65)", "A"),
    ("like('a%','abc')", 1), ("glob('a*','abc')", 1), ("json('{\"a\": 1}')", '{"a":1}'),
    ("json_valid('{\"a\":1}')", 1), ("json_extract('{\"a\":2}','$.a')", 2),
    ("json_type('{\"a\":2}','$.a')", "integer"), ("json_array_length('[1,2]')", 2),
    ("json_array(1,'x')", '[1,"x"]'), ("json_object('a',2)", '{"a":2}'),
    ("json_group_array(2)", "[2]"), ("json_group_object('a',2)", '{"a":2}'),
    ("'{\"a\":2}' -> '$.a'", "2"), ("'{\"a\":2}' ->> '$.a'", 2), ("ABS(-4)", 4),
], ids=["abs", "avg", "count", "sum", "total", "min", "max", "round", "coalesce", "ifnull", "nullif",
        "length", "lower", "upper", "substr", "substring", "trim", "ltrim", "rtrim", "replace", "instr",
        "printf", "format", "typeof", "hex", "quote", "unicode", "char", "like", "glob", "json", "json_valid",
        "json_extract", "json_type", "json_array_length", "json_array", "json_object", "json_group_array",
        "json_group_object", "json-arrow", "json-arrow-text", "case-insensitive"])
def test_query_exact_builtin_allowlist(query_recording, expression, expected):
    data = _data(inspect.query(query_recording, "SELECT " + expression + " AS value"))
    check.equal(data["rows"], [[expected]], msg="Every individually named permitted builtin remains usable under authorization.")


@pytest.mark.parametrize("source,expected", [("observed_events", 12), ("request_members", 18),
                                             ("answer_occurrences", 3), ("sqlite_master WHERE type='table'", 7)],
                         ids=["observed-events", "json-request-members", "json-answer-occurrences", "schema-metadata"])
def test_query_shipped_views_and_schema_metadata(query_recording, source, expected):
    data = _data(inspect.query(query_recording, "SELECT count(*) AS n FROM " + source))
    check.equal(data["rows"], [[expected]], msg="The unchanged core oracle must be readable through its shipped JSON-backed views.")


@pytest.mark.parametrize("sql,params", [
    ("SELECT X'00FF'", None), ("SELECT ?", [b"blob"]), ("SELECT 1e999", None),
    ("SELECT -1e999", None), ("SELECT 1 AS v UNION ALL SELECT X'01'", None),
], ids=["literal-blob", "bound-blob", "positive-infinity", "negative-infinity", "bad-later-retained-cell"])
def test_query_unsupported_result_rejects_entire_response(query_recording, sql, params):
    _error(inspect.query(query_recording, sql, params), "sql_rejected", "unsupported_sql_value")


def test_query_does_not_validate_unretained_lookahead_value(query_recording):
    result = inspect.query(query_recording, "SELECT 1 AS v UNION ALL SELECT X'01'", limit=1)
    data = _data(result)
    check.equal(data["rows"], [[1]], msg="Only the retained prefix is subject to JSON value conversion.")
    check.is_true(data["has_more"], msg="A single extra row establishes continuation without retaining its BLOB value.")
    check.equal([item["code"] for item in result["cutoffs"]], ["item_limit"],
                msg="The bounded lookahead must report the exact row-cap cutoff.")


DENIALS = [
    ("SELECT 1; SELECT 2", None, "statement_or_binding"),
    ("INSERT INTO runs(run_id) VALUES('bad')", None, "statement_not_select"),
    ("UPDATE runs SET dropped_events=99", None, "statement_not_select"),
    ("DELETE FROM runs", None, "statement_not_select"),
    ("CREATE TABLE forbidden(x)", None, "statement_not_select"),
    ("DROP TABLE runs", None, "statement_not_select"),
    ("WITH x(v) AS (SELECT 1) UPDATE runs SET dropped_events=99", None, "statement_denied"),
    ("ATTACH DATABASE ':memory:' AS other", None, "statement_not_select"),
    ("PRAGMA user_version", None, "statement_not_select"),
    ("BEGIN", None, "statement_not_select"), ("COMMIT", None, "statement_not_select"),
    ("ROLLBACK", None, "statement_not_select"), ("SAVEPOINT forbidden", None, "statement_not_select"),
    ("VALUES(1)", None, "statement_not_select"), ("EXPLAIN SELECT 1", None, "statement_not_select"),
    ("SELECT load_extension('untrusted-sentinel-extension')", None, "statement_denied"),
    ("SELECT random()", None, "statement_denied"), ("SELECT date('now')", None, "statement_denied"),
    ("SELECT * FROM pragma_user_version", None, "statement_denied"),
    ("SELECT FROM runs", None, "statement_denied"),
    ("SELECT ?", [], "statement_or_binding"), ("SELECT 1", [1], "statement_or_binding"),
    ("SELECT :needed", {"wrong": 1}, "statement_or_binding"), ("/* unclosed", None, "statement_not_select"),
]
DENIAL_IDS = ["multiple-statements", "insert", "update", "delete", "create", "drop", "write-cte", "attach",
              "pragma", "begin", "commit", "rollback", "savepoint", "values", "explain", "extension-loading",
              "unlisted-random", "unlisted-date", "table-valued-pragma", "syntax", "missing-positional-binding",
              "extra-positional-binding", "missing-named-binding", "unterminated-leading-comment"]


@pytest.mark.parametrize("sql,params,reason", DENIALS, ids=DENIAL_IDS)
def test_query_denial_python_cli_and_unchanged_source(query_recording, tmp_path, sql, params, reason):
    before = _fingerprint(query_recording)
    result = inspect.query(query_recording, sql, params)
    _error(result, "sql_rejected", reason)
    cwd = tmp_path / "empty-cli-directory"
    cwd.mkdir()
    command = [str(Path(sys.executable).with_name("fr")), "sql", str(query_recording), sql]
    if params is not None:
        command.extend(["--params", json.dumps(params)])
    process = subprocess.run(command, cwd=cwd, capture_output=True, text=True, timeout=10)
    assert process.stdout.strip(), "An expected installed CLI denial must still emit a JSON result."
    cli = json.loads(process.stdout)
    check.equal(process.returncode, 1, msg="SQL rejection has the pinned expected-failure exit status, not a storage exit.")
    check.equal(process.stderr, "", msg="Expected SQL denial must not leak a traceback or SQLite diagnostic.")
    check.equal(process.stdout, _encoded(cli).decode() + "\n", msg="The CLI writes exactly one compact JSON value and newline.")
    check.equal(cli, result, msg="Installed CLI and direct API must expose the same named refusal.")
    check.equal(_fingerprint(query_recording), before, msg="Each rejected statement leaves all source rows and content hashes unchanged.")


@pytest.mark.parametrize("params", ["text", b"bytes", 3, [object()], [[1]], {1: "value"},
                                             [2 ** 63], [-(2 ** 63) - 1], [float("nan")], [float("inf")],
                                             {"x": {"nested": 1}}, ["\ud800"], {"\ud800": 1}],
                         ids=["string-container", "bytes-container", "scalar-container", "object-cell", "nested-array",
                              "nonstring-key", "int64-overflow", "int64-underflow", "nan", "infinity", "nested-object",
                              "invalid-utf8-value", "invalid-utf8-key"])
def test_query_invalid_parameter_shapes_precede_database_access(tmp_path, params):
    _error(inspect.query(tmp_path / "absent.sqlite", "SELECT ?", params), "invalid_request", "invalid_params")


@pytest.mark.parametrize("options", [{"limit": True}, {"limit": 0}, {"limit": 1001}, {"limit": 1.0},
                                              {"timeout": True}, {"timeout": 0}, {"timeout": -1}, {"timeout": 31},
                                              {"timeout": float("nan")}, {"timeout": float("inf")},
                                              {"max_output_bytes": True}, {"max_output_bytes": 1023},
                                              {"max_output_bytes": 1_048_577}, {"no_content": 1}],
                         ids=["boolean-limit", "zero-limit", "over-limit", "float-limit", "boolean-timeout",
                              "zero-timeout", "negative-timeout", "over-timeout", "nan-timeout", "infinite-timeout",
                              "boolean-budget", "small-budget", "large-budget", "nonboolean-policy"])
def test_query_invalid_options_precede_content_policy_and_storage(tmp_path, options):
    request = {"no_content": True, **options}
    _error(inspect.query(tmp_path / "absent.sqlite", "SELECT 1", **request), "invalid_request")


@pytest.mark.parametrize("sql", [None, b"SELECT 1", 1, "SELECT '\ud800'"],
                         ids=["null-sql", "bytes-sql", "integer-sql", "invalid-utf8-sql"])
def test_query_invalid_sql_shape_precedes_content_and_storage(tmp_path, sql):
    _error(inspect.query(tmp_path / "absent.sqlite", sql, no_content=True), "invalid_request", "invalid_sql")


def test_query_no_content_rejects_before_sql_or_file_processing(tmp_path):
    _error(inspect.query(tmp_path / "absent.sqlite", "DROP TABLE secret_sentinel", no_content=True),
           "content_disabled", "no_content")


def test_query_sql_byte_limit_exact_and_one_over(query_recording):
    prefix, suffix = "SELECT 7 /*", "*/"
    remaining = 65_536 - len((prefix + suffix).encode())
    sql = prefix + "é" * (remaining // 2) + "x" * (remaining % 2) + suffix
    assert len(sql.encode("utf-8")) == 65_536, "The control must independently hit the exact UTF-8 SQL input boundary."
    check.equal(_data(inspect.query(query_recording, sql))["rows"], [[7]], msg="Exactly 65536 SQL bytes remain permitted.")
    _error(inspect.query(query_recording, sql + " "), "invalid_request", "sql_input_limit")


@pytest.mark.parametrize("limit,extra", [(1, 0), (1, 1), (200, 0), (200, 1), (1000, 0), (1000, 1)],
                         ids=["one-exact", "one-over", "default-exact", "default-over", "maximum-exact", "maximum-over"])
def test_query_exact_row_caps_and_bounded_lookahead(query_recording, limit, extra):
    result = inspect.query(query_recording,
        "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<?) SELECT x FROM n",
        [limit + extra], limit=limit, max_output_bytes=1_048_576)
    data = _data(result)
    check.equal(data["rows"], [[index] for index in range(1, limit + 1)], msg="The fetched result is the exact deterministic retained prefix.")
    check.equal(data["returned_rows"], limit, msg="returned_rows counts emitted rows without inventing the unscanned total.")
    check.equal(data["has_more"], bool(extra), msg="One lookahead row distinguishes an exact cap from overflow.")
    check.equal([item["code"] for item in result["cutoffs"]], ["item_limit"] if extra else [],
                msg="Only a known extra row creates an item-limit cutoff.")
    check.equal(set(data), {"columns", "rows", "returned_rows", "has_more"}, msg="Arbitrary SQL never claims an exact total population.")


@pytest.mark.parametrize("extra", [0, 1], ids=["512-bytes", "513-bytes"])
def test_query_display_strings_and_labels_use_utf8_cap(query_recording, extra):
    label = "é" * 256 + "Z" * extra
    result = inspect.query(query_recording, 'SELECT ? AS "' + label + '",? AS artifact_id', [label, label])
    data = _data(result)
    check.equal(data["columns"], ["é" * 256, "artifact_id"], msg="Column labels are UTF-8 display strings with the same 512-byte cap.")
    check.equal(data["rows"], [["é" * 256, "é" * 256]], msg="An alias named artifact_id must not grant arbitrary SQL cells identity semantics.")
    check.equal(len(result["cutoffs"]), 3 if extra else 0, msg="Each over-cap displayed string gets its own counted cutoff.")
    if extra:
        check.equal({entry["path"] for entry in result["cutoffs"]}, {"/columns/0", "/rows/0/0", "/rows/0/1"},
                    msg="String cutoff pointers resolve to the actual emitted labels and cells.")


def test_query_output_exact_byte_cap_and_one_byte_less(query_recording):
    sql = "WITH RECURSIVE n(x) AS (SELECT 0 UNION ALL SELECT x+1 FROM n WHERE x<7) SELECT x,:text FROM n"
    params = {"text": 'Aé中😀"\\\n' * 12}
    full = inspect.query(query_recording, sql, params)
    full_data = _data(full)
    size = len(_encoded(full))
    assert size > 1024, "The escaped multibyte control must exceed the minimum legal output budget."
    exact = inspect.query(query_recording, sql, params, max_output_bytes=size)
    check.equal(exact, full, msg="The exact compact UTF-8 envelope byte cap permits the complete result.")
    smaller = inspect.query(query_recording, sql, params, max_output_bytes=size - 1)
    data = _data(smaller)
    check.less_equal(len(_encoded(smaller)), size - 1, msg="Encoding must include JSON escapes, multibyte text and diagnostics in its byte budget.")
    check.less(data["returned_rows"], len(full_data["rows"]), msg="One byte below the full response removes complete trailing rows.")
    check.equal(data["rows"], full_data["rows"][:data["returned_rows"]], msg="Output trimming preserves an intact ordered prefix.")
    check.is_true(data["has_more"], msg="Byte-trimmed rows make continuation truthful even when the row cap did not fire.")
    check.is_in("byte_limit", [item["code"] for item in smaller["cutoffs"]], msg="Encoded-output trimming is explicitly reported.")


def test_query_required_row_cannot_fit_and_diagnostic_cap_is_honest(query_recording):
    _error(inspect.query(query_recording, "SELECT ?,?,?", ["x" * 512] * 3, max_output_bytes=1024),
           "limit_exceeded", "required_output")
    result = inspect.query(query_recording,
        "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<25) SELECT ? FROM n",
        ["é" * 256 + "Z"], max_output_bytes=1_048_576)
    data = _data(result)
    check.equal(data["returned_rows"], 25, msg="Diagnostic caps do not silently drop otherwise fitting query rows.")
    check.equal(len(result["cutoffs"]), 20, msg="The public diagnostic list has its documented fixed cap.")
    check.equal(result["diagnostics_omitted"]["cutoffs"], 5, msg="Every omitted per-cell cutoff is counted exactly.")


def test_query_value_limit_preserves_below_cap_analysis(query_recording):
    before = _fingerprint(query_recording)
    result = inspect.query(query_recording,
        "SELECT length(hex(printf('%*s',2097152,'x'))) AS n", max_output_bytes=1024)
    check.equal(_data(result), {"columns": ["n"], "rows": [[4194304]], "returned_rows": 1, "has_more": False},
                msg="A four-MiB intermediate remains usable with the independent one-KiB output budget.")
    check.equal(_fingerprint(query_recording), before, msg="Below-cap analysis preserves the source recording.")


@pytest.mark.parametrize("max_output_bytes", [1024, 1048576], ids=["min-output", "max-output"])
def test_query_value_limit_real_error_api_cli(query_recording, tmp_path, max_output_bytes):
    before = _fingerprint(query_recording)
    sql = "SELECT length(hex(printf('%*s',4194305,'x'))) AS n"
    result = inspect.query(query_recording, sql, max_output_bytes=max_output_bytes)
    _error(result, "limit_exceeded", "sql_value_limit")
    check.equal(result["error"]["message"],
                "The SQL value or encoded row exceeds the 8,388,608-byte query limit.",
                msg="Native too-big errors expose the fixed safe query-limit message.")
    cwd = tmp_path / "empty-cli-directory"
    cwd.mkdir()
    command = [str(Path(sys.executable).with_name("fr")), "sql", str(query_recording), sql,
               "--max-output-bytes", str(max_output_bytes)]
    process = subprocess.run(command, cwd=cwd, capture_output=True, text=True, timeout=10)
    assert process.stdout.strip(), "The installed CLI must emit the structured native value-limit refusal."
    cli = json.loads(process.stdout)
    check.equal(process.returncode, 1, msg="The SQL value limit uses the existing request-failure CLI exit.")
    check.equal(process.stderr, "", msg="The expected value-limit refusal emits no traceback or native diagnostic.")
    check.equal(process.stdout, _encoded(cli).decode() + "\n", msg="The CLI emits exactly one compact JSON value and newline.")
    check.equal(cli, result, msg="Installed API and CLI agree independently of the requested output budget.")
    check.equal(_fingerprint(query_recording), before, msg="A real native too-big error preserves source evidence.")
    check.equal(_data(inspect.query(query_recording, "SELECT 42"))["rows"], [[42]],
                msg="A refused query leaves the same recording usable by a fresh query.")


@pytest.mark.parametrize("function", ["printf", "format"])
def test_query_value_limit_preserves_native_format_outcome(query_recording, function):
    before = _fingerprint(query_recording)
    sql = "SELECT " + function + "('%*s',8388609,'x') AS v"
    # SQLite versions/builds differ: oversized formatting may yield SQL NULL
    # or SQLITE_TOOBIG. Establish this interpreter's native outcome independently.
    native_too_big = False
    with closing(sqlite3.connect(":memory:")) as native:
        native.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, 8_388_608)
        try:
            native_row = native.execute(sql).fetchone()
        except sqlite3.DataError as error:
            assert error.sqlite_errorcode == sqlite3.SQLITE_TOOBIG
            native_too_big = True
        else:
            assert native_row == (None,), "Successful oversized native formatting must return SQL NULL."
    result = inspect.query(query_recording, sql)
    if native_too_big:
        _error(result, "limit_exceeded", "sql_value_limit")
    else:
        check.equal(_data(result), {"columns": ["v"], "rows": [[None]], "returned_rows": 1, "has_more": False},
                    msg="Native formatting NULL remains a successful SQL value without claiming a large string was produced.")
        check.is_none(result["error"], msg="A successful native NULL does not manufacture a value-limit error.")
        check.equal(result["cutoffs"], [], msg="A native NULL has no displayed string to clip.")
    check.equal(_data(inspect.query(query_recording, "SELECT NULL AS v"))["rows"], [[None]],
                msg="An explicit SQL NULL remains a successful JSON null on every SQLite build.")
    check.equal(_data(inspect.query(query_recording, "SELECT " + function + "('%s','valid')"))["rows"], [["valid"]],
                msg="Below-cap formatting remains available after the oversized native outcome.")
    check.equal(_data(inspect.query(query_recording, "SELECT 42"))["rows"], [[42]],
                msg="Either documented native formatting outcome leaves ordinary analysis usable.")
    check.equal(_fingerprint(query_recording), before, msg="Native formatting preserves the source recording.")


def test_query_value_limit_keeps_large_recording_and_other_readers_usable(tmp_path):
    path = tmp_path / "large-query.sqlite"
    recorder = Recorder(
        path, capture="content",
        coverage={"producers": {"model": None, "host": "query.large", "mcp": None}},
        limits={"ordinary_bytes": 16_777_216},
    )
    run_id = recorder.capture_scope().run_id
    try:
        with recorder.turn("x" * 8_388_609) as turn:
            assert turn.receipt.status == "accepted"
            turn_id = turn.receipt.turn_id
    finally:
        closed = recorder.close()
    assert closed.writer_stopped and closed.portable
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as raw:
        prompt = raw.execute(
            "SELECT prompt_ref FROM turns WHERE run_id=? AND turn_id=?",
            (run_id, turn_id),
        ).fetchone()
        assert prompt is not None and prompt[0] is not None
        artifact_id = prompt[0]
        body = raw.execute(
            "SELECT length(body_text),stored_bytes,omitted_reason,truncated "
            "FROM artifacts WHERE run_id=? AND artifact_id=?",
            (run_id, artifact_id),
        ).fetchone()
        assert body == (8_388_611, 8_388_611, None, 0)
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    check.equal(_data(inspect.query(path, "SELECT count(*) AS n FROM artifacts"))["rows"], [[1]],
                msg="The genuine retained large artifact still permits a small population query.")
    check.equal(_data(inspect.query(path, "SELECT dropped_events,writer_errors FROM runs"))["rows"], [[0, 0]],
                msg="Separate small recording metadata remains readable with no capture loss.")
    for sql in ("SELECT body_text FROM artifacts", "SELECT length(body_text) FROM artifacts",
                "SELECT substr(body_text,1,4) FROM artifacts"):
        result = inspect.query(path, sql)
        _error(result, "limit_exceeded", "sql_value_limit")
        check.equal(result["error"]["message"],
                    "The SQL value or encoded row exceeds the 8,388,608-byte query limit.",
                    msg="Reading an oversized stored value uses the same native too-big envelope.")
    assert inspect.runs(path)["ok"], "The value cap must remain local to arbitrary SQL."
    payload = inspect.payload(path, run_id, artifact_id, max_bytes=4)
    assert payload["ok"], "The separate payload reader must expose its ordinary retained-byte window."
    check.equal(payload["data"]["text"], '"xxx', msg="The payload prefix preserves the exact retained JSON bytes.")
    check.equal(payload["data"]["returned_bytes"], 4, msg="Payload returns only the requested four-byte window.")
    check.equal(payload["data"]["total_bytes"], 8388611, msg="The unchanged payload route sees the full retained body length.")
    check.equal(payload["data"]["next_offset"], 4, msg="The retained prefix has an exact continuation byte offset.")
    check.equal(_data(inspect.query(path, "SELECT 42"))["rows"], [[42]],
                msg="A later query owns a fresh usable connection after all large-value refusals.")
    check.equal(hashlib.sha256(path.read_bytes()).hexdigest(), before,
                msg="All readers leave the stopped source byte-identical.")


def test_query_cooperative_timeout_has_watchdog_and_allows_reuse(query_recording, tmp_path):
    before = _fingerprint(query_recording)
    code = '''import json,sys
from mcp_flightrecorder import inspect
expensive="WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<1000000000) SELECT sum(x) FROM n"
expired=inspect.query(sys.argv[1],expensive,timeout=0.02)
ordinary=inspect.query(sys.argv[1],"SELECT count(*) AS n FROM events")
print(json.dumps({"expired":expired,"ordinary":ordinary},ensure_ascii=False,allow_nan=False))
'''
    process = subprocess.run([sys.executable, "-I", "-B", "-c", code, str(query_recording)],
                             cwd=tmp_path, capture_output=True, text=True, timeout=15)
    assert process.returncode == 0, "The bounded child watchdog must complete the expensive query and subsequent ordinary read."
    response = json.loads(process.stdout)
    _error(response["expired"], "query_timeout")
    check.equal(_data(response["ordinary"])["rows"], [[12]], msg="Timeout cleanup leaves the installed reader and same file reusable.")
    check.equal(process.stderr, "", msg="A cooperative timeout is an expected structured result without a traceback.")
    check.equal(_fingerprint(query_recording), before, msg="The interrupted recursive query preserves source rows and exact content hashes.")


@pytest.mark.parametrize("outcome", ["success", "denial", "unsupported", "keyboard", "system-exit",
                                     "value-limit", "expired-value-limit"])
def test_query_connection_statement_and_transaction_cleanup(query_recording, monkeypatch, outcome):
    before = _fingerprint(query_recording)
    original_connect = sqlite3.connect
    owners = []
    value_sql = "SELECT 7 AS v UNION ALL SELECT 8 UNION ALL SELECT length(hex(printf('%*s',4194305,'x'))) AS n"
    value_limit = outcome in {"value-limit", "expired-value-limit"}
    clock, native_rows, native_errors = [10.0], [], []

    class TrackedCursor(sqlite3.Cursor):
        def execute(self, sql, parameters=()):
            self.statement = sql
            return super().execute(sql, parameters)

        def fetchone(self):
            if self.statement == "SELECT 41" and outcome in {"keyboard", "system-exit"}:
                raise KeyboardInterrupt() if outcome == "keyboard" else SystemExit(17)
            if value_limit and self.statement == value_sql:
                try:
                    row = super().fetchone()
                except sqlite3.Error as error:
                    code = getattr(error, "sqlite_errorcode", 0) & 255
                    native_errors.append(code)
                    if outcome == "expired-value-limit" and code == 18:
                        clock[0] = 11.0
                    raise
                if row is not None:
                    native_rows.append(tuple(row))
                return row
            return super().fetchone()

        def close(self):
            if self.statement in {"SELECT 41", "SELECT X'01'", value_sql}:
                self.connection.statement_closed_with_authorizer = self.connection.authorization is not None
            return super().close()

    class TrackedConnection(sqlite3.Connection):
        def execute(self, sql, parameters=()):
            self.statements.append(sql)
            return self.cursor(factory=TrackedCursor).execute(sql, parameters)

        def set_authorizer(self, callback):
            self.authorization = callback
            return super().set_authorizer(callback)

        def close(self):
            self.active_at_close = self.in_transaction
            self.closed = True
            return super().close()

    def connect(database, **options):
        connection = original_connect(database, factory=TrackedConnection, **options)
        connection.statements, connection.authorization, connection.closed = [], None, False
        connection.uri, connection.statement_closed_with_authorizer = database, None
        owners.append(connection)
        return connection

    sql = {"denial": "SELECT random()", "unsupported": "SELECT X'01'"}.get(outcome, "SELECT 41")
    if value_limit:
        sql = value_sql
    with monkeypatch.context() as patch:
        patch.setattr(sqlite3, "connect", connect)
        if value_limit:
            patch.setattr(time, "monotonic", lambda: clock[0])
        if outcome in {"keyboard", "system-exit"}:
            # Host cancellation must propagate after resource cleanup, rather than becoming a JSON success/error.
            with pytest.raises(KeyboardInterrupt if outcome == "keyboard" else SystemExit):
                inspect.query(query_recording, sql)
        else:
            result = inspect.query(query_recording, sql, **({"timeout": 0.5} if value_limit else {}))
            if outcome == "success":
                check.equal(_data(result)["rows"], [[41]], msg="The instrumented connection still executes real SQLite work.")
            elif outcome == "value-limit":
                _error(result, "limit_exceeded", "sql_value_limit")
                check.equal(result["error"]["message"],
                            "The SQL value or encoded row exceeds the 8,388,608-byte query limit.",
                            msg="A real stepping failure uses the same fixed message as a preparation failure.")
            elif outcome == "expired-value-limit":
                _error(result, "query_timeout", "deadline_expired")
            else:
                _error(result, "sql_rejected")
    if value_limit:
        check.equal(native_rows, [(7,)], msg="The real cursor returned one native row before the later step failed.")
        check.equal(native_errors, [18], msg="Both cases must catch the actual SQLite too-big error before translation.")
    assert len(owners) == 1, "Each public query must own exactly one fresh SQLite connection."
    owner = owners[0]
    check.is_true(owner.closed, msg="The owned source connection closes on success, SQL error and both host interruption paths.")
    check.is_false(owner.active_at_close, msg="The short owned read transaction ends before its connection closes.")
    check.is_in("?mode=ro", owner.uri, msg="The source opens through an escaped read-only URI.")
    check.is_not_in("immutable", owner.uri, msg="The reader must not suppress WAL behavior with immutable mode.")
    check.is_in("%3F", owner.uri, msg="A literal question mark in a local filename must be escaped, not interpreted as URI syntax.")
    check.is_in("PRAGMA query_only=ON", owner.statements, msg="The owned connection sets query_only before processing caller SQL.")
    check.is_in("BEGIN", owner.statements, msg="Related query reads share one explicit short read transaction.")
    if outcome != "denial":
        check.is_true(owner.statement_closed_with_authorizer, msg="The authorizer stays active through final cursor cleanup.")
    check.equal(_data(inspect.query(query_recording, "SELECT 42"))["rows"], [[42]], msg="Every exit path permits a fresh subsequent query.")
    check.equal(_fingerprint(query_recording), before, msg="Read success, invalid SQL and host interruption preserve all source evidence.")


def test_query_one_response_uses_coherent_wal_snapshot(query_recording, monkeypatch):
    original_connect = sqlite3.connect
    with closing(original_connect(query_recording)) as writer:
        writer.execute("PRAGMA journal_mode=WAL")
        writer.commit()
        changed = []
        sql = "SELECT dropped_events FROM runs UNION ALL SELECT dropped_events FROM runs"

        class InterleavedCursor(sqlite3.Cursor):
            def execute(self, statement, parameters=()):
                self.is_control = statement == sql
                return super().execute(statement, parameters)

            def fetchone(self):
                value = super().fetchone()
                if self.is_control and value is not None and not changed:
                    writer.execute("UPDATE runs SET dropped_events=7")
                    writer.commit()
                    changed.append(True)
                return value

        class InterleavedConnection(sqlite3.Connection):
            def execute(self, statement, parameters=()):
                return self.cursor(factory=InterleavedCursor).execute(statement, parameters)

        with monkeypatch.context() as patch:
            patch.setattr(sqlite3, "connect", lambda *args, **kwargs:
                          original_connect(*args, factory=InterleavedConnection, **kwargs))
            first = inspect.query(query_recording, sql)
        check.equal(_data(first)["rows"], [[0], [0]], msg="A concurrent controlled WAL commit cannot mix old and new facts within one response.")
        check.equal(changed, [True], msg="The control must actually commit between two result fetches.")
        check.equal(_data(inspect.query(query_recording, "SELECT dropped_events FROM runs"))["rows"], [[7]],
                    msg="The next public call owns a fresh snapshot and observes the independently committed change.")
        check.equal(writer.execute("PRAGMA journal_mode").fetchone()[0], "wal", msg="Read-only inspection leaves the source journal mode unchanged.")
