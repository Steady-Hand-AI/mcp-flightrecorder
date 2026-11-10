"""Installed fr/module/API parity, strict errors and exact-cardinality selection."""
from contextlib import closing
import importlib.metadata
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys

import pytest
import pytest_check as check

from mcp_flightrecorder import inspect

from .recordings import integration_recording


@pytest.fixture
def command_recording(tmp_path):
    path = tmp_path / "input ? #.sqlite"
    source, symbols = integration_recording()
    shutil.copyfile(source, path)
    cwd = tmp_path / "outside-checkout"
    cwd.mkdir()
    return path, symbols, cwd


def _argv(entry):
    return [str(Path(sys.executable).with_name("fr"))] if entry == "fr" else [sys.executable, "-I", "-B", "-m", "mcp_flightrecorder"]


def _command(entry, args, cwd):
    process = subprocess.run(_argv(entry) + [str(arg) for arg in args], cwd=cwd,
                             capture_output=True, text=True, timeout=20)
    assert process.stdout, "Every ordinary installed command must return one JSON result, including expected failures."
    result, end = json.JSONDecoder().raw_decode(process.stdout)
    check.equal(process.stdout[end:], "\n", msg="Only one terminal newline follows the single CLI JSON value.")
    check.equal(process.stdout, json.dumps(result, ensure_ascii=False, allow_nan=False, separators=(",", ":")) + "\n",
                msg="The installed CLI emits compact finite UTF-8 JSON without progress text.")
    check.equal(process.stderr, "", msg="Expected CLI results must not contaminate stderr with tracebacks or parser prose.")
    check.equal(set(result), {"format", "operation", "ok", "scope", "data", "unknowns", "cutoffs",
                             "diagnostics_omitted", "error", "untrusted_content"},
                msg="CLI and Python expose precisely the versioned envelope keys.")
    check.equal(result["format"], "mfr.inspect/1", msg="All installed routes use the assigned inspection format.")
    check.is_true(result["untrusted_content"], msg="Successful and failed CLI output retains its untrusted-content marker.")
    return process, result


def _refusal(process, result, operation, code="invalid_request", exit_code=1, reason=None):
    assert isinstance(result.get("error"), dict), "The intended negative control must return a structured public error."
    check.equal(process.returncode, exit_code, msg="The expected-failure command must use its specifically assigned exit status.")
    check.equal(result["operation"], operation, msg="Parser and operation failures retain the assigned Python operation name.")
    check.equal(result["error"]["code"], code, msg="The named negative control must reach the intended error category.")
    if reason is not None:
        check.equal(result["error"]["reason"], reason, msg="This refusal must preserve its pinned, machine-readable reason.")
    check.is_false(result["ok"], msg="Expected command failures cannot claim successful inspection.")
    check.is_none(result["data"], msg="A failed command cannot emit partial successful data.")
    check.less_equal(len(process.stdout[:-1].encode("utf-8")), 1024, msg="Parser and operational errors remain within the independent error byte cap.")


@pytest.mark.parametrize("entry", ["fr", "module"])
@pytest.mark.parametrize("command", ["schema", "runs", "check", "turns", "explain", "payload", "slice", "sql"])
def test_installed_entrypoints_match_public_python(command_recording, entry, command):
    path, symbols, cwd = command_recording
    common = ["--max-output-bytes", "1048576"]
    if command == "schema":
        args, expected = ["schema"], inspect.schema()
    elif command == "runs":
        args = ["runs", path, "--limit", "1"] + common
        expected = inspect.runs(path, limit=1, max_output_bytes=1_048_576)
    elif command == "check":
        args = ["check", path, "--run", symbols["R"], "--expect", "model,host,mcp"] + common
        expected = inspect.check(path, symbols["R"], ["model", "host", "mcp"], max_output_bytes=1_048_576)
    elif command == "turns":
        filters = {"condition_key": "350e533bc07966f3aa72b68318c590452d306846b121f2cf29c2587ef50faec4",
                   "host_outcome": "returned", "capture_state": "complete", "tool_error": False,
                   "text_mismatch": True, "unattributed_operation": False, "forwarded_change": True}
        args = ["turns", path, "--run", symbols["R"], "--condition-key", filters["condition_key"],
                "--host-outcome", "returned", "--capture-state", "complete", "--tool-error", "false",
                "--text-mismatch", "true", "--unattributed-operation", "false", "--forwarded-change", "true",
                "--limit", "1"] + common
        expected = inspect.find_turns(path, symbols["R"], filters=filters, limit=1, max_output_bytes=1_048_576)
    elif command == "explain":
        args = ["explain", path, "--run", symbols["R"], "--turn", symbols["T1"], "--model-call", symbols["M0"]] + common
        expected = inspect.explain(path, symbols["R"], symbols["T1"], symbols["M0"], max_output_bytes=1_048_576)
    elif command == "payload":
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as connection:
            artifact = connection.execute("SELECT prompt_ref FROM turns WHERE turn_id=?", (symbols["T1"],)).fetchone()[0]
        args = ["payload", path, artifact, "--run", symbols["R"], "--offset", "0", "--max-bytes", "4"] + common
        expected = inspect.payload(path, symbols["R"], artifact, 0, 4, max_output_bytes=1_048_576)
    elif command == "slice":
        args = ["slice", path, symbols["R0"], "--run", symbols["R"], "--limit", "1000", "--max-depth", "12"] + common
        expected = inspect.slice(path, symbols["R"], symbols["R0"], limit=1000, max_depth=12, max_output_bytes=1_048_576)
    else:
        sql, params = "SELECT :text AS name,:number AS value", {"text": "Aé中😀Z", "number": 17}
        args = ["sql", path, sql, "--params", json.dumps(params), "--limit", "200", "--timeout", "10"] + common
        expected = inspect.query(path, sql, params, limit=200, timeout=10, max_output_bytes=1_048_576)
    assert expected["ok"], "Each parity case must first be a successful independent public Python invocation."
    process, result = _command(entry, args, cwd)
    check.equal(process.returncode, 0, msg="Canonical assigned command success has exit zero in both installed entry points.")
    check.equal(result, expected, msg="Installed console/module routes and Python must expose identical scope, data and diagnostics.")
    check.is_false((cwd / "sql").exists(), msg="Command resources must work without a checkout SQL directory.")
    check.is_false((cwd / "scripts").exists(), msg="Assigned installed commands must not depend on sibling reference scripts.")


@pytest.mark.parametrize("entry", ["fr", "module"])
@pytest.mark.parametrize("case", ["sql-denial", "unknown-run", "missing-file", "content-disabled"])
def test_installed_entrypoint_errors_match_public_python(command_recording, entry, case):
    path, symbols, cwd = command_recording
    if case == "sql-denial":
        args = ["sql", path, "SELECT random()"]
        expected = inspect.query(path, "SELECT random()")
    elif case == "unknown-run":
        args = ["turns", path, "--run", "absent-run"]
        expected = inspect.find_turns(path, "absent-run")
    elif case == "missing-file":
        absent = cwd / "absent.sqlite"
        args = ["runs", absent]
        expected = inspect.runs(absent)
    else:
        args = ["sql", path, "SELECT 'secret-sentinel'", "--no-content"]
        expected = inspect.query(path, "SELECT 'secret-sentinel'", no_content=True)
    assert not expected["ok"], "Each error parity control must independently reach the expected public API refusal."
    process, result = _command(entry, args, cwd)
    check.equal(result, expected, msg="Both installed executable forms must preserve the exact Python error envelope and reason.")
    check.equal(process.returncode, 3 if case == "missing-file" else 1,
                msg="CLI error parity preserves the distinction between ordinary refusal and unreadable storage.")


@pytest.mark.parametrize("entry", ["fr", "module"])
@pytest.mark.parametrize("options,needle", [(["--help"], "snapshot"), (["sql", "--help"], "--params"),
                                             (["--version"], "0.5.20")], ids=["help", "command-help", "version"])
def test_installed_help_and_version_remain_human_text(tmp_path, entry, options, needle):
    process = subprocess.run(_argv(entry) + options, cwd=tmp_path, capture_output=True, text=True, timeout=10)
    check.equal(process.returncode, 0, msg="Explicit help/version is successful human-facing discovery.")
    check.is_in(needle, process.stdout, msg="Human discovery contains the requested assigned command or exact version.")
    check.is_false(process.stdout.lstrip().startswith("{"), msg="Help/version follows the separate text convention instead of a JSON result.")
    check.equal(process.stderr, "", msg="Help/version does not emit an error traceback.")


CLI_ERRORS = [
    ([], "schema"), (["unassigned-command"], "schema"), (["--no-content", "runs", "DB"], "schema"),
    (["runs"], "runs"), (["runs", "DB", "--text"], "runs"), (["check", "DB", "--acceptance"], "check"),
    (["sql", "DB", "SELECT 1", "--params", "{"], "query"),
    (["sql", "DB", "SELECT 1", "--params", "null"], "query"),
    (["sql", "DB", "SELECT 1", "--params", "[NaN]"], "query"),
    (["sql", "DB", "SELECT 1", "--params", "[{}]"], "query"),
    (["sql", "DB", "SELECT 1", "--params", '{"x":9223372036854775808}'], "query"),
    (["runs", "DB", "--limit", "true"], "runs"), (["runs", "DB", "--limit", "0"], "runs"),
    (["turns", "DB", "--tool-error", "yes"], "find_turns"),
    (["turns", "DB", "--capture-state", "invented"], "find_turns"),
    (["turns", "DB", "--host-outcome", ""], "find_turns"),
    (["turns", "DB", "--condition-key", ""], "find_turns"),
    (["turns", "DB", "--max-output-bytes", "1023"], "find_turns"),
    (["turns", "DB", "--max-output-bytes", "1048577"], "find_turns"),
    (["check", "DB", "--expect", ""], "check"), (["check", "DB", "--expect", "model,model"], "check"),
    (["check", "DB", "--expect", "model,network"], "check"),
    (["payload", "DB", "artifact", "--offset", "-1"], "payload"),
    (["payload", "DB", "artifact", "--max-bytes", "3"], "payload"),
    (["slice", "DB", "event", "--max-depth", "13"], "slice"),
    (["explain", "DB", "--turn", ""], "explain"),
    (["snapshot", "DB", "OUT", "--no-content"], "snapshot"),
    (["snapshot", "DB", "OUT", "--run", "r"], "snapshot"),
    (["snapshot", "DB", "OUT", "--max-output-bytes", "1024"], "snapshot"),
    (["schema", "--no-content"], "schema"), (["sql", "DB", "SELECT 1", "--run", "r"], "query"),
]
CLI_ERROR_IDS = ["missing-command", "unknown-command", "options-before-command", "missing-database", "removed-text",
                 "removed-acceptance", "malformed-json", "null-json-params", "nonfinite-json-params", "nested-json-params",
                 "oversized-json-integer", "boolean-integer-spelling", "zero-limit", "invalid-boolean-filter",
                 "invalid-capture-filter", "empty-outcome-filter", "empty-condition-filter", "small-output-budget",
                 "large-output-budget", "empty-expect", "duplicate-expect", "unknown-expect", "negative-offset",
                 "small-payload-window", "over-depth", "empty-turn", "snapshot-content-option", "snapshot-run-option",
                 "snapshot-budget-option", "schema-content-option", "sql-run-option"]


@pytest.mark.parametrize("args,operation", CLI_ERRORS, ids=CLI_ERROR_IDS)
def test_cli_request_errors_are_bounded_json_before_storage(tmp_path, args, operation):
    # Missing/unknown commands use operation=schema as bounded discovery; there is no extra public 'cli' operation.
    rewritten = [str(tmp_path / "absent.sqlite") if arg == "DB" else str(tmp_path / "out.sqlite") if arg == "OUT" else arg for arg in args]
    process, result = _command("fr", rewritten, tmp_path)
    _refusal(process, result, operation)
    check.is_false((tmp_path / "absent.sqlite").exists(), msg="Argument errors cannot create the missing source database.")
    check.is_false((tmp_path / "out.sqlite").exists(), msg="Unsupported snapshot options cannot reserve an output file.")


@pytest.mark.parametrize("entry", ["fr", "module"])
@pytest.mark.parametrize("population", [0, 1, 2], ids=["zero-runs", "one-run", "two-runs"])
def test_cli_run_inference_uses_exact_database_cardinality(command_recording, entry, population):
    path, symbols, cwd = command_recording
    with closing(sqlite3.connect(path)) as connection:
        if population == 0:
            connection.execute("DELETE FROM runs")
        elif population == 2:
            connection.execute("INSERT INTO runs(run_id,started_at,environment_json,policy_json,coverage_json) VALUES(?,?,?,?,?)",
                               ("other-run", "later", "{}", "{}", "{}"))
        connection.commit()
    process, listed = _command(entry, ["runs", path, "--limit", "1"], cwd)
    assert listed["ok"], "Discovery must work before selected-run inference is assessed."
    check.equal(process.returncode, 0, msg="Even empty or ambiguous run populations remain discoverable.")
    process, selected = _command(entry, ["turns", path, "--limit", "1"], cwd)
    if population == 1:
        assert selected["ok"], "Exactly one stored run permits omitted --run selection."
        check.equal(selected["scope"]["run_id"], symbols["R"], msg="Automatic selection uses the actual sole stored identity.")
        check.equal(process.returncode, 0, msg="A single-run inferred navigation command succeeds.")
    else:
        _refusal(process, selected, "find_turns", reason="ambiguous_run")
        check.is_in("fr runs", selected["error"]["message"], msg="Ambiguous or empty inference gives the required discovery guidance.")
        if population == 2:
            process, explicit = _command(entry, ["turns", path, "--run", symbols["R"], "--limit", "1"], cwd)
            assert explicit["ok"], "An explicit valid run must remain usable in a multi-run recording."
            check.equal(process.returncode, 0, msg="Limited discovery pages cannot prevent explicit run selection.")


@pytest.mark.parametrize("case,operation,code,reason", [
    ("run", "find_turns", "unknown_run", "unknown_run"),
    ("turn", "explain", "unknown_turn", "unknown_turn"),
    ("model-call", "explain", "unknown_event", "unknown_model_call"),
    ("artifact", "payload", "unknown_artifact", "unknown_artifact"),
    ("event", "slice", "unknown_event", "unknown_event"),
    ("root-kind", "slice", "invalid_request", None),
], ids=["unknown-run", "unknown-turn", "unknown-selected-model-call", "unknown-artifact", "unknown-event", "wrong-root-kind"])
def test_cli_unknown_id_and_wrong_root_errors(command_recording, case, operation, code, reason):
    path, symbols, cwd = command_recording
    choices = {
        "run": ["turns", path, "--run", "absent-run"],
        "turn": ["explain", path, "--run", symbols["R"], "--turn", "absent-turn"],
        "model-call": ["explain", path, "--run", symbols["R"], "--turn", symbols["T1"], "--model-call", "absent-call"],
        "artifact": ["payload", path, "absent-artifact", "--run", symbols["R"]],
        "event": ["slice", path, "absent-event", "--run", symbols["R"]],
        "root-kind": ["slice", path, symbols["D1"], "--run", symbols["R"]],
    }
    process, result = _command("fr", choices[case], cwd)
    _refusal(process, result, operation, code=code, reason=reason)


@pytest.mark.parametrize("command,options,code", [
    ("payload", [], "content_disabled"), ("sql", [], "content_disabled"),
    ("payload", ["--offset", "-1"], "invalid_request"),
    ("sql", ["--params", "[{}]"], "invalid_request"),
    ("sql", ["--timeout", "0"], "invalid_request"),
], ids=["payload-before-storage", "query-before-storage", "bad-offset-before-policy", "bad-params-before-policy", "bad-timeout-before-policy"])
def test_cli_no_content_and_argument_precedence(tmp_path, command, options, code):
    operand = "sensitive-artifact-sentinel" if command == "payload" else "DELETE FROM sensitive_sql_sentinel"
    process, result = _command("fr", [command, tmp_path / "absent.sqlite", operand, "--no-content"] + options, tmp_path)
    _refusal(process, result, "query" if command == "sql" else command, code=code)
    check.is_not_in("sentinel", process.stdout, msg="Policy and argument failures must not echo arbitrary content-bearing input.")


PAYLOAD_OFFSET_CASES = [
    ("upper-offset", 2 ** 63, 4, "invalid_request", "invalid_offset", "offset must be an integer in its documented range."),
    ("joint-upper-window", 2 ** 63, 3, "invalid_request", "invalid_offset", "offset must be an integer in its documented range."),
    ("joint-negative-window", -1, 3, "invalid_request", "invalid_offset", "offset must be an integer in its documented range."),
    ("valid-upper-bad-window", 2 ** 63 - 1, 3, "invalid_request", "invalid_max_bytes", "max_bytes must be an integer in its documented range."),
    ("valid-upper-policy", 2 ** 63 - 1, 4, "content_disabled", "no_content", "Explicit content access is disabled by this output policy."),
    ("zero-policy", 0, 4, "content_disabled", "no_content", "Explicit content access is disabled by this output policy."),
]


@pytest.mark.parametrize("offset,max_bytes,code,reason,message", [
    pytest.param(offset, max_bytes, code, reason, message, id=case)
    for case, offset, max_bytes, code, reason, message in PAYLOAD_OFFSET_CASES
])
def test_cli_payload_offset_range_and_precedence(tmp_path, offset, max_bytes, code, reason, message):
    path = tmp_path / "absent.sqlite"
    args = ["payload", path, "valid-artifact", "--offset", offset, "--max-bytes", max_bytes, "--no-content"]
    expected = inspect.payload(path, "valid-run", "valid-artifact", offset, max_bytes, no_content=True)
    assert expected["ok"] is False and isinstance(expected.get("error"), dict), "The public Python precedence control must refuse before its literal error and CLI parity are assessed"
    check.equal(expected["error"], {"code": code, "reason": reason, "message": message},
                msg="The Python control independently follows the literal offset, window and content-policy precedence matrix.")
    process, result = _command("fr", args, tmp_path)
    _refusal(process, result, "payload", code=code, reason=reason)
    check.equal(result, expected, msg="The installed fr command without an explicit run preserves the complete Python payload refusal envelope.")
    for suffix in ("", "-wal", "-shm", "-journal"):
        check.is_false(Path(str(path) + suffix).exists(),
                       msg="Payload argument/policy precedence leaves the absent main file and every SQLite companion absent: " + (suffix or "main"))


def test_cli_payload_offset_validation_precedes_storage(tmp_path):
    path = tmp_path / "absent.sqlite"
    args = ["payload", path, "valid-artifact", "--offset", 2 ** 63, "--max-bytes", 4]
    expected = inspect.payload(path, "valid-run", "valid-artifact", 2 ** 63, 4)
    assert expected["ok"] is False and isinstance(expected.get("error"), dict), "The public upper-bound control must refuse before comparing the installed storage-order behavior"
    check.equal(expected["error"], {"code": "invalid_request", "reason": "invalid_offset", "message": "offset must be an integer in its documented range."},
                msg="The literal integer-range error must precede storage access even when explicit content is allowed.")
    process, result = _command("fr", args, tmp_path)
    _refusal(process, result, "payload", reason="invalid_offset")
    check.equal(result, expected, msg="The installed fr command without an explicit run validates the oversized offset before touching a missing recording.")
    for suffix in ("", "-wal", "-shm", "-journal"):
        check.is_false(Path(str(path) + suffix).exists(),
                       msg="Range validation cannot create the missing recording or a SQLite companion: " + (suffix or "main"))


def test_cli_payload_valid_upper_neighbor_reaches_retained_boundary(command_recording):
    path, symbols, cwd = command_recording
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as connection:
        row = connection.execute("SELECT a.artifact_id FROM turns t JOIN artifacts a ON a.artifact_id=t.prompt_ref WHERE t.turn_id=? AND a.body_text IS NOT NULL", (symbols["T1"],)).fetchone()
    assert row is not None, "The stored canonical T1 prompt must be retained before exercising an in-range offset beyond its actual UTF-8 boundary"
    artifact_id = row[0]
    before = path.read_bytes()
    expected = inspect.payload(path, symbols["R"], artifact_id, 2 ** 63 - 1, 4)
    assert expected["ok"] is False and isinstance(expected.get("error"), dict), "The valid integer upper neighbor must reach a structured retained-content refusal before CLI parity is inspected"
    check.equal(expected["error"], {"code": "invalid_request", "reason": "invalid_offset", "message": "offset exceeds retained UTF-8 content."},
                msg="The valid integer upper neighbor passes range validation and is rejected only by the stored UTF-8 content boundary.")
    process, result = _command("fr", ["payload", path, artifact_id, "--run", symbols["R"], "--offset", 2 ** 63 - 1, "--max-bytes", 4], cwd)
    _refusal(process, result, "payload", reason="invalid_offset")
    check.equal(result, expected, msg="The installed fr command with an explicit run preserves the exact retained-boundary Python envelope for the legal integer upper neighbor.")
    check.equal(path.read_bytes(), before, msg="Both the public and installed retained-boundary refusals leave the source recording byte-for-byte unchanged.")


@pytest.mark.parametrize("kind,code", [("missing", "recording_unreadable"), ("invalid", "recording_unreadable"),
                                        ("version", "incompatible_schema"), ("structure", "incompatible_schema")],
                         ids=["missing-file", "invalid-bytes", "wrong-version", "missing-required-schema"])
def test_cli_operational_errors_have_exit_three(command_recording, kind, code):
    path, symbols, cwd = command_recording
    if kind == "missing":
        path.unlink()
    elif kind == "invalid":
        path.write_bytes(b"readable but not a SQLite recording")
    else:
        with closing(sqlite3.connect(path)) as connection:
            connection.execute("PRAGMA user_version=7" if kind == "version" else "DROP VIEW answer_occurrences")
            connection.commit()
    before = path.read_bytes() if path.exists() else None
    process, result = _command("module", ["runs", path], cwd)
    _refusal(process, result, "runs", code=code, exit_code=3)
    check.equal(path.read_bytes() if path.exists() else None, before, msg="Storage/schema failure cannot repair or rewrite the input recording.")


def test_cli_budget_counts_utf8_json_without_terminal_newline(command_recording):
    path, symbols, cwd = command_recording
    sql = "WITH RECURSIVE n(x) AS (SELECT 0 UNION ALL SELECT x+1 FROM n WHERE x<7) SELECT x,? FROM n"
    params = ['Aé中😀"\\\n' * 12]
    expected = inspect.query(path, sql, params)
    assert expected["ok"], "The full reference query must succeed before its exact encoded budget is selected."
    budget = len(json.dumps(expected, ensure_ascii=False, separators=(",", ":")).encode())
    assert budget >= 1024, "The multibyte control must require at least the minimum legal envelope budget."
    process, result = _command("fr", ["sql", path, sql, "--params", json.dumps(params), "--max-output-bytes", str(budget)], cwd)
    check.equal(result, expected, msg="An exact JSON byte cap must preserve the complete Python-equivalent response.")
    check.equal(len(process.stdout.encode("utf-8")), budget + 1, msg="The CLI's single newline is explicitly outside the output budget.")


def test_installed_cli_imports_from_distribution_outside_checkout(command_recording):
    path, symbols, cwd = command_recording
    probe = '''import importlib.metadata,json,pathlib,sys
import mcp_flightrecorder,mcp_flightrecorder.cli,mcp_flightrecorder.inspect
print(json.dumps({"prefix":sys.prefix,"package":mcp_flightrecorder.__file__,"cli":mcp_flightrecorder.cli.__file__,"reader":mcp_flightrecorder.inspect.__file__,"version":importlib.metadata.version("mcp-flightrecorder")}))
'''
    process = subprocess.run([sys.executable, "-I", "-B", "-c", probe], cwd=cwd, capture_output=True, text=True, timeout=10)
    assert process.returncode == 0, "Installed provenance must be established by a fresh isolated interpreter."
    observed = json.loads(process.stdout)
    for name in ("package", "cli", "reader"):
        check.is_true(Path(observed[name]).resolve().is_relative_to(Path(observed["prefix"]).resolve()),
                      msg="The " + name + " module must come from the installed distribution, never the checkout.")
    check.equal(observed["version"], "0.5.20", msg="The subprocess exercises this release version.")
    check.equal(importlib.metadata.version("mcp-flightrecorder"), observed["version"],
                msg="The direct Python test and isolated CLI probe identify the same installed version.")
