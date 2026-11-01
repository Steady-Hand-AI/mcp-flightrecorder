"""Public expectation and cursor input contracts over isolated reader controls.

Coverage is produced by Recorder. Navigation controls are labelled copies of
the accepted schema and data, with explicit additional rows. Cursor oracles use
only stdlib JSON/base64 and the documented v1 fields, never reader helpers.
"""
import base64
from contextlib import closing
import copy
import hashlib
import json
from pathlib import Path
import shutil
import signal
import sqlite3
import subprocess
import sys
import time

import pytest
from pytest_check import check

from mcp_flightrecorder import Recorder, inspect

from .recordings import integration_recording


INTEGRATION, SYMBOLS = integration_recording()
MAX_OUTPUT = 1_048_576
COVERAGE = {"producers": {"model": "notes.model", "host": None, "mcp": None}, "model_complete": True}
ENVELOPE_KEYS = {"format", "operation", "ok", "scope", "data", "unknowns", "cutoffs",
                 "diagnostics_omitted", "error", "untrusted_content"}
COVERAGE_ORACLES = {
    "empty": ("pass", [], [("coverage.model", "pass", 1, 1, 0, 0),
                             ("coverage.host", "not_applicable", 0, 0, 0, 0),
                             ("coverage.mcp", "not_applicable", 0, 0, 0, 0)]),
    "host": ("fail", ["coverage.host"], [("coverage.model", "pass", 1, 1, 0, 0),
                                         ("coverage.host", "fail", 1, 1, 0, 1),
                                         ("coverage.mcp", "not_applicable", 0, 0, 0, 0)]),
    "all": ("fail", ["coverage.host", "coverage.mcp"], [("coverage.model", "pass", 1, 1, 0, 0),
                                                         ("coverage.host", "fail", 1, 1, 0, 1),
                                                         ("coverage.mcp", "fail", 1, 1, 0, 1)]),
}


def compact(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def successful(result, operation):
    assert result.get("ok") is True and isinstance(result.get("data"), dict), (
        f"The {operation} control must complete before dependent data checks: {result.get('error')}"
    )
    check.equal(set(result), ENVELOPE_KEYS, msg="Successful input controls retain the exact public envelope shape.")
    check.equal(result["operation"], operation, msg="The public result identifies the selected Python operation.")
    check.equal(result["format"], "mfr.inspect/1", msg="Input repairs preserve the inspection format version.")
    check.is_none(result["error"], msg="Successful inspection has no operational error.")
    check.is_true(result["untrusted_content"], msg="Reader controls remain explicitly untrusted recorded content.")
    return result["data"]


def refused(result, operation, code, reason, *forbidden):
    assert result.get("ok") is False and isinstance(result.get("error"), dict), (
        f"The {operation} negative control must refuse before dependent error checks: expected {code}/{reason}"
    )
    check.equal(set(result), ENVELOPE_KEYS, msg="Expected input failures use the ordinary common envelope.")
    check.equal((result["format"], result["operation"]), ("mfr.inspect/1", operation),
                msg="Refusal preserves the version and selected public operation.")
    check.equal(set(result["error"]), {"code", "reason", "message"}, msg="The public error has only its three documented fields.")
    check.equal((result["error"]["code"], result["error"]["reason"]), (code, reason),
                msg="The refusal reaches the exact assigned category and reason, not a nearby storage or argument error.")
    check.is_none(result["data"], msg="A refused page exposes no partial successful data object.")
    check.equal(result["scope"], {}, msg="Input refusal does not echo arbitrary scope identities.")
    check.equal((result["unknowns"], result["cutoffs"], result["diagnostics_omitted"]),
                ([], [], {"unknowns": 0, "cutoffs": 0}), msg="An operational error does not retain partial-page diagnostics.")
    check.is_true(result["untrusted_content"], msg="Error envelopes preserve the untrusted-content marker.")
    encoded = compact(result)
    check.less_equal(len(encoded), 1024, msg="Every refusal fits the independent small error-envelope budget.")
    for value in forbidden:
        check.is_not_in(value, encoded.decode("utf-8"), msg="Safe errors do not echo the supplied cursor or long identity.")


@pytest.fixture(autouse=True)
def bounded_input_control():
    """Bound each Linux acceptance control while restoring caller timer ownership."""
    def expired(signum, frame):
        raise TimeoutError("Inspection input control exceeded its 45-second watchdog.")

    previous_handler = signal.getsignal(signal.SIGALRM)
    previous_timer = signal.getitimer(signal.ITIMER_REAL)
    started = time.monotonic()
    signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, min(45, previous_timer[0]) if previous_timer[0] else 45)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)
        if previous_timer[0]:
            remaining = max(0.000001, previous_timer[0] - (time.monotonic() - started))
            signal.setitimer(signal.ITIMER_REAL, remaining, previous_timer[1])


@pytest.fixture
def coverage_recording(tmp_path):
    path = tmp_path / "public-expectation-control.sqlite"
    recorder = Recorder(path, capture="metadata", coverage=COVERAGE)
    try:
        run_id = recorder.capture_scope().run_id
    finally:
        closed = recorder.close(timeout=5)
    assert closed.writer_stopped and closed.portable, (
        "The public producer must close a portable recording before coverage input assertions."
    )
    return path, run_id


def coverage_facts(result, label):
    data = successful(result, "check")
    status, failures, facts = COVERAGE_ORACLES[label]
    actual = [tuple(item[key] for key in ("id", "status", "applicable", "observed", "unknown", "failed"))
              for item in data["items"][:3]]
    check.equal(actual, facts, msg="Coverage uses the literal model/host/MCP order and declaration-unit counts.")
    check.equal((data["status"], data["required_failures"], data["required_unknowns"]),
                (status, failures, []), msg="Only explicitly required absent producers fail this clean public recording.")
    check.equal(data["run"]["coverage"], COVERAGE, msg="Expectations change requirements without rewriting stored declarations.")
    check.equal((data["stats"]["turns"], data["stats"]["events"]), (0, 0),
                msg="Coverage requirements are established by declarations without invented observations.")


def copied(tmp_path, name="labelled-navigation-copy.sqlite"):
    path = tmp_path / name
    shutil.copyfile(INTEGRATION, path)
    return path


def clone_rows(path, kind, identities, *, outcome=None):
    """Copy existing valid-schema row shapes with explicit ordering identities."""
    table, field = ("runs", "run_id") if kind == "runs" else ("turns", "turn_id")
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("PRAGMA foreign_keys=ON")
        connection.row_factory = sqlite3.Row
        template = dict(connection.execute("SELECT * FROM " + table + " LIMIT 1").fetchone())
        columns = tuple(template)
        for identifier, started_at in identities:
            row = {**template, field: identifier, "started_at": started_at}
            if kind == "turns":
                row["prompt_ref"] = None
                if outcome is not None:
                    row["host_outcome"] = outcome
            connection.execute("INSERT INTO " + table + " (" + ",".join(columns) + ") VALUES (" +
                               ",".join("?" for _ in columns) + ")", [row[column] for column in columns])
        connection.commit()


def navigate(path, kind, **kwargs):
    if kind == "runs":
        return inspect.runs(path, **kwargs)
    return inspect.find_turns(path, SYMBOLS["R"], **kwargs)


def page_ids(data, kind):
    return [item["run_id"] if kind == "runs" else item["turn"]["turn_id"] for item in data["items"]]


def cursor_document(path, kind, no_content, last):
    """Independent documented v1 scope, preserving full ordering identities."""
    return {"version": 1, "operation": "runs" if kind == "runs" else "find_turns",
            "recording": hashlib.sha256(str(path.resolve()).encode("utf-8")).hexdigest(),
            "run_id": None if kind == "runs" else SYMBOLS["R"], "filters": {},
            "no_content": no_content, "order": ["started_at", "run_id" if kind == "runs" else "turn_id"],
            "last": list(last)}


def token_for(document, *, escaped=False):
    raw = json.dumps(document, ensure_ascii=escaped, allow_nan=False, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def decoded(token):
    return json.loads(base64.urlsafe_b64decode(token + "=" * (-len(token) % 4)))


def command(entry, args, cwd):
    argv = [str(Path(sys.executable).with_name("fr"))] if entry == "fr" else [sys.executable, "-I", "-B", "-m", "mcp_flightrecorder"]
    process = subprocess.run(argv + [str(arg) for arg in args], cwd=cwd,
                             capture_output=True, text=True, timeout=20)
    assert process.stdout, "Every installed input-control invocation must emit its one JSON result before parsing."
    result = json.loads(process.stdout)
    check.equal(process.stdout, compact(result).decode("utf-8") + "\n",
                msg="Installed entrypoints emit exactly one compact JSON value and terminal newline.")
    check.equal(process.stderr, "", msg="Expected cursor and expectation failures emit no traceback or parser prose.")
    return process.returncode, result


@pytest.mark.parametrize("no_content", [False, True], ids=["ordinary", "no-content"])
@pytest.mark.parametrize("container", [list, tuple, set, frozenset], ids=["list", "tuple", "set", "frozenset"])
def test_expect_supported_containers(coverage_recording, container, no_content):
    path, run_id = coverage_recording
    before = digest(path)
    baseline = inspect.check(path, run_id, no_content=no_content)
    coverage_facts(baseline, "empty")
    for label, names in (("empty", []), ("host", ["host"]), ("all", ["mcp", "host", "model"])):
        source = names + names if container in (set, frozenset) else names
        expect = container(source)
        preserved = copy.deepcopy(expect)
        result = inspect.check(path, run_id, expect=expect, no_content=no_content)
        coverage_facts(result, label)
        reference = inspect.check(path, run_id, expect=sorted(names), no_content=no_content)
        check.equal(result, reference, msg="Equal effective names produce equal complete envelopes regardless of container or input order.")
        if label == "empty":
            check.equal(result, baseline, msg="Every supported empty container has the same meaning as None.")
        check.equal(expect, preserved, msg="Validation and coverage assessment do not mutate caller containers.")
    check.equal(digest(path), before, msg="All supported expectation forms leave public recording bytes unchanged.")


@pytest.mark.parametrize("case", ["duplicate-list", "duplicate-tuple", "unknown-list", "unknown-set", "unknown-frozenset",
                                  "boolean-member", "nested-list", "mapping", "generator", "bare-string",
                                  "boolean-container", "list-subclass", "string-subclass"])
def test_expect_invalid_inputs_precede_storage(coverage_recording, tmp_path, case):
    class ListSubclass(list):
        pass

    class StringSubclass(str):
        pass

    factories = {
        "duplicate-list": lambda: ["model", "model"], "duplicate-tuple": lambda: ("host", "host"),
        "unknown-list": lambda: ["context"],
        "unknown-set": lambda: {"context"}, "unknown-frozenset": lambda: frozenset(("context",)),
        "boolean-member": lambda: [True], "nested-list": lambda: [["model"]],
        "mapping": lambda: {"model": True}, "generator": lambda: (name for name in ("host",)),
        "bare-string": lambda: "model", "boolean-container": lambda: False,
        "list-subclass": lambda: ListSubclass(["host"]), "string-subclass": lambda: [StringSubclass("host")],
    }
    value = factories[case]()
    path, run_id = coverage_recording
    before = digest(path)
    for selected_path in (path, tmp_path / "absent.sqlite"):
        refused(inspect.check(selected_path, run_id, expect=value), "check", "invalid_request", "invalid_expect")
    if case == "generator":
        check.equal(list(value), ["host"], msg="Rejecting an unsupported iterable must not consume or coerce it.")
    check.is_false((tmp_path / "absent.sqlite").exists(), msg="Expectation validation outranks opening or creating an absent DB.")
    check.equal(digest(path), before, msg="Invalid expectation inputs cannot change recording bytes.")


@pytest.mark.parametrize("declaration,no_content", [("missing", False), ("malformed", True)], ids=["missing", "malformed"])
def test_expect_preserves_unknown_declarations(coverage_recording, tmp_path, declaration, no_content):
    original, run_id = coverage_recording
    original_hash = digest(original)
    path = tmp_path / ("labelled-" + declaration + "-coverage.sqlite")
    shutil.copyfile(original, path)
    value = copy.deepcopy(COVERAGE)
    if declaration == "missing":
        del value["producers"]["host"]
    else:
        value["producers"]["host"] = {"wrong": "type"}
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("UPDATE runs SET coverage_json=?", (json.dumps(value),))
        connection.commit()
    before = digest(path)
    result = inspect.check(path, run_id, expect=frozenset(("host",)), no_content=no_content)
    data = successful(result, "check")
    coverage = data["items"][:3]
    check.equal([(row["id"], row["status"], row["applicable"], row["observed"], row["unknown"], row["failed"]) for row in coverage],
                [("coverage.model", "pass", 1, 1, 0, 0), ("coverage.host", "unknown", 1, 0, 1, 0),
                 ("coverage.mcp", "not_applicable", 0, 0, 0, 0)],
                msg="Requiring an unavailable declaration remains unknown instead of becoming an explicit absent-producer failure.")
    check.equal((data["status"], data["required_failures"], data["required_unknowns"]),
                ("unknown", [], ["coverage.host"]), msg="The missing or malformed host declaration is the only required uncertainty.")
    check.equal(result, inspect.check(path, run_id, expect=["host"], no_content=no_content),
                msg="Supported set-like inputs preserve the prior unknown-declaration semantics exactly.")
    check.equal((digest(path), digest(original)), (before, original_hash),
                msg="Inspection preserves both the labelled input and the original public producer recording.")


@pytest.mark.parametrize("entry", ["fr", "module"])
def test_expect_cli_csv_contract(coverage_recording, tmp_path, entry):
    path, run_id = coverage_recording
    before = digest(path)
    exit_code, result = command(entry, ["check", path, "--run", run_id, "--expect", "mcp,host,model"], tmp_path)
    coverage_facts(result, "all")
    check.equal(exit_code, 2, msg="A completed check with required absent host/MCP producers uses failed-check exit 2.")
    check.equal(result, inspect.check(path, run_id, expect={"model", "host", "mcp"}),
                msg="Installed CSV entrypoints preserve the complete public Python expectation envelope.")
    for csv in ("", "model,model", "model,context"):
        exit_code, result = command(entry, ["check", path, "--run", run_id, "--expect", csv], tmp_path)
        refused(result, "check", "invalid_request", "invalid_expect")
        check.equal(exit_code, 1, msg="Empty, duplicate and unknown CSV values retain invalid-request exit 1.")
    check.equal(digest(path), before, msg="Valid and invalid installed expectation commands never write source bytes.")


@pytest.mark.parametrize("kind,position,surrogate,no_content", [
    ("runs", 0, "\ud800", False), ("runs", 1, "\udfff", False),
    ("runs", 0, "\udfff", True), ("runs", 1, "\ud800", True),
    ("turns", 0, "\ud800", False), ("turns", 1, "\udfff", False),
    ("turns", 0, "\udfff", True), ("turns", 1, "\ud800", True),
], ids=["runs-time-high", "runs-id-low", "runs-time-low-no-content", "runs-id-high-no-content",
        "turns-time-high", "turns-id-low", "turns-time-low-no-content", "turns-id-high-no-content"])
def test_cursor_surrogates_are_bounded_refusals(tmp_path, kind, position, surrogate, no_content):
    path = copied(tmp_path)
    clone_rows(path, kind, [("notes-cursor-control", "0000")])
    before = digest(path)
    operation = "runs" if kind == "runs" else "find_turns"
    first = successful(navigate(path, kind, limit=1, no_content=no_content), operation)
    assert isinstance(first["next_cursor"], str), "A real matching-scope cursor is required before changing only one ordering component."
    document = decoded(first["next_cursor"])
    preserved = copy.deepcopy(document)
    document["last"][position] = surrogate
    malformed = token_for(document, escaped=True)
    check.equal({key: value for key, value in document.items() if key != "last"},
                {key: value for key, value in preserved.items() if key != "last"},
                msg="The malformed control changes no valid scope, filter, version or policy field.")
    check.equal(document["last"][1 - position], preserved["last"][1 - position],
                msg="Each negative control independently changes exactly one ordering component.")
    result = navigate(path, kind, cursor=malformed, no_content=no_content)
    refused(result, operation, "cursor_mismatch", "cursor_scope", malformed)
    if (kind, position, no_content) in (("runs", 0, False), ("turns", 1, True)):
        args = [kind, path] + (["--run", SYMBOLS["R"]] if kind == "turns" else [])
        args += ["--cursor", malformed] + (["--no-content"] if no_content else [])
        exit_code, cli_result = command("fr", args, tmp_path)
        refused(cli_result, operation, "cursor_mismatch", "cursor_scope", malformed)
        check.equal((exit_code, cli_result), (1, result),
                    msg="Installed runs/turns report the same small cursor refusal without a Unicode traceback.")
    check.equal(digest(path), before, msg="Malformed decoded cursor strings never alter any recording byte.")


@pytest.mark.parametrize("no_content", [False, True], ids=["ordinary", "no-content"])
@pytest.mark.parametrize("kind", ["runs", "turns"])
def test_cursor_valid_unicode_and_scope_controls(tmp_path, kind, no_content):
    path = copied(tmp_path)
    controls = [("notes-ascii", "0000"), ("notes-é😀-a", "0001-é😀"), ("notes-é😀-b", "0001-é😀")]
    clone_rows(path, kind, controls)
    before = digest(path)
    operation = "runs" if kind == "runs" else "find_turns"
    first = successful(navigate(path, kind, limit=1, no_content=no_content), operation)
    check.equal(page_ids(first, kind), ["notes-ascii"], msg="Ordinary UTF-8 ordering text starts from the exact first stored row.")
    assert isinstance(first["next_cursor"], str), "The ASCII page must provide a token before continuation controls."
    second = successful(navigate(path, kind, limit=1, cursor=first["next_cursor"], no_content=no_content), operation)
    check.equal(page_ids(second, kind), ["notes-é😀-a"], msg="Continuation retains the non-ASCII and astral identity in full.")
    assert isinstance(second["next_cursor"], str), "The Unicode boundary needs a continuation before its complete tuple is checked."
    check.equal(decoded(second["next_cursor"])["last"], ["0001-é😀", "notes-é😀-a"],
                msg="Both non-ASCII/astral ordering strings survive the actual returned cursor exactly.")
    terminal = successful(navigate(path, kind, cursor=second["next_cursor"], no_content=no_content), operation)
    tail = [SYMBOLS["R"]] if kind == "runs" else [SYMBOLS["T1"], SYMBOLS["T2"]]
    check.equal(page_ids(terminal, kind), ["notes-é😀-b", *tail],
                msg="Valid Unicode continuation returns precisely the remaining equal-time row and original rows.")
    check.equal((terminal["has_more"], terminal["next_cursor"]), (False, None), msg="A completed Unicode continuation has no fabricated next page.")
    changed = decoded(first["next_cursor"])
    changed["version"] = 2
    for invalid in ("not a base64 cursor!", token_for(changed)):
        refused(navigate(path, kind, cursor=invalid, no_content=no_content), operation, "cursor_mismatch", "cursor_scope", invalid)
    refused(navigate(path, kind, cursor=first["next_cursor"], no_content=not no_content),
            operation, "cursor_mismatch", "cursor_scope", first["next_cursor"])
    if kind == "turns":
        refused(navigate(path, kind, cursor=first["next_cursor"], no_content=no_content, filters={"host_outcome": "returned"}),
                operation, "cursor_mismatch", "cursor_scope", first["next_cursor"])
    check.equal(digest(path), before, msg="Valid and mismatched Unicode cursor controls are read-only.")


@pytest.mark.parametrize("kind", ["runs", "turns"])
def test_cursor_precedence_and_unrelated_unicode_validation(tmp_path, kind):
    path = copied(tmp_path)
    clone_rows(path, kind, [("notes-precedence", "0000")])
    operation = "runs" if kind == "runs" else "find_turns"
    first = successful(navigate(path, kind, limit=1), operation)
    assert isinstance(first["next_cursor"], str), "Precedence controls need a real cursor before altering its ordering text."
    document = decoded(first["next_cursor"])
    document["last"][0] = "\ud800"
    malformed = token_for(document, escaped=True)
    before = digest(path)
    absent = tmp_path / "absent.sqlite"
    refused(navigate(absent, kind, limit=0, cursor=malformed), operation, "invalid_request", "invalid_limit")
    refused(navigate(absent, kind, cursor=malformed), operation, "recording_unreadable", "storage_failure")
    unreadable = tmp_path / "labelled-not-sqlite.sqlite"
    unreadable.write_bytes(b"This is deliberately not a SQLite recording.\n")
    unreadable_hash = digest(unreadable)
    refused(navigate(unreadable, kind, cursor=malformed), operation, "recording_unreadable", "storage_failure")
    refused(navigate(str(tmp_path / "invalid-\ud800.sqlite"), kind, cursor=malformed), operation, "invalid_request", "invalid_path")
    if kind == "turns":
        refused(inspect.find_turns(path, "absent-run", cursor=malformed), operation, "unknown_run", "unknown_run")
        refused(inspect.find_turns(path, "\udfff", cursor=malformed), operation, "invalid_request", "invalid_run_id")
    check.equal((digest(path), digest(unreadable)), (before, unreadable_hash),
                msg="Argument/storage/selection precedence leaves every existing input unchanged.")
    check.is_false(absent.exists(), msg="Neither limit validation nor malformed cursors may create the absent DB.")


@pytest.mark.parametrize("raw_bytes,characters", [(6144, 8192), (6145, 8194)], ids=["accepted-8192", "refused-8194"])
@pytest.mark.parametrize("no_content", [False, True], ids=["ordinary", "no-content"])
@pytest.mark.parametrize("kind", ["runs", "turns"])
def test_cursor_shared_exact_boundary(tmp_path, kind, no_content, raw_bytes, characters):
    path = copied(tmp_path)
    identifier = "notes-boundary-"
    document = cursor_document(path, kind, no_content, ("0000", identifier))
    padding = raw_bytes - len(compact(document))
    assert padding > 0, "The independent scope must leave positive identity padding to reach the intended byte boundary."
    identifier += "x" * padding
    document["last"][1] = identifier
    independent_token = token_for(document)
    assert len(compact(document)) == raw_bytes and len(independent_token) == characters, (
        "The stdlib JSON/base64 oracle must reach exact 8192 or the first achievable overflow 8194 before exercising readers."
    )
    check.is_true(independent_token.isascii(), msg="The shared cursor ceiling counts complete ASCII token characters.")
    check.is_not_in("=", independent_token, msg="The version-1 cursor remains unpadded base64url.")
    check.equal(characters % 4, 0 if raw_bytes == 6144 else 2,
                msg="Unpadded base64 jumps from 8192 to 8194; 8193 is not an achievable encoded length.")
    clone_rows(path, kind, [(identifier, "0000")])
    before = digest(path)
    operation = "runs" if kind == "runs" else "find_turns"
    first_result = navigate(path, kind, limit=1, no_content=no_content, max_output_bytes=MAX_OUTPUT)
    if characters == 8192:
        first = successful(first_result, operation)
        check.equal(page_ids(first, kind), [identifier], msg="The accepted boundary retains the full emitted row identity.")
        check.equal((first["has_more"], first["next_cursor"]), (True, independent_token),
                    msg="The complete emitted token matches the independently encoded 8192-character boundary.")
        continuation = successful(navigate(path, kind, cursor=first["next_cursor"], no_content=no_content, max_output_bytes=MAX_OUTPUT), operation)
        expected = [SYMBOLS["R"]] if kind == "runs" else [SYMBOLS["T1"], SYMBOLS["T2"]]
        check.equal(page_ids(continuation, kind), expected, msg="The exact generated boundary returns every remaining row and no repeated boundary row.")
        check.equal((continuation["has_more"], continuation["next_cursor"]), (False, None), msg="An accepted exact-boundary continuation exhausts normally.")
        independent = successful(navigate(path, kind, cursor=independent_token, no_content=no_content, max_output_bytes=MAX_OUTPUT), operation)
        check.equal(page_ids(independent, kind), expected, msg="The independently supplied exact input ceiling is also accepted.")
    else:
        refused(first_result, operation, "limit_exceeded", "cursor_size", identifier)
        refused(navigate(path, kind, cursor=independent_token, no_content=no_content, max_output_bytes=MAX_OUTPUT),
                operation, "cursor_mismatch", "cursor_scope", independent_token)
    check.equal(digest(path), before, msg="Exact token boundary checks never mutate the schema-valid recording copy.")


@pytest.mark.parametrize("no_content", [False, True], ids=["ordinary", "no-content"])
@pytest.mark.parametrize("kind", ["runs", "turns"])
def test_cursor_long_identity_terminal_page(tmp_path, kind, no_content):
    path = copied(tmp_path)
    identifier = "long-run-" + "x" * 7000 if kind == "runs" else "long-turn-" + "x" * 6999
    check.equal(len(identifier), 7009, msg="The labelled long-ID control reproduces the assigned 7009-character identity boundary.")
    clone_rows(path, kind, [(identifier, "0000")])
    before = digest(path)
    operation = "runs" if kind == "runs" else "find_turns"
    refused(navigate(path, kind, limit=1, no_content=no_content, max_output_bytes=MAX_OUTPUT),
            operation, "limit_exceeded", "cursor_size", identifier)
    terminal_result = navigate(path, kind, limit=200, no_content=no_content)
    terminal = successful(terminal_result, operation)
    expected = [identifier, *([SYMBOLS["R"]] if kind == "runs" else [SYMBOLS["T1"], SYMBOLS["T2"]])]
    check.equal(page_ids(terminal, kind), expected, msg="A terminal unpaged request preserves every full identity because it needs no continuation.")
    check.equal((terminal["has_more"], terminal["next_cursor"]), (False, None), msg="A real terminal page does not invent a cursor or report false continuation.")
    check.less_equal(len(compact(terminal_result)), 65536, msg="Long terminal identities remain subject to the whole-envelope budget.")
    check.equal(digest(path), before, msg="Long-ID refusal and terminal success leave source bytes untouched.")


@pytest.mark.parametrize("seam", ["create", "replace", "short-prefix"])
@pytest.mark.parametrize("kind", ["runs", "turns"])
def test_cursor_byte_trimming_never_searches_another_boundary(tmp_path, kind, seam):
    path = copied(tmp_path)
    no_content = kind == "turns"
    operation = "runs" if kind == "runs" else "find_turns"
    identifier = "long-row-" + "x" * 7000
    controls = [(identifier, "0000")]
    if seam == "replace":
        controls.append(("notes-middle", "0001"))
    elif seam == "short-prefix":
        controls = [("notes-short-prefix", "0000"), (identifier, "0001"), ("notes-middle", "0002")]
    options = {"no_content": no_content}
    if kind == "turns":
        controls.append(("notes-tail", "0009"))
        options["filters"] = {"host_outcome": "notes-trim-control"}
    clone_rows(path, kind, controls, outcome="notes-trim-control")
    expected = [row[0] for row in controls] + ([SYMBOLS["R"]] if kind == "runs" else [])
    before = digest(path)
    limit = {"create": 200, "replace": 2, "short-prefix": 3}[seam]
    full_result = navigate(path, kind, limit=limit, max_output_bytes=MAX_OUTPUT, **options)
    full = successful(full_result, operation)
    check.equal(page_ids(full, kind), expected if seam == "create" else expected[:limit],
                msg="The independent controlled row order places the long identity immediately before the row removed by byte trimming.")
    if seam == "create":
        check.equal((full["has_more"], full["next_cursor"]), (False, None), msg="The initial whole page is genuinely terminal before byte trimming introduces a cursor.")
    else:
        assert isinstance(full["next_cursor"], str), "The replacement seam must start from a representable real continuation."
        check.less_equal(len(full["next_cursor"]), 8192, msg="The initial short-boundary token is representable before trimming replaces it.")
        check.equal(decoded(full["next_cursor"])["last"][1], "notes-middle", msg="The initial continuation names the last intact short row.")
    budget = len(compact(full_result)) - 1
    assert 1024 <= budget <= MAX_OUTPUT, "The trimming seam must use a valid output budget exactly one byte below the known full envelope."
    refused(navigate(path, kind, limit=limit, max_output_bytes=budget, **options),
            operation, "limit_exceeded", "cursor_size", identifier)
    if seam == "short-prefix":
        short = successful(navigate(path, kind, limit=1, max_output_bytes=MAX_OUTPUT, **options), operation)
        check.equal(page_ids(short, kind), ["notes-short-prefix"], msg="An earlier short prefix really exists, so refusing instead of searching for it is observable.")
        refused(navigate(path, kind, limit=2, max_output_bytes=MAX_OUTPUT, **options),
                operation, "limit_exceeded", "cursor_size", identifier)
    check.equal(digest(path), before, msg="Every initial or recomputed cursor refusal preserves the labelled source bytes.")


@pytest.mark.parametrize("no_content", [False, True], ids=["ordinary", "no-content"])
@pytest.mark.parametrize("kind", ["runs", "turns"])
def test_cursor_tied_rows_survive_changing_page_budgets(tmp_path, kind, no_content):
    path = copied(tmp_path)
    identities = ["notes-tie-" + suffix for suffix in ("a", "b", "c", "d", "e")]
    clone_rows(path, kind, [(identifier, "0000") for identifier in reversed(identities)])
    before = digest(path)
    operation = "runs" if kind == "runs" else "find_turns"
    expected = [*identities, *([SYMBOLS["R"]] if kind == "runs" else [SYMBOLS["T1"], SYMBOLS["T2"]])]
    full_result = navigate(path, kind, limit=200, no_content=no_content, max_output_bytes=MAX_OUTPUT)
    full = successful(full_result, operation)
    check.equal(page_ids(full, kind), expected, msg="Equal timestamps use full IDs to order rows independently of insertion order.")
    budget = len(compact(full_result)) - 1
    assert 1024 <= budget <= MAX_OUTPUT, "The pagination control needs a valid smaller byte budget before checking continuation identity."
    collected, cursor = [], None
    for index in range(len(expected) + 1):
        row_limit = 200 if index == 0 else (1 if index % 2 else 3)
        output_limit = budget if index % 2 == 0 else MAX_OUTPUT
        result = navigate(path, kind, limit=row_limit, cursor=cursor, no_content=no_content, max_output_bytes=output_limit)
        page = successful(result, operation)
        emitted = page_ids(page, kind)
        collected.extend(emitted)
        check.less_equal(len(compact(result)), output_limit, msg="Every changed page budget bounds the complete encoded public envelope.")
        cursor = page["next_cursor"]
        if index == 0:
            check.less(len(emitted), len(expected), msg="The first page genuinely removes trailing records because of its byte budget.")
            check.is_in("byte_limit", [row["code"] for row in result["cutoffs"]], msg="Representable byte trimming remains explicit in diagnostics.")
        if cursor is None:
            check.is_false(page["has_more"], msg="Exhaustion agrees with the absence of a continuation token.")
            break
        assert emitted, "A continuation must follow an emitted row before its last full identity can be compared."
        check.less_equal(len(cursor), 8192, msg="Every representable continuation respects the shared cap while budgets change.")
        check.equal(decoded(cursor)["last"][1], emitted[-1], msg="Continuation follows the last emitted identity, not a discarded or scanned row.")
        check.is_true(page["has_more"], msg="A real continuation declares remaining rows.")
    check.equal(collected, expected, msg="Changing item and byte budgets returns every tied/full identity exactly once in source order.")
    check.is_none(cursor, msg="The bounded stable population exhausts without a repeated-page loop.")
    check.equal(digest(path), before, msg="Multi-page stateless reads never write the input recording.")


@pytest.mark.parametrize("no_content", [False, True], ids=["ordinary", "no-content"])
@pytest.mark.parametrize("kind", ["runs", "turns"])
def test_cursor_representable_token_keeps_required_output_refusal(tmp_path, kind, no_content):
    path = copied(tmp_path)
    identities = ["notes-required-" + suffix + "x" * 1200 for suffix in ("a", "b")]
    clone_rows(path, kind, list(zip(identities, ("0000", "0001"))))
    before = digest(path)
    operation = "runs" if kind == "runs" else "find_turns"
    first = successful(navigate(path, kind, limit=1, no_content=no_content, max_output_bytes=MAX_OUTPUT), operation)
    check.equal(page_ids(first, kind), identities[:1], msg="The first full long row establishes an ordinary representable continuation.")
    assert isinstance(first["next_cursor"], str), "The required-output control needs a real input token before requesting the second row."
    check.less_equal(len(first["next_cursor"]), 8192, msg="This failure control stays below the cursor-size ceiling.")
    next_result = navigate(path, kind, limit=1, cursor=first["next_cursor"], no_content=no_content, max_output_bytes=MAX_OUTPUT)
    next_page = successful(next_result, operation)
    check.equal(page_ids(next_page, kind), identities[1:], msg="The same cursor succeeds when the required second-row envelope fits.")
    assert isinstance(next_page["next_cursor"], str), "The second long row still precedes the original fixture tail."
    check.less_equal(len(next_page["next_cursor"]), 8192, msg="Both input and would-be output tokens are representable in this byte-budget control.")
    check.greater(len(compact(next_result)), 1024, msg="The intact successful response independently proves that the minimum byte budget cannot fit it.")
    refused(navigate(path, kind, limit=1, cursor=first["next_cursor"], no_content=no_content, max_output_bytes=1024),
            operation, "limit_exceeded", "required_output", first["next_cursor"])
    check.equal(digest(path), before, msg="Required-output refusal keeps its prior read-only semantics.")


@pytest.mark.parametrize("kind,no_content", [("runs", False), ("turns", True)], ids=["runs", "turns-no-content"])
def test_cursor_size_cli_refusal_matches_public_api(tmp_path, kind, no_content):
    path = copied(tmp_path)
    identifier = "long-cli-" + "x" * 7000
    clone_rows(path, kind, [(identifier, "0000")])
    before = digest(path)
    operation = "runs" if kind == "runs" else "find_turns"
    expected = navigate(path, kind, limit=1, no_content=no_content, max_output_bytes=MAX_OUTPUT)
    refused(expected, operation, "limit_exceeded", "cursor_size", identifier)
    args = [kind, path] + (["--run", SYMBOLS["R"]] if kind == "turns" else [])
    args += ["--limit", "1", "--max-output-bytes", str(MAX_OUTPUT)] + (["--no-content"] if no_content else [])
    exit_code, result = command("fr", args, tmp_path)
    refused(result, operation, "limit_exceeded", "cursor_size", identifier)
    check.equal((exit_code, result), (1, expected), msg="Installed discovery commands refuse oversized generated cursors with exact Python parity and exit 1.")
    check.equal(digest(path), before, msg="Installed cursor-size refusal cannot change any source recording byte.")
