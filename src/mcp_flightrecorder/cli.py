"""Installed JSON command line for the shared, read-only inspection functions."""
import argparse
import json
import sys

from . import __version__
from . import inspect
from ._inspect_common import (InspectionError, encode, error_envelope, run_read,
                              validate_id, validate_int, validate_path, validate_timeout)
from ._inspect_diagnostics import FORMAT as DIAGNOSTIC_FORMAT
from ._inspect_text import render


class Parser(argparse.ArgumentParser):
    def error(self, message):
        if self.prog == "fr gate":
            raise InspectionError("invalid_request", "invalid_cli", "Use fr gate FILE --expect model,host,mcp --min-answer-occurrences 1. A positive non-turn minimum is required; see fr gate --help.")
        raise InspectionError("invalid_request", "invalid_cli", "The command or its arguments are invalid; use fr --help for the supported syntax.")


def _common(parser):
    parser.add_argument("--max-output-bytes", type=int, default=65_536)
    parser.add_argument("--no-content", action="store_true")


def _run_options(parser):
    _common(parser)
    parser.add_argument("--run")
    parser.add_argument("--timeout", type=float, default=10.0)


def _parser():
    parser = Parser(prog="fr", description="Inspect retained recording evidence as bounded JSON or opt-in text.")
    parser.add_argument("--version", action="version", version="mcp-flightrecorder " + __version__)
    commands = parser.add_subparsers(dest="command", required=True, parser_class=Parser)
    runs = commands.add_parser("runs", help="List recorded runs.")
    runs.add_argument("database")
    _common(runs)
    runs.add_argument("--limit", type=int, default=50)
    runs.add_argument("--cursor")
    runs.add_argument("--timeout", type=float, default=10.0)
    check = commands.add_parser("check", help="Check integrity and evidence availability.")
    check.add_argument("database")
    _run_options(check)
    check.add_argument("--expect")
    turns = commands.add_parser("turns", help="Find recorded turns.")
    turns.add_argument("database")
    _run_options(turns)
    turns.add_argument("--limit", type=int, default=50)
    turns.add_argument("--cursor")
    turns.add_argument("--text", action="store_true", help="Render readable labels and machine IDs.")
    turns.add_argument("--last", action="store_true", help="Select the latest matching recorded turn.")
    for name in ("condition-key", "host-outcome", "capture-state"):
        turns.add_argument("--" + name)
    for name in ("tool-error", "text-mismatch", "unattributed-operation", "forwarded-change"):
        turns.add_argument("--" + name, choices=("true", "false"))
    explain = commands.add_parser("explain", help="Explain one turn or model call.")
    explain.add_argument("database")
    selection = explain.add_mutually_exclusive_group(required=True)
    selection.add_argument("--turn")
    selection.add_argument("--last", action="store_true", help="Select the latest recorded turn in the selected run.")
    explain.add_argument("--model-call")
    explain.add_argument("--text", action="store_true")
    _run_options(explain)
    doctor = commands.add_parser("doctor", help="Describe bounded recorded gaps and likely corrective actions; not a PASS gate.")
    doctor.add_argument("database")
    _run_options(doctor)
    doctor.add_argument("--limit", type=int, default=50)
    doctor.add_argument("--json", action="store_true", help="Emit the separate mfr.diagnose/1 envelope.")
    selection = doctor.add_mutually_exclusive_group()
    selection.add_argument("--turn")
    selection.add_argument("--last", action="store_true")
    payload = commands.add_parser("payload", help="Read explicitly requested retained content.")
    payload.add_argument("database")
    payload.add_argument("artifact_id")
    payload.add_argument("--offset", type=int, default=0)
    payload.add_argument("--max-bytes", type=int, default=4096)
    _run_options(payload)
    graph = commands.add_parser("slice", help="Follow explicit submitted-input ancestry.")
    graph.add_argument("database")
    graph.add_argument("request_event_id")
    graph.add_argument("--limit", type=int, default=1000)
    graph.add_argument("--max-depth", type=int, default=12)
    _run_options(graph)
    query = commands.add_parser("sql", help="Run one explicit bounded SELECT or WITH query.")
    query.add_argument("database")
    query.add_argument("sql")
    query.add_argument("--params")
    query.add_argument("--limit", type=int, default=200)
    query.add_argument("--timeout", type=float, default=10.0)
    _common(query)
    commands.add_parser("schema", help="Describe the installed inspection contract.")
    copy = commands.add_parser("snapshot", help="Create one new portable local recording copy.")
    copy.add_argument("database")
    copy.add_argument("output")
    copy.add_argument("--timeout", type=float, default=10.0)
    gate = commands.add_parser("gate", help="Verify complete host evidence against explicit coverage and activity expectations.")
    gate.add_argument("database")
    gate.add_argument("--run")
    gate.add_argument("--answers", choices=("all", "mcp"), default="all", help="Compare all answers by default; mcp excludes only validated local comparisons.")
    gate.add_argument("--text", action="store_true", help="Render the completed verdict, scope and bounded evidence witnesses.")
    gate.add_argument("--expect", required=True, help="Comma-separated boundaries, including model and host.")
    for name in ("turns", "model-requests", "model-responses", "decisions", "tool-calls", "answer-occurrences"):
        gate.add_argument("--min-" + name, type=int)
    gate.add_argument("--timeout", type=float, default=10.0)
    gate.add_argument("--max-output-bytes", type=int, default=65_536)
    mcp = commands.add_parser("mcp", help="Serve one stable portable recording through optional stdio MCP.")
    mcp.add_argument("database")
    mcp.add_argument("--allow-content", action="store_true", help="Enable explicit retained-payload reads; other tools still omit bodies.")
    commands.add_parser("proxy", help="Forward one stdio child with optional MCP-boundary recording.")
    return parser


def _operation(command):
    return {"turns": "find_turns", "sql": "query", "doctor": "diagnose"}.get(command, command if command in {
        "check", "explain", "payload", "slice", "runs", "schema", "snapshot", "gate", "mcp"} else "schema")


def _expect(value):
    if value is None:
        return None
    items = value.split(",")
    if len(items) != len(set(items)) or any(item not in {"model", "host", "mcp"} for item in items):
        raise InspectionError("invalid_request", "invalid_expect", "Expect requires a nonempty comma-separated list of distinct model, host or mcp boundaries.")
    return items


def _params(value):
    if value is None:
        return None

    def nonfinite(value):
        raise ValueError("nonfinite")

    try:
        result = json.loads(value, parse_constant=nonfinite)
    except (ValueError, RecursionError):
        raise InspectionError("invalid_request", "invalid_params", "SQL parameters must be a valid JSON array or object.") from None
    if type(result) not in (list, dict):
        raise InspectionError("invalid_request", "invalid_params", "SQL parameters must be a JSON array or object.")
    return result


def _filters(args):
    result = {}
    for name in ("condition_key", "host_outcome", "capture_state"):
        value = getattr(args, name)
        if value is not None:
            validate_id(value, name)
            if name == "capture_state" and value not in {"complete", "partial", "unknown"}:
                raise InspectionError("invalid_request", "invalid_filters", "capture_state must be complete, partial or unknown.")
            result[name] = value
    for name in ("tool_error", "text_mismatch", "unattributed_operation", "forwarded_change"):
        value = getattr(args, name)
        if value is not None:
            result[name] = value == "true"
    return result


def _validate_scoped(args):
    validate_timeout(args.timeout)
    validate_path(args.database)
    validate_int(args.max_output_bytes, 1024, 1_048_576, "max_output_bytes")
    if args.run is not None:
        validate_id(args.run, "run_id")
    if args.command == "turns":
        validate_int(args.limit, 1, 200, "limit")
        _filters(args)
        if args.last and args.cursor is not None:
            raise InspectionError("invalid_request", "invalid_last", "--last cannot be combined with --cursor.")
    elif args.command == "check":
        _expect(args.expect)
    elif args.command == "explain":
        if args.turn is not None:
            validate_id(args.turn, "turn_id")
        if args.model_call is not None:
            validate_id(args.model_call, "model_call_id")
    elif args.command == "doctor":
        validate_int(args.limit, 1, 200, "limit")
        if args.turn is not None:
            validate_id(args.turn, "turn_id")
    elif args.command == "payload":
        validate_id(args.artifact_id, "artifact_id")
        validate_int(args.offset, 0, 2 ** 63 - 1, "offset")
        validate_int(args.max_bytes, 4, 65_536, "max_bytes")
    elif args.command == "slice":
        validate_id(args.request_event_id, "request_event_id")
        validate_int(args.limit, 1, 1000, "limit")
        validate_int(args.max_depth, 0, 12, "max_depth")


def _infer_run(args):
    def select(ctx):
        rows = ctx.connection.execute("SELECT run_id FROM runs LIMIT 2").fetchall()
        if len(rows) != 1:
            raise InspectionError("invalid_request", "ambiguous_run", "The recording does not contain exactly one run; use fr runs and select --run explicitly.")
        return {"run_id": rows[0]["run_id"]}

    return run_read(_operation(args.command), args.database, {}, select,
                    max_output_bytes=args.max_output_bytes, no_content=args.no_content, timeout=args.timeout)


def _scoped(args, run_id):
    common = {"max_output_bytes": args.max_output_bytes, "no_content": args.no_content, "timeout": args.timeout}
    if args.command == "check":
        return inspect.check(args.database, run_id, _expect(args.expect), **common)
    if args.command == "turns":
        return inspect.find_turns(args.database, run_id, filters=_filters(args),
                                  limit=args.limit, cursor=args.cursor, last=args.last, **common)
    if args.command in {"explain", "doctor"} and args.last:
        selected = inspect.find_turns(args.database, run_id, last=True, **common)
        if not selected["ok"]:
            selected["operation"] = _operation(args.command)
            return selected
        if not selected["data"]["items"]:
            return error_envelope(_operation(args.command), "unknown_turn", "no_turns", "The selected run has no recorded turns.")
        args.turn = selected["data"]["items"][0]["turn"]["turn_id"]
    if args.command == "explain":
        return inspect.explain(args.database, run_id, args.turn, args.model_call, **common)
    if args.command == "doctor":
        return inspect.diagnose(args.database, run_id, turn_id=args.turn, limit=args.limit, **common)
    if args.command == "payload":
        return inspect.payload(args.database, run_id, args.artifact_id, args.offset, args.max_bytes, **common)
    return inspect.slice(args.database, run_id, args.request_event_id, limit=args.limit,
                         max_depth=args.max_depth, **common)


def _dispatch(args):
    if args.command == "schema":
        return inspect.schema()
    if args.command == "snapshot":
        return inspect.snapshot(args.database, args.output, timeout=args.timeout)
    if args.command == "gate":
        names = ("turns", "model_requests", "model_responses", "decisions", "tool_calls", "answer_occurrences")
        minima = {name: getattr(args, "min_" + name) for name in names if getattr(args, "min_" + name) is not None}
        return inspect.gate(args.database, args.run, expect=_expect(args.expect), min_activity=minima or None, answers=args.answers,
                            timeout=args.timeout, max_output_bytes=args.max_output_bytes)
    common = {"max_output_bytes": args.max_output_bytes, "no_content": args.no_content}
    if args.command == "runs":
        return inspect.runs(args.database, limit=args.limit, cursor=args.cursor, timeout=args.timeout, **common)
    if args.command == "sql":
        return inspect.query(args.database, args.sql, _params(args.params), limit=args.limit,
                             timeout=args.timeout, **common)
    _validate_scoped(args)
    if args.command == "payload" and args.no_content:
        return error_envelope("payload", "content_disabled", "no_content", "Explicit content access is disabled by this output policy.")
    run_id = args.run
    if run_id is None:
        selected = _infer_run(args)
        if not selected["ok"]:
            return selected
        run_id = selected["data"]["run_id"]
    return _scoped(args, run_id)


def exit_status(result):
    if not result["ok"]:
        return 3 if result["error"]["code"] in {"incompatible_schema", "recording_unreadable", "recording_open_wal"} else 1
    if result["operation"] == "gate":
        data = result["data"]
        if data.get("status") == "fail":
            return 2
        return 0 if data.get("status") == "pass" and data.get("evaluation_complete") is True else 4
    if result["operation"] == "check":
        return {"fail": 2, "unknown": 4}.get(result["data"]["status"], 0)
    return 0


def main():
    """Emit one result; expected errors never include arbitrary parser input."""
    arguments = sys.argv[1:]
    if arguments and arguments[0] == "proxy":
        # This transport owns stdout even for configuration failures and help.
        from ._proxy import main as proxy_main
        return proxy_main(arguments[1:])
    operation = _operation(arguments[0] if arguments else None)
    args = None
    try:
        try:
            args = _parser().parse_args(arguments)
            if args.command == "mcp":
                from ._mcp_inspect import serve
                serve(args.database, allow_content=args.allow_content)
                return 0
            result = _dispatch(args)
        except (InspectionError, argparse.ArgumentError) as error:
            if isinstance(error, InspectionError):
                result = error_envelope(operation, error.code, error.reason, error.message)
            else:
                result = error_envelope(operation, "invalid_request", "invalid_cli", "The command arguments are invalid; use fr --help.")
        if operation == "mcp":
            sys.stderr.write(result["error"]["message"] + "\n")
            return exit_status(result)
        if operation == "diagnose":
            result["format"] = DIAGNOSTIC_FORMAT
        as_text = args is not None and (getattr(args, "text", False) or (args.command == "doctor" and not args.json))
        if as_text:
            sys.stdout.write(render(result, args.max_output_bytes))
        else:
            serialized = encode(result).decode("utf-8")
            sys.stdout.write(serialized + "\n")
        return exit_status(result)
    except KeyboardInterrupt:
        return 130
