"""Bounded discovery, explicit call explanation and retained UTF-8 retrieval."""
import base64
import hashlib
import json

from . import __version__
from ._inspect_common import (
    Context, InspectionError, _context_envelope, encode, finish_envelope, invalid, run_read,
    validate_id, validate_int,
)
from ._inspect_query import QUERY_VALUE_BYTES
from ._inspect_records import (
    artifact_record, event_complete, event_record, link_record, operation_record,
    run_record, turn_record,
)
from ._inspect_relations import _answer_summary, _member_count, answer_rows, dispatch_rows, member_rows, request_info, witnessed_binding_failures


ERROR_STATUSES = {"tool_error", "protocol_error", "transport_error", "timeout", "local_error"}
FLAG_NAMES = ("tool_error", "text_mismatch", "unattributed_operation", "forwarded_change")
STRING_FILTERS = {"condition_key", "host_outcome", "capture_state"}
DETAIL_ARRAYS = ("events", "operations", "links", "artifacts", "members", "input_answers", "produced_answers", "dispatches")


def rows(ctx, sql, params):
    result = []
    cursor = ctx.connection.execute(sql, params)
    try:
        for row in cursor:
            ctx.check_deadline()
            result.append(row)
    finally:
        cursor.close()
    return result


def _filters(filters):
    if filters is None:
        return {}
    if type(filters) is not dict or any(key not in STRING_FILTERS | set(FLAG_NAMES) for key in filters):
        invalid("invalid_filters", "filters must contain only the documented field names.")
    for key, value in filters.items():
        if key in STRING_FILTERS:
            validate_id(value, key)
            if key == "capture_state" and value not in {"complete", "partial", "unknown"}:
                invalid("invalid_filters", "capture_state must be complete, partial or unknown.")
        elif type(value) is not bool:
            invalid("invalid_filters", "Issue filters must be booleans.")
    return {key: filters[key] for key in sorted(filters)}


CURSOR_CHARS = 8192


def _cursor_scope(ctx, filters, order):
    return {"version": 1, "operation": ctx.operation,
            "recording": hashlib.sha256(str(ctx.path).encode("utf-8")).hexdigest(),
            "run_id": ctx.run_id, "filters": filters, "no_content": ctx.no_content, "order": order}


def _read_cursor(token, expected):
    if token is None:
        return None
    try:
        if type(token) is not str or not token or len(token) > CURSOR_CHARS:
            raise ValueError
        decoded = base64.b64decode(token + "=" * (-len(token) % 4), altchars=b"-_", validate=True)
        value = json.loads(decoded)
        if type(value) is not dict or set(value) != set(expected) | {"last"}:
            raise ValueError
        if any(value[key] != item for key, item in expected.items()):
            raise ValueError
        last = value["last"]
        if type(last) is not list or len(last) != 2 or any(type(part) is not str for part in last):
            raise ValueError
        for part in last:
            part.encode("utf-8")
        return tuple(last)
    except (ValueError, TypeError, RecursionError):
        raise InspectionError("cursor_mismatch", "cursor_scope", "The cursor does not match this recording, scope, filters or policy.") from None


def _make_cursor(scope, last):
    token = base64.urlsafe_b64encode(encode({**scope, "last": list(last)})).rstrip(b"=").decode("ascii")
    if len(token) > CURSOR_CHARS:
        raise InspectionError("limit_exceeded", "cursor_size", "The required continuation cursor exceeds its character limit.")
    return token


def _page(ctx, items, ordering, has_more, scope):
    data = {"items": items, "next_cursor": None, "has_more": has_more}

    def update():
        data["next_cursor"] = _make_cursor(scope, ordering[-1]) if data["has_more"] and ordering else None

    def shrink():
        if len(items) <= 1:
            return False
        items.pop()
        ordering.pop()
        data["has_more"] = True
        update()
        return True

    if has_more:
        ctx.cutoff("item_limit", "/items", count=None)
    update()
    return data, shrink


def runs(path, *, limit=50, cursor=None, max_output_bytes=65536, no_content=False, timeout=10.0):
    def work(ctx):
        binding = _cursor_scope(ctx, {}, ["started_at", "run_id"])
        last = _read_cursor(cursor, binding)
        statement, params = "SELECT * FROM runs", ()
        if last is not None:
            statement += " WHERE (started_at,run_id)>(?,?)"
            params = last
        raw = rows(ctx, statement + " ORDER BY started_at,run_id LIMIT ?", (*params, limit + 1))
        selected = raw[:limit]
        items = [run_record(ctx, row, "/items/" + str(index)) for index, row in enumerate(selected)]
        return _page(ctx, items, [(row["started_at"], row["run_id"]) for row in selected], len(raw) > limit, binding)
    return run_read("runs", path, {}, work, max_output_bytes=max_output_bytes, no_content=no_content,
                    timeout=timeout, validate=lambda: validate_int(limit, 1, 200, "limit"))


def _turn_facts(ctx, run_id, turn_id):
    events = rows(ctx, "SELECT * FROM events WHERE run_id=? AND turn_id=? ORDER BY sequence,event_id", (run_id, turn_id))
    observed = rows(ctx, "SELECT * FROM observed_events WHERE run_id=? AND turn_id=? ORDER BY sequence,event_id", (run_id, turn_id))
    authoritative_ids = {row["event_id"] for row in observed}
    for row in events:
        if row["event_id"] not in authoritative_ids:
            ctx.unknown("non_authoritative", ids={"event_id": row["event_id"]}, count=1)
    operations = rows(ctx, "SELECT o.*,e.sequence AS start_sequence FROM operations o LEFT JOIN events e ON e.run_id=o.run_id AND e.event_id=o.start_event_id WHERE o.run_id=? AND o.turn_id=? ORDER BY e.sequence IS NULL,e.sequence,o.operation_id", (run_id, turn_id))
    requests = [row["event_id"] for row in observed if row["kind"] == "model.request"]
    decisions = [row["event_id"] for row in observed if row["kind"] == "model.tool_call"]
    dispatches = [row["event_id"] for row in observed if row["kind"] == "host.dispatch"]
    for request_id in requests:
        structure = request_info(ctx, run_id, request_id)
        if structure["status"] != "pass":
            ctx.unknown(structure["reason"], ids={"request_event_id": request_id}, count=1)
        for link, reason in witnessed_binding_failures(ctx, run_id, request_id, witness_info=structure):
            ctx.unknown(reason, ids={"request_event_id": request_id, "link_id": link["link_id"]}, count=1)
    request_members = _member_count(ctx, run_id, requests) if requests else 0
    input_answers, text_mismatch = _answer_summary(ctx, run_id, request_ids=requests) if requests else (0, False)
    produced_answers = _answer_summary(ctx, run_id, decision_ids=decisions)[0] if decisions else 0
    forwarded_change = False
    if dispatches:
        for row in dispatch_rows(ctx, run_id, dispatch_ids=dispatches):
            if row["forwarded_changed"] is True:
                forwarded_change = True
    calls = {row["model_call_id"] for row in observed if row["boundary"] == "model" and row["model_call_id"] is not None}
    calls.update(row["model_call_id"] for row in operations if row["model_call_id"] is not None)
    counts = {"events": len(events), "observed_events": len(observed), "model_calls": len(calls),
              "model_requests": len(requests), "model_responses": sum(row["kind"] == "model.response" for row in observed),
              "decisions": len(decisions), "operations": len(operations), "request_members": request_members,
              "input_answers": input_answers, "produced_answers": produced_answers}
    flags = {"tool_error": any(row["status"] in ERROR_STATUSES for row in operations),
             "text_mismatch": text_mismatch,
             "unattributed_operation": any(row["method"] == "tools/call" and row["link_basis"] == "none" for row in operations),
             "forwarded_change": forwarded_change}
    return {"observed": observed, "operations": operations, "calls": calls,
            "counts": counts, "flags": flags}


def find_turns(path, run_id, *, filters=None, limit=50, cursor=None, max_output_bytes=65536, no_content=False, timeout=10.0, last=False):
    normalized = {}
    def validate():
        validate_int(limit, 1, 200, "limit")
        if type(last) is not bool or (last and cursor is not None):
            invalid("invalid_last", "last must be a boolean and cannot be combined with a cursor.")
        normalized.update(_filters(filters))

    def work(ctx):
        binding = _cursor_scope(ctx, normalized, ["started_at", "turn_id"])
        previous = _read_cursor(cursor, binding)
        statement, params = "SELECT * FROM turns WHERE run_id=?", [run_id]
        if previous is not None:
            statement += " AND (started_at,turn_id)>(?,?)"
            params.extend(previous)
        for name in sorted(STRING_FILTERS & normalized.keys()):
            statement += " AND " + name + "=?"
            params.append(normalized[name])
        order = " ORDER BY started_at DESC,turn_id DESC" if last else " ORDER BY started_at,turn_id"
        raw = ctx.connection.execute(statement + order, params)
        selected = []
        try:
            for row in raw:
                ctx.check_deadline()
                facts = _turn_facts(ctx, run_id, row["turn_id"])
                if any(facts["flags"][name] != normalized[name] for name in FLAG_NAMES if name in normalized):
                    continue
                selected.append((row, facts))
                if last:
                    break
                if len(selected) > limit:
                    break
        finally:
            raw.close()
        items, ordering = [], []
        for index, (row, facts) in enumerate(selected[:limit]):
            items.append({"turn": turn_record(ctx, row, "/items/" + str(index) + "/turn"), "counts": facts["counts"], "flags": facts["flags"]})
            ordering.append((row["started_at"], row["turn_id"]))
        return _page(ctx, items, ordering, len(selected) > limit, binding)
    return run_read("find_turns", path, {"run_id": run_id}, work, max_output_bytes=max_output_bytes,
                    no_content=no_content, timeout=timeout, validate=validate)


def _call_complete(events):
    states = [event_complete(row) for row in events]
    if False in states:
        return False
    kinds = {row["kind"] for row in events}
    if {"model.request", "model.response"} <= kinds and all(state is True for state in states):
        return True
    return None


def _call_summary(ctx, call_id, facts):
    events = [row for row in facts["observed"] if row["boundary"] == "model" and row["model_call_id"] == call_id]
    decisions = [row for row in events if row["kind"] == "model.tool_call"]
    decision_ids = {row["decision_id"] for row in decisions if row["decision_id"] is not None}
    dispatch_ids = {row["dispatch_event_id"] for row in dispatch_rows(ctx, ctx.run_id, decision_ids=[row["event_id"] for row in decisions])} if decisions else set()
    operations = [row for row in facts["operations"] if _operation_matches(row, call_id, decision_ids, dispatch_ids)]
    result = {"model_call_id": call_id,
              "request_event_ids": [row["event_id"] for row in events if row["kind"] == "model.request"],
              "response_event_ids": [row["event_id"] for row in events if row["kind"] == "model.response"],
              "decision_event_ids": [row["event_id"] for row in events if row["kind"] == "model.tool_call"],
              "operation_ids": [row["operation_id"] for row in operations], "complete": _call_complete(events)}
    if result["complete"] is None:
        ctx.unknown("call_completeness_unavailable", ids={"model_call_id": call_id}, count=1)
    sequences = [row["sequence"] for row in events] + [row["start_sequence"] for row in operations if row["start_sequence"] is not None]
    return (min(sequences) if sequences else float("inf"), call_id), result


def _operation_matches(row, call_id, decision_ids, dispatch_ids):
    return row["model_call_id"] == call_id or row["decision_id"] in decision_ids or row["dispatch_event_id"] in dispatch_ids


def _related_operations(ctx, run_id, call_id, decision_rows, dispatch_ids):
    decision_ids = {row["decision_id"] for row in decision_rows if row["decision_id"] is not None}
    all_operations = rows(ctx, "SELECT o.*,e.sequence AS start_sequence FROM operations o LEFT JOIN events e ON e.run_id=o.run_id AND e.event_id=o.start_event_id WHERE o.run_id=? ORDER BY e.sequence IS NULL,e.sequence,o.operation_id", (run_id,))
    return [row for row in all_operations if _operation_matches(row, call_id, decision_ids, dispatch_ids)]


def _detail_events(ctx, run_id, call_id, operations, dispatch_ids):
    ids = set(dispatch_ids)
    for row in operations:
        ids.update(value for value in (row["start_event_id"], row["end_event_id"], row["dispatch_event_id"]) if value is not None)
    raw = rows(ctx, "SELECT * FROM events WHERE run_id=? ORDER BY sequence,event_id", (run_id,))
    return [row for row in raw if row["model_call_id"] == call_id or row["event_id"] in ids]


def _detail_links(ctx, run_id, event_ids):
    raw = rows(ctx, "SELECT l.*,e.sequence AS witness_sequence FROM links l LEFT JOIN events e ON e.run_id=l.run_id AND e.event_id=l.witness_event_id WHERE l.run_id=? ORDER BY e.sequence IS NULL,e.sequence,l.link_id", (run_id,))
    return [row for row in raw if any(row[name] in event_ids for name in ("source_event_id", "target_event_id", "witness_event_id"))]


def _detail_artifacts(ctx, run_id, events, answers):
    references = {row[field] for row in events for field in ("payload_ref", "catalog_ref") if row[field] is not None}
    for row in events:
        if row["kind"] == "model.request":
            references.update(request_info(ctx, run_id, row["event_id"])["artifact_ids"])
    references.update(row["message_artifact_id"] for row in answers if row["message_artifact_id"] is not None)
    artifacts = []
    for reference in sorted(references):
        ctx.check_deadline()
        row = ctx.connection.execute("SELECT * FROM artifacts WHERE run_id=? AND artifact_id=?", (run_id, reference)).fetchone()
        if row is None:
            ctx.unknown("missing_artifact", ids={"artifact_id": reference}, count=1)
        else:
            artifacts.append(row)
    return artifacts


def _call_detail(ctx, run_id, call_id, facts):
    model = [row for row in facts["observed"] if row["boundary"] == "model" and row["model_call_id"] == call_id]
    requests = [row["event_id"] for row in model if row["kind"] == "model.request"]
    decisions = [row for row in model if row["kind"] == "model.tool_call"]
    decision_ids = [row["event_id"] for row in decisions]
    dispatches = list(dispatch_rows(ctx, run_id, decision_ids=decision_ids)) if decision_ids else []
    dispatch_ids = {row["dispatch_event_id"] for row in dispatches}
    operations = _related_operations(ctx, run_id, call_id, decisions, dispatch_ids)
    events = _detail_events(ctx, run_id, call_id, operations, dispatch_ids)
    links = _detail_links(ctx, run_id, {row["event_id"] for row in events})
    members = list(member_rows(ctx, run_id, request_ids=requests)) if requests else []
    inputs = list(answer_rows(ctx, run_id, request_ids=requests)) if requests else []
    produced = list(answer_rows(ctx, run_id, decision_ids=decision_ids)) if decision_ids else []
    artifacts = _detail_artifacts(ctx, run_id, events, inputs + produced)
    arrays = {"events": [event_record(ctx, row) for row in events],
              "operations": [operation_record(ctx, row) for row in operations],
              "links": [link_record(ctx, row) for row in links],
              "artifacts": [artifact_record(ctx, row) for row in artifacts], "members": members,
              "input_answers": inputs, "produced_answers": produced, "dispatches": dispatches}
    totals = {name: len(arrays[name]) for name in DETAIL_ARRAYS}
    remaining = 200
    for name in DETAIL_ARRAYS:
        retained = arrays[name][:remaining]
        if len(retained) != len(arrays[name]):
            ctx.cutoff("item_limit", "/detail/" + name, count=len(arrays[name]) - len(retained))
        arrays[name] = retained
        remaining -= len(retained)
    arrays["totals"] = totals
    return arrays


def explain(path, run_id, turn_id, model_call_id=None, *, max_output_bytes=65536, no_content=False, timeout=10.0):
    scope = {"run_id": run_id, "turn_id": turn_id}
    if model_call_id is not None:
        scope["model_call_id"] = model_call_id

    def work(ctx):
        turn = ctx.connection.execute("SELECT * FROM turns WHERE run_id=? AND turn_id=?", (run_id, turn_id)).fetchone()
        if turn is None:
            raise InspectionError("unknown_turn", "unknown_turn", "The selected turn is unavailable in this run.")
        facts = _turn_facts(ctx, run_id, turn_id)
        summaries = [item[1] for item in sorted(_call_summary(ctx, identifier, facts) for identifier in facts["calls"])]
        if model_call_id is not None and model_call_id not in facts["calls"]:
            raise InspectionError("unknown_event", "unknown_model_call", "The selected model call is unavailable in this turn.")
        assigned_ids = {identifier for summary in summaries for identifier in summary["operation_ids"]}
        unassigned = [operation_record(ctx, row) for row in facts["operations"] if row["operation_id"] not in assigned_ids]
        if len(unassigned) > 200:
            ctx.cutoff("item_limit", "/unassigned_operations", count=len(unassigned) - 200)
        if model_call_id is None and len(summaries) > 50:
            ctx.cutoff("item_limit", "/calls", count=len(summaries) - 50)
        selected = [row for row in summaries if row["model_call_id"] == model_call_id] if model_call_id is not None else summaries[:50]
        data = {"turn": turn_record(ctx, turn, "/turn"), "counts": facts["counts"], "flags": facts["flags"],
                "calls": selected, "unassigned_operations": unassigned[:200],
                "detail": _call_detail(ctx, run_id, model_call_id, facts) if model_call_id is not None else None}

        def shrink():
            if data["detail"] is not None:
                for name in reversed(DETAIL_ARRAYS):
                    if data["detail"][name]:
                        data["detail"][name].pop()
                        return True
            if data["unassigned_operations"]:
                data["unassigned_operations"].pop()
                return True
            if model_call_id is None and len(data["calls"]) > 1:
                data["calls"].pop()
                return True
            return False
        return data, shrink
    return run_read("explain", path, scope, work, max_output_bytes=max_output_bytes, no_content=no_content, timeout=timeout)


def payload(path, run_id, artifact_id, offset=0, max_bytes=4096, *, max_output_bytes=65536, no_content=False, timeout=10.0):
    def validate():
        validate_int(offset, 0, 2 ** 63 - 1, "offset")
        validate_int(max_bytes, 4, 65536, "max_bytes")

    def work(ctx):
        row = ctx.connection.execute("SELECT * FROM artifacts WHERE run_id=? AND artifact_id=?", (run_id, artifact_id)).fetchone()
        if row is None:
            raise InspectionError("unknown_artifact", "unknown_artifact", "The selected artifact is unavailable in this run.")
        if row["body_text"] is None:
            raise InspectionError("payload_omitted", "retention_omission", "This artifact has no retained body.")
        raw = row["body_text"].encode("utf-8")
        if offset > len(raw):
            invalid("invalid_offset", "offset exceeds retained UTF-8 content.")
        try:
            raw[:offset].decode("utf-8")
        except UnicodeDecodeError:
            invalid("invalid_offset", "offset must be a UTF-8 character boundary.")
        window = raw[offset:offset + max_bytes].decode("utf-8", errors="ignore")
        data = {"artifact": artifact_record(ctx, row, "/artifact"), "offset": offset,
                "returned_bytes": len(window.encode("utf-8")), "total_bytes": len(raw), "text": window, "next_offset": None}
        fitting_done = False

        def update():
            data["returned_bytes"] = len(data["text"].encode("utf-8"))
            end = offset + data["returned_bytes"]
            data["next_offset"] = end if end < len(raw) else None

        def shrink():
            nonlocal fitting_done
            if fitting_done or len(window) <= 1:
                return False
            fitting_done = True
            low, high, best = 1, len(window) - 1, 1
            # Proper positive prefixes keep integer continuation offsets and
            # monotonic encoded size. Diagnostics stay fixed during this fit.
            while low <= high:
                middle = (low + high) // 2
                data["text"] = window[:middle]
                update()
                ctx.check_deadline()
                fits = len(encode(_context_envelope(ctx, data))) <= ctx.max_output_bytes
                ctx.check_deadline()
                if fits:
                    best = middle
                    low = middle + 1
                else:
                    high = middle - 1
            # If no prefix fits, the common owner may drop diagnostics around
            # this one character; later shrink calls must never grow it back.
            data["text"] = window[:best]
            update()
            return True
        update()
        return data, shrink
    return run_read("payload", path, {"run_id": run_id, "artifact_id": artifact_id}, work,
                    max_output_bytes=max_output_bytes, no_content=no_content, timeout=timeout, validate=validate, content=True)


def schema():
    common = {"max_output_bytes": 65536, "no_content": False}
    specifications = [
        ("check", "check", ["path", "run_id"], {"expect": None, "timeout": 10.0, **common}, "reader"),
        ("find_turns", "turns", ["path", "run_id"], {"filters": None, "limit": 50, "cursor": None, "timeout": 10.0, **common}, "reader"),
        ("explain", "explain", ["path", "run_id", "turn_id"], {"model_call_id": None, "timeout": 10.0, **common}, "reader"),
        ("payload", "payload", ["path", "run_id", "artifact_id"], {"offset": 0, "max_bytes": 4096, "timeout": 10.0, **common}, "reader"),
        ("slice", "slice", ["path", "run_id", "request_event_id"], {"limit": 1000, "max_depth": 12, "timeout": 10.0, **common}, "reader"),
        ("query", "sql", ["path", "sql"], {"params": None, "limit": 200, "timeout": 10.0, **common}, "reader"),
        ("runs", "runs", ["path"], {"limit": 50, "cursor": None, "timeout": 10.0, **common}, "helper"),
        ("schema", "schema", [], {}, "helper"),
        ("snapshot", "snapshot", ["path", "output"], {"timeout": 10.0}, "local_copy"),
        ("gate", "gate", ["path"], {"run_id": None, "expect": None, "min_activity": None,
                                     "timeout": 10.0, "max_output_bytes": 65536}, "evidence_gate"),
    ]
    operations = {name: {"python": name, "cli": cli, "required": required,
                         "optional_defaults": defaults, "designation": designation}
                  for name, cli, required, defaults, designation in specifications}
    errors = {code: 1 for code in ("invalid_request", "unknown_run", "unknown_turn", "unknown_event", "unknown_artifact",
              "cursor_mismatch", "payload_omitted", "content_disabled", "sql_rejected", "query_timeout", "limit_exceeded")}
    errors.update({code: 3 for code in ("incompatible_schema", "recording_unreadable", "recording_open_wal")})
    limits = {"max_output_bytes": {"default": 65536, "min": 1024, "max": 1048576}, "display_string_bytes": 512,
              "runs_items": {"default": 50, "min": 1, "max": 200}, "find_turns_items": {"default": 50, "min": 1, "max": 200},
              "cursor_chars": CURSOR_CHARS, "explain_calls": 50, "explain_detail_records": 200, "check_witnesses": 10,
              "slice_records": {"default": 1000, "min": 1, "max": 1000}, "slice_depth": {"default": 12, "min": 0, "max": 12},
              "payload_bytes": {"default": 4096, "min": 4, "max": 65536}, "query_rows": {"default": 200, "min": 1, "max": 1000},
              "query_sql_bytes": 65536, "query_value_bytes": QUERY_VALUE_BYTES,
              "diagnostics_per_kind": 20, "db_deadline_seconds": 10,
              "scoped_reader_timeout": {"default": 10.0, "min_exclusive": 0, "max": 30},
              "query_snapshot_timeout": {"default": 10.0, "min_exclusive": 0, "max": 30}, "busy_wait_ms": 1000}
    data = {"api_format": "mfr.inspect/1", "database_schema_version": 6, "package_version": __version__,
            "operations": operations, "limits": limits, "errors": errors,
            "check_statuses": {"values": ["pass", "fail", "unknown", "not_applicable"], "reserved": ["warn"],
                               "exits": {"pass": 0, "not_applicable": 0, "unknown": 4, "fail": 2}},
            "gate_statuses": {"values": ["pass", "fail", "unknown"],
                              "exits": {"pass": 0, "fail": 2, "unknown": 4},
                              "required_boundaries": ["model", "host"],
                              "activity_counts": ["turns", "model_requests", "model_responses", "decisions",
                                                  "tool_calls", "answer_occurrences"]},
            "expect_boundaries": ["model", "host", "mcp"],
            "content_policy": {"default": "metadata", "explicit_content": ["payload", "query"],
                               "no_content_disables": ["payload", "query"], "conditions_max_bytes": 512, "untrusted_content": True}}
    return finish_envelope(Context("schema", {}), data)
