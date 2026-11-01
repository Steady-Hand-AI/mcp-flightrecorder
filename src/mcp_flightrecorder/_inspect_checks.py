"""Nine fixed integrity and availability checks over one owned read snapshot."""
from datetime import datetime
import hashlib
import math
import re

from ._inspect_common import invalid, json_object, run_read
from ._inspect_records import HEALTH_REASONS, LOSS_REASONS, event_complete, run_record
from ._inspect_relations import MEMBERS_SQL, answer_rows, association_diagnostics_reason, dispatch_rows, event_row, relation_state, request_info, witnessed_binding_failures


def _item(identifier, unit):
    return {"id": identifier, "required": True, "status": "not_applicable", "unit": unit,
            "applicable": 0, "observed": 0, "unknown": 0, "failed": 0, "witnesses": []}


def _count(item, status, ids, reason):
    if status == "not_applicable":
        return
    item["applicable"] += 1
    item["unknown" if status == "unknown" else "observed"] += 1
    if status == "fail":
        item["failed"] += 1
    if status in {"fail", "unknown"} and len(item["witnesses"]) < 10:
        item["witnesses"].append({"ids": ids or {}, "reason": reason or status})


def _status(item):
    return "fail" if item["failed"] else "unknown" if item["unknown"] else "pass" if item["applicable"] else "not_applicable"


def _coverage_state(raw, name, expect):
    producers = raw.get("producers")
    if type(producers) is not dict or name not in producers:
        return "unknown", "coverage_missing"
    value = producers[name]
    if value is None:
        return ("fail", "expected_producer_unavailable") if name in expect else ("not_applicable", None)
    if type(value) is str and value:
        return "pass", None
    return "unknown", "coverage_malformed"


def _health_state(ctx, run):
    policy = json_object(run["policy_json"])
    health = policy.get("health")
    counts = health.get("reason_counts") if type(health) is dict else None
    counts = counts if type(counts) is dict else {}
    valid = {key: counts[key] for key in HEALTH_REASONS if type(counts.get(key)) is int and counts[key] >= 0}
    if any(type(run[key]) is int and run[key] > 0 for key in ("dropped_events", "writer_errors")):
        return "fail", "recorded_capture_loss"
    if any(valid.get(key, 0) > 0 for key in LOSS_REASONS):
        return "fail", "recorded_capture_loss"
    if len(valid) != len(HEALTH_REASONS):
        return "unknown", "health_counters_unavailable"
    if any(type(run[key]) is not int or run[key] < 0 for key in ("dropped_events", "writer_errors")):
        return "unknown", "health_totals_unavailable"
    if run["close_state"] != "clean" or not run["ended_at"]:
        return "unknown", "capture_not_closed_cleanly"
    if any(valid[key] > 0 for key in ("partial", "unresolved", "other")):
        return "unknown", "partial_capture_diagnostics"
    for turn in ctx.connection.execute("SELECT * FROM turns WHERE run_id=?", (ctx.run_id,)):
        ctx.check_deadline()
        if not turn["ended_at"] or turn["capture_state"] != "complete":
            return "unknown", "turn_capture_incomplete"
        if json_object(turn["coverage_json"]).get("model_complete") is not True:
            return "unknown", "turn_coverage_incomplete"
    for event in ctx.connection.execute("SELECT details_json FROM observed_events WHERE run_id=?", (ctx.run_id,)):
        ctx.check_deadline()
        if event_complete(event) is not True:
            return "unknown", "observation_incomplete"
    return "pass", None


def _storage_state(artifact):
    body, digest, size = artifact["body_text"], artifact["stored_sha256"], artifact["stored_bytes"]
    if body is None:
        if digest is not None or size != 0:
            return "fail", "contradictory_omission_storage"
        if type(artifact["omitted_reason"]) is not str or not artifact["omitted_reason"]:
            return "unknown", "omission_reason_unavailable"
        return "pass", None
    if type(body) is not str:
        return "unknown", "retained_text_unavailable"
    if artifact["omitted_reason"] is not None:
        return "fail", "contradictory_retained_omission"
    try:
        data = body.encode("utf-8")
    except UnicodeEncodeError:
        return "fail", "invalid_retained_utf8"
    if type(size) is not int:
        return "unknown", "stored_length_unavailable"
    if size != len(data):
        return "fail", "stored_length_mismatch"
    if type(digest) is not str or re.fullmatch("[0-9a-f]{64}", digest) is None:
        return "unknown", "stored_digest_unavailable"
    if hashlib.sha256(data).hexdigest() != digest:
        return "fail", "stored_digest_mismatch"
    return "pass", None


def _time(value):
    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError, OverflowError):
        return None


def _operation_state(ctx, operation):
    start = event_row(ctx, ctx.run_id, operation["start_event_id"], observed=True)
    end = event_row(ctx, ctx.run_id, operation["end_event_id"], observed=True)
    status = operation["status"]
    terminal = {"ok", "tool_error", "protocol_error", "transport_error", "local_error", "timeout", "cancelled"}
    if status not in terminal | {"running", "incomplete"}:
        return "fail", "invalid_operation_status"
    if status in {"running", "incomplete"} and any(operation[field] is not None for field in ("end_event_id", "ended_at", "duration_ms")):
        return "fail", "contradictory_open_operation"
    identities = ("run_id", "operation_id", "turn_id", "model_call_id", "decision_id", "client_id", "server_key", "method", "name")
    for event, kind in ((start, "mcp.request"), (end, "mcp.response")):
        if event is not None and (event["kind"] != kind or any(event[field] != operation[field] for field in identities)):
            return "fail", "contradictory_operation_identity"
    if start is not None and start["observed_at"] != operation["started_at"]:
        return "fail", "contradictory_operation_start"
    if end is not None and (end["state"] != status or end["observed_at"] != operation["ended_at"]):
        return "fail", "contradictory_operation_terminal"
    if start is not None and end is not None and start["sequence"] >= end["sequence"]:
        return "fail", "contradictory_operation_order"
    duration = operation["duration_ms"]
    if duration is not None and (type(duration) not in (float, int) or not math.isfinite(duration) or duration < 0):
        return "fail", "contradictory_operation_duration"
    if start is not None and end is not None:
        first, last = start["observed_mono_ms"], end["observed_mono_ms"]
        if type(first) in (int, float) and type(last) in (int, float) and last < first:
            return "fail", "contradictory_operation_order"
    first, last = _time(operation["started_at"]), _time(operation["ended_at"])
    if first is not None and last is not None:
        try:
            if last < first:
                return "fail", "contradictory_operation_order"
        except TypeError:
            return "unknown", "operation_time_unavailable"
    if operation["link_basis"] == "none" and (operation["decision_id"] is not None or operation["dispatch_event_id"] is not None):
        return "fail", "contradictory_operation_attribution"
    if operation["link_basis"] == "host_hook" and operation["dispatch_event_id"] is None:
        return "fail", "contradictory_operation_attribution"
    if operation["link_basis"] == "propagated" and (operation["decision_id"] is None or operation["dispatch_event_id"] is not None):
        return "fail", "contradictory_operation_attribution"
    if operation["link_basis"] not in {"none", "host_hook", "propagated"}:
        return "fail", "contradictory_operation_attribution"
    if start is None or (status in terminal and end is None):
        return "unknown", "operation_witness_unavailable"
    if status in {"running", "incomplete"}:
        return "unknown", "operation_terminal_unavailable"
    if duration is None or first is None or last is None:
        return "unknown", "operation_time_unavailable"
    return "pass", None


def request_binding_state(ctx, info, states):
    """One request unit; observed rows never stand in for missing populations."""
    if info["status"] == "fail":
        return "fail", info["reason"]
    if info["event"] is not None:
        for _link, reason in witnessed_binding_failures(ctx, ctx.run_id, info["event"]["event_id"]):
            return "fail", reason
    if "invalid_binding" in states:
        return "fail", "invalid_binding"
    if info["status"] == "unknown":
        return "unknown", info["reason"]
    if any(state != "linked" for state in states):
        return "unknown", "unresolved_answer_occurrence"
    details = json_object(info["event"]["details_json"])
    reason = association_diagnostics_reason(details)
    return ("unknown", reason) if reason else ("pass", None)


def _stats(ctx):
    result = {}
    for name in ("turns", "events", "observed_events", "artifacts", "operations", "links", "findings"):
        ctx.check_deadline()
        result[name] = ctx.connection.execute("SELECT count(*) FROM " + name + " WHERE run_id=?", (ctx.run_id,)).fetchone()[0]
    result["model_calls"] = ctx.connection.execute("SELECT count(DISTINCT model_call_id) FROM observed_events WHERE run_id=? AND boundary='model'", (ctx.run_id,)).fetchone()[0]
    for key, kind in (("model_requests", "model.request"), ("model_responses", "model.response"), ("decisions", "model.tool_call")):
        result[key] = ctx.connection.execute("SELECT count(*) FROM observed_events WHERE run_id=? AND kind=?", (ctx.run_id, kind)).fetchone()[0]
    result["request_members"] = ctx.connection.execute("WITH members AS (" + MEMBERS_SQL + ") SELECT count(*) FROM members WHERE run_id=?", (ctx.run_id,)).fetchone()[0]
    for key in ("answer_occurrences", "linked_answers", "unresolved_answers", "text_equal", "text_mismatch", "text_unknown", "forwarded_changed", "forwarded_equal", "forwarded_unknown"):
        result[key] = 0
    return result


def _check(ctx, expect):
    run = ctx.connection.execute("SELECT * FROM runs WHERE run_id=?", (ctx.run_id,)).fetchone()
    run_data = run_record(ctx, run, "/run")
    items = [_item("coverage." + name, "declaration") for name in ("model", "host", "mcp")]
    items += [_item(name, unit) for name, unit in (("capture_health", "run"), ("artifact_storage", "artifact"),
               ("request_structure", "model.request"), ("operation_lifecycle", "operation"), ("links", "link"),
               ("answer_bindings", "model.request"))]
    for item, name in zip(items, ("model", "host", "mcp")):
        status, reason = _coverage_state(json_object(run["coverage_json"]), name, expect)
        _count(item, status, {"run_id": ctx.run_id}, reason)
    health_status, health_reason = _health_state(ctx, run)
    _count(items[3], health_status, {"run_id": ctx.run_id}, health_reason)
    stats = _stats(ctx)
    for artifact in ctx.connection.execute("SELECT * FROM artifacts WHERE run_id=? ORDER BY artifact_id", (ctx.run_id,)):
        ctx.check_deadline()
        status, reason = _storage_state(artifact)
        _count(items[4], status, {"artifact_id": artifact["artifact_id"]}, reason)
    request_states = {}
    for occurrence in answer_rows(ctx, ctx.run_id):
        request_states.setdefault(occurrence["request_event_id"], set()).add(occurrence["association_state"])
        stats["answer_occurrences"] += 1
        stats["linked_answers" if occurrence["association_state"] == "linked" else "unresolved_answers"] += 1
        stats["text_unknown" if occurrence["text_equal"] is None else "text_equal" if occurrence["text_equal"] else "text_mismatch"] += 1
    for request in ctx.connection.execute("SELECT event_id FROM observed_events WHERE run_id=? AND kind='model.request' ORDER BY sequence,event_id", (ctx.run_id,)):
        info = request_info(ctx, ctx.run_id, request["event_id"])
        _count(items[5], info["status"], {"request_event_id": request["event_id"]}, info["reason"])
        status, reason = request_binding_state(ctx, info, request_states.get(request["event_id"], ()))
        _count(items[8], status, {"request_event_id": request["event_id"]}, reason)
    # Reuse request/binding facts before operation-only event rows enter the
    # same bounded LRU. Fixed item positions and their witness order stay intact.
    for link in ctx.connection.execute("SELECT l.* FROM links l LEFT JOIN events w ON w.run_id=l.run_id AND w.event_id=l.witness_event_id WHERE l.run_id=? ORDER BY w.sequence IS NULL,w.sequence,l.link_id", (ctx.run_id,)):
        status, reason = relation_state(ctx, ctx.run_id, link)
        _count(items[7], status, {"link_id": link["link_id"]}, reason)
    for operation in ctx.connection.execute("SELECT * FROM operations WHERE run_id=? ORDER BY started_at,operation_id", (ctx.run_id,)):
        ctx.check_deadline()
        status, reason = _operation_state(ctx, operation)
        _count(items[6], status, {"operation_id": operation["operation_id"]}, reason)
    for dispatch in dispatch_rows(ctx, ctx.run_id):
        stats["forwarded_unknown" if dispatch["forwarded_changed"] is None else "forwarded_changed" if dispatch["forwarded_changed"] else "forwarded_equal"] += 1
    for event in ctx.connection.execute("SELECT e.event_id FROM events e LEFT JOIN observed_events o ON o.run_id=e.run_id AND o.event_id=e.event_id WHERE e.run_id=? AND o.event_id IS NULL ORDER BY e.sequence,e.event_id", (ctx.run_id,)):
        ctx.check_deadline()
        ctx.unknown("non_authoritative", ids={"event_id": event["event_id"]}, count=1)
    for index, item in enumerate(items):
        item["status"] = _status(item)
        missing = item["unknown"] + item["failed"] - len(item["witnesses"])
        if missing:
            ctx.cutoff("witness_limit", "/items/" + str(index) + "/witnesses", count=missing)
        if item["unknown"]:
            ctx.unknown("check_unresolved", "/items/" + str(index), {"check_id": item["id"]}, item["unknown"])
    failures = [item["id"] for item in items if item["status"] == "fail"]
    unknowns = [item["id"] for item in items if item["status"] == "unknown"]
    data = {"run": run_data, "status": "fail" if failures else "unknown" if unknowns else "pass",
            "items": items, "stats": stats, "required_failures": failures, "required_unknowns": unknowns}

    def shrink():
        for item in reversed(items):
            if item["witnesses"]:
                item["witnesses"].pop()
                return True
        return False

    return data, shrink


def check(path, run_id, expect=None, *, max_output_bytes=65536, no_content=False, timeout=10.0):
    def validate():
        if expect is None:
            return
        if type(expect) not in (list, tuple, set, frozenset) or any(type(item) is not str or item not in {"model", "host", "mcp"} for item in expect) or len(set(expect)) != len(expect):
            invalid("invalid_expect", "expect must contain distinct model, host or mcp boundary names.")

    return run_read("check", path, {"run_id": run_id}, lambda ctx: _check(ctx, expect or ()),
                    max_output_bytes=max_output_bytes, no_content=no_content, timeout=timeout, validate=validate)
