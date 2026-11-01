"""Allowlisted projections of recorded metadata; retained bodies stay explicit."""
from ._inspect_common import TABLE_COLUMNS, VIEW_COLUMNS, display, encode, json_value


HEALTH_REASONS = (
    "body_omitted", "capacity", "closed", "duplicate_completion", "finalize_busy", "finalize_timeout",
    "invalid_input", "invalid_scope", "other", "partial", "prerequisite_lost", "turn_update_lost",
    "unavailable_boundary", "unresolved", "writer_failed",
)
LOSS_REASONS = {
    "invalid_input", "invalid_scope", "unavailable_boundary", "capacity", "duplicate_completion",
    "prerequisite_lost", "writer_failed", "closed", "turn_update_lost",
}


def is_identity(name):
    return name.endswith(("_id", "_ref", "_pointer")) or name in {"condition_key", "message_path"}


def project(ctx, row, fields, path):
    result = {}
    for field in fields:
        value = row[field]
        pointer = path + "/" + field if path else ""
        if field in {"redacted", "truncated", "artifact_missing"} and value is not None:
            value = bool(value)
        result[field] = display(value, ctx, pointer, identity=is_identity(field))
    return result


def coverage(ctx, text, path, ids):
    raw = json_value(text)
    producers = raw.get("producers") if type(raw) is dict else None
    projected = {}
    for name in ("model", "host", "mcp"):
        present = type(producers) is dict and name in producers
        value = producers[name] if present else None
        valid = present and (value is None or (type(value) is str and bool(value)))
        if not valid:
            ctx.unknown("coverage_unavailable", path, ids, 1)
            value = None
        projected[name] = display(value, ctx, path, identity=False)
    complete = raw.get("model_complete") if type(raw) is dict else None
    if type(complete) is not bool:
        complete = None
        ctx.unknown("model_coverage_unavailable", path, ids, 1)
    return {"producers": projected, "model_complete": complete}


def health(ctx, text, path, ids):
    raw = json_value(text)
    group = raw.get("health") if type(raw) is dict else None
    counts = group.get("reason_counts") if type(group) is dict else None
    valid = {name: counts[name] for name in HEALTH_REASONS
             if type(counts) is dict and type(counts.get(name)) is int and counts[name] >= 0}
    complete = len(valid) == len(HEALTH_REASONS)
    if not complete:
        ctx.unknown("health_unavailable", path, ids, len(HEALTH_REASONS) - len(valid))
    return {"reason_counts": valid, "complete": complete}


def run_record(ctx, row, path=""):
    fields = ("run_id", "started_at", "ended_at", "close_state", "dropped_events", "writer_errors")
    result = project(ctx, row, fields, path)
    ids = {"run_id": row["run_id"]}
    result["coverage"] = coverage(ctx, row["coverage_json"], path + "/coverage" if path else "", ids)
    result["health"] = health(ctx, row["policy_json"], path + "/health" if path else "", ids)
    return result


def condition_value(ctx, row, path):
    if ctx.no_content:
        return None
    value = json_value(row["conditions_json"])
    try:
        size = len(encode(value))
    except (TypeError, ValueError, RecursionError):
        ctx.unknown("conditions_unavailable", path, {"turn_id": row["turn_id"]}, 1)
        return None
    if size > 512:
        ctx.cutoff("string_limit", path, {"turn_id": row["turn_id"]}, 1)
        return None
    return value


def turn_record(ctx, row, path=""):
    fields = [name for name in TABLE_COLUMNS["turns"] if name not in {"conditions_json", "coverage_json"}]
    result = project(ctx, row, fields, path)
    result["conditions"] = condition_value(ctx, row, path + "/conditions" if path else "")
    result["coverage"] = coverage(ctx, row["coverage_json"], path + "/coverage" if path else "", {"turn_id": row["turn_id"]})
    return result


def authoritative(ctx, row):
    return ctx.connection.execute("SELECT 1 FROM observed_events WHERE run_id=? AND event_id=?", (row["run_id"], row["event_id"])).fetchone() is not None


def event_complete(row):
    details = json_value(row["details_json"])
    value = details.get("complete") if type(details) is dict else None
    return value if type(value) is bool else None


def event_record(ctx, row):
    result = project(ctx, row, [name for name in TABLE_COLUMNS["events"] if name != "details_json"], "")
    result["authoritative"] = authoritative(ctx, row)
    result["complete"] = event_complete(row)
    if not result["authoritative"]:
        ctx.unknown("non_authoritative", "", {"event_id": row["event_id"]}, 1)
    return result


def operation_record(ctx, row):
    return project(ctx, row, TABLE_COLUMNS["operations"], "")


def artifact_record(ctx, row, path=""):
    return project(ctx, row, [name for name in TABLE_COLUMNS["artifacts"] if name != "body_text"], path)


def link_record(ctx, row):
    result = project(ctx, row, [name for name in TABLE_COLUMNS["links"] if name != "details_json"], "")
    details = json_value(row["details_json"])
    for name in ("binding_format", "message_artifact_id", "relative_pointer", "result_event_id"):
        value = details.get(name) if type(details) is dict else None
        result[name] = display(value if type(value) is str else None, ctx, "", identity=name != "binding_format")
    return result


def member_record(ctx, row):
    return project(ctx, row, VIEW_COLUMNS["request_members"], "")
