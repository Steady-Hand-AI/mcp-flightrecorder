"""One conservative host-evidence policy over a complete owned read snapshot."""
from ._inspect_common import InspectionError, invalid, json_object, run_read, validate_int
from ._inspect_checks import _check, _count, _item, _status
from ._inspect_diagnostics import _preparation
from ._inspect_records import event_complete
from ._inspect_relations import answer_rows, decision_state, event_row, relation_state


ACTIVITY = ("turns", "model_requests", "model_responses", "decisions", "tool_calls", "answer_occurrences")
_BOUNDARIES = {
    "model.request": "model", "model.response": "model", "model.tool_call": "model",
    "host.dispatch": "host", "host.tool_result": "host",
    "mcp.request": "mcp", "mcp.response": "mcp",
}


def _attribution(ctx, operation):
    """Validate explicit identities, not name/argument similarity or an ID alone."""
    start = event_row(ctx, ctx.run_id, operation["start_event_id"], observed=True)
    if start is None:
        return "unknown", "operation_witness_unavailable"
    decision = None
    if operation["decision_id"] is not None:
        rows = ctx.connection.execute("""SELECT * FROM observed_events
            WHERE run_id=? AND decision_id=? AND kind='model.tool_call' LIMIT 2""",
            (ctx.run_id, operation["decision_id"])).fetchall()
        if len(rows) > 1:
            return "fail", "decision_identity_conflict"
        decision = rows[0] if rows else None
        state, reason = decision_state(ctx, ctx.run_id, decision, start)
        if state != "pass":
            return state, reason
        if operation["model_call_id"] != decision["model_call_id"]:
            return "fail", "operation_model_conflict"
    if operation["link_basis"] == "propagated":
        return ("pass", None) if decision is not None else ("unknown", "decision_unavailable")
    if operation["link_basis"] != "host_hook":
        return "unknown", "operation_attribution_unavailable"
    dispatch = event_row(ctx, ctx.run_id, operation["dispatch_event_id"], observed=True)
    if dispatch is None:
        return "unknown", "dispatch_unavailable"
    if dispatch["kind"] != "host.dispatch" or dispatch["boundary"] != "host":
        return "fail", "wrong_dispatch_kind"
    if dispatch["sequence"] >= start["sequence"] or dispatch["state"] not in {"execute", "local"}:
        return "fail", "dispatch_execution_conflict"
    if event_complete(dispatch) is not True:
        return "unknown", "dispatch_incomplete"
    sources = json_object(dispatch["details_json"]).get("decision_event_ids")
    if type(sources) is not list or any(type(value) is not str or not value for value in sources):
        return "unknown", "dispatch_sources_unavailable"
    links = ctx.connection.execute("""SELECT * FROM links WHERE run_id=?
        AND relation='dispatch_source' AND target_event_id=? ORDER BY link_id""",
        (ctx.run_id, dispatch["event_id"])).fetchall()
    if set(sources) != {link["source_event_id"] for link in links}:
        return "fail", "dispatch_sources_conflict"
    source_models = {}
    for link in links:
        ctx.check_deadline()
        state, reason = relation_state(ctx, ctx.run_id, link)
        if state != "pass":
            return state, reason
        source = event_row(ctx, ctx.run_id, link["source_event_id"], observed=True)
        state, reason = decision_state(ctx, ctx.run_id, source, dispatch)
        if state != "pass":
            return state, reason
        source_models[source["event_id"]] = source["model_call_id"]
    if decision is not None and decision["event_id"] not in source_models:
        return "fail", "dispatch_decision_conflict"
    if decision is None:
        expected_model = next(iter(source_models.values())) if len(source_models) == 1 else None
        if operation["model_call_id"] != expected_model:
            return "fail", "dispatch_model_conflict"
    # An authoritative, complete dispatch explicitly declaring no decision
    # sources is a host-initiated operation, rather than a fabricated choice.
    return "pass", None


def _comparison(ctx, answer):
    if answer["origin_status"] != "pass":
        return answer["origin_status"], answer["origin_reason"]
    if answer["origin"] != "mcp":
        return "unknown", "result_unavailable"
    if answer["text_equal"] is True:
        return "pass", None
    if answer["text_equal"] is None:
        return "unknown", answer["comparison_reason"]
    insertions = ctx.connection.execute("""SELECT * FROM links WHERE run_id=?
        AND relation='inserted_into' AND target_event_id=? AND target_pointer=? ORDER BY link_id LIMIT 2""",
        (ctx.run_id, answer["request_event_id"], answer["target_pointer"])).fetchall()
    if not insertions:
        return "fail", "text_mismatch"
    if len(insertions) != 1:
        return "unknown", "preparation_ambiguous"
    prepared = _preparation(ctx, ctx.run_id, insertions[0])
    if prepared["state"] == "matches":
        return "pass", None
    if prepared["state"] == "differs":
        return "fail", "prepared_text_mismatch"
    return "unknown", prepared["reason"] or "preparation_unavailable"


def _gate_relation(ctx, run_id, link):
    state, reason = relation_state(ctx, run_id, link)
    if link["relation"] == "inserted_into" and state == "unknown" and reason == "pointer_content_unavailable":
        # Omitted member bodies alone cannot prove a pointer. Phase-1's exact
        # occurrence, source and original-token validation can establish it.
        prepared = _preparation(ctx, run_id, link)
        if prepared["state"] in {"matches", "differs"}:
            return "pass", None
    return state, reason


def _gate(ctx, expect, minimum, answers):
    if ctx.run_id is None:
        rows = ctx.connection.execute("SELECT run_id FROM runs LIMIT 2").fetchall()
        if len(rows) != 1:
            raise InspectionError("invalid_request", "ambiguous_run", "Select an explicit run when the recording does not contain exactly one run.")
        ctx.scope["run_id"] = ctx.run_id = rows[0]["run_id"]
    data, shrink = _check(ctx, expect, dispatch_comparisons=False, link_validator=_gate_relation)
    items, stats = data["items"], data["stats"]
    extra = {name: _item(name, unit) for name, unit in (
        ("model_coverage", "declaration"), ("complete_turns", "turn"),
        ("authoritative_observations", "event"), ("answer_comparisons", "answer_occurrence"),
        ("tool_attribution", "tools/call"), ("tool_witnesses", "mcp.event"),
        ("minimum_activity", "expectation"),
        ("evidence_bounds", "evaluation"))}
    items.extend(extra.values())
    run = ctx.connection.execute("SELECT coverage_json FROM runs WHERE run_id=?", (ctx.run_id,)).fetchone()
    complete = json_object(run["coverage_json"]).get("model_complete")
    _count(extra["model_coverage"], "pass" if complete is True else "unknown",
           {"run_id": ctx.run_id}, "model_coverage_unavailable")
    for turn in ctx.connection.execute("SELECT * FROM turns WHERE run_id=? ORDER BY started_at,turn_id", (ctx.run_id,)):
        ctx.check_deadline()
        complete = (turn["ended_at"] and turn["capture_state"] == "complete"
                    and json_object(turn["coverage_json"]).get("model_complete") is True)
        _count(extra["complete_turns"], "pass" if complete else "unknown",
               {"turn_id": turn["turn_id"]}, "turn_incomplete")
    for event in ctx.connection.execute("""SELECT e.*,o.event_id AS authoritative
            FROM events e LEFT JOIN observed_events o ON o.run_id=e.run_id AND o.event_id=e.event_id
            WHERE e.run_id=? ORDER BY e.sequence,e.event_id""", (ctx.run_id,)):
        ctx.check_deadline()
        reason = "non_authoritative" if not event["authoritative"] else "observation_incomplete" if event_complete(event) is not True else None
        state = "unknown" if reason else "pass"
        if event["kind"] in _BOUNDARIES and event["boundary"] != _BOUNDARIES[event["kind"]]:
            state, reason = "fail", "event_boundary_conflict"
        _count(extra["authoritative_observations"], state,
               {"event_id": event["event_id"]}, reason)
    excluded, answer_witnesses = [], []
    excluded_count = mcp_answer_count = answer_count = 0
    for answer in answer_rows(ctx, ctx.run_id):
        answer_count += 1
        mcp_answer_count += answer["origin"] == "mcp"
        ids = {key: answer[key] for key in ("request_event_id", "source_event_id", "result_event_id",
               "dispatch_event_id", "binding_witness_event_id", "link_id", "target_pointer")}
        if answers == "mcp" and answer["origin"] == "local":
            excluded_count += 1
            state, reason = "excluded", "local_result_comparison_excluded"
            if len(excluded) < 10:
                excluded.append({"ids": ids, "reason": reason})
        else:
            state, reason = _comparison(ctx, answer)
            _count(extra["answer_comparisons"], state, ids, reason)
        if len(answer_witnesses) < 10:
            answer_witnesses.append({"ids": ids, "origin": answer["origin"],
                "origin_reason": answer["origin_reason"], "status": state, "reason": reason,
                "text_equal": answer["text_equal"], "comparison_reason": answer["comparison_reason"],
                "preparation": "matches" if state == "pass" and answer["text_equal"] is False else "unknown"})
    if answers == "mcp":
        scoped = _item("mcp_answer_scope", "answer_occurrence")
        items.append(scoped)
        _count(scoped, "pass" if mcp_answer_count else "unknown", {}, "no_mcp_answers_in_scope")
    for operation in ctx.connection.execute("SELECT * FROM operations WHERE run_id=? AND method='tools/call' ORDER BY started_at,operation_id", (ctx.run_id,)):
        ctx.check_deadline()
        state, reason = _attribution(ctx, operation)
        _count(extra["tool_attribution"], state, {"operation_id": operation["operation_id"]}, reason)
    # Visit every endpoint, including extra endpoints reusing an otherwise
    # valid operation. Operation-row validation alone cannot see those claims.
    identities = ("turn_id", "model_call_id", "decision_id", "client_id", "server_key", "method", "name")
    for event in ctx.connection.execute("""SELECT * FROM observed_events
            WHERE run_id=? AND method='tools/call' AND kind IN ('mcp.request','mcp.response')
            ORDER BY sequence,event_id""", (ctx.run_id,)):
        ctx.check_deadline()
        operation = ctx.connection.execute("SELECT * FROM operations WHERE run_id=? AND operation_id=?",
                                           (ctx.run_id, event["operation_id"])).fetchone()
        state, reason = "pass", None
        if operation is None:
            state, reason = "unknown", "operation_unavailable"
        elif event["boundary"] != "mcp":
            state, reason = "fail", "operation_boundary_conflict"
        elif (event["event_id"] != operation["start_event_id" if event["kind"] == "mcp.request" else "end_event_id"]
              or any(event[field] != operation[field] for field in identities)):
            state, reason = "fail", "operation_witness_conflict"
        _count(extra["tool_witnesses"], state, {"event_id": event["event_id"]}, reason)
    stats["tool_calls"] = ctx.connection.execute("SELECT count(*) FROM observed_events WHERE run_id=? AND kind='mcp.request' AND method='tools/call'", (ctx.run_id,)).fetchone()[0]
    actual = {name: stats[name] for name in ACTIVITY}
    for name, value in minimum.items():
        _count(extra["minimum_activity"], "pass" if actual[name] >= value else "fail",
               {"count": name}, "minimum_not_met")
    # Sampling failure witnesses is safe because the intact item counts remain.
    # Any other cutoff could hide evidence and cannot establish a pass. This is
    # recorded before the shared envelope fitter can prune diagnostic samples.
    bounded = not ctx.diagnostics_omitted["cutoffs"] and all(entry["code"] == "witness_limit" for entry in ctx.cutoffs)
    _count(extra["evidence_bounds"], "pass" if bounded else "unknown", {}, "evidence_cutoff")
    for item in items:
        item["status"] = _status(item)
        item["witnesses_omitted"] = item["failed"] + item["unknown"] - len(item["witnesses"])
    data.update(expectations={"boundaries": sorted(expect), "min_activity": dict(minimum), "answers": answers},
                excluded_count=excluded_count, excluded=excluded,
                excluded_omitted_count=excluded_count - len(excluded),
                mcp_answer_count=mcp_answer_count, answer_witnesses=answer_witnesses,
                answer_witnesses_omitted_count=answer_count - len(answer_witnesses),
                actual_activity=actual, evaluation_complete=True,
                required_failures=[item["id"] for item in items if item["status"] == "fail"],
                required_unknowns=[item["id"] for item in items if item["status"] == "unknown"])
    data["status"] = "fail" if data["required_failures"] else "unknown" if data["required_unknowns"] else "pass"

    def bounded_samples():
        for samples, omitted in ((answer_witnesses, "answer_witnesses_omitted_count"),
                                 (excluded, "excluded_omitted_count")):
            if samples:
                samples.pop()
                data[omitted] += 1
                return True
        before = sum(len(item["witnesses"]) for item in items)
        changed = shrink()
        if changed:
            assert sum(len(item["witnesses"]) for item in items) == before - 1
            for item in items:
                item["witnesses_omitted"] = item["failed"] + item["unknown"] - len(item["witnesses"])
        return changed
    return data, bounded_samples


def gate(path, run_id=None, *, expect=None, min_activity=None, answers="all", max_output_bytes=65536, timeout=10.0):
    """Verify complete host evidence against explicit coverage and activity.

    A completed read has status pass, fail or unknown. Read/timeout/output errors
    have ok=False and never carry a partial success verdict. Samples do not bound
    evaluation; raw comparisons remain unchanged by valid preparation evidence.
    answers="mcp" excludes only validated local result comparisons, never other
    evidence checks. Exclusion counts include every occurrence, including replay.
    """
    def validate():
        if type(answers) is not str or answers not in {"all", "mcp"}:
            invalid("invalid_answers", "answers must be all (default) or explicit mcp scope.")
        if (type(expect) not in (list, tuple, set, frozenset)
                or any(type(name) is not str or name not in {"model", "host", "mcp"} for name in expect)
                or len(set(expect)) != len(expect) or not {"model", "host"} <= set(expect)):
            invalid("invalid_expect", "gate requires distinct expected boundaries including model and host; mcp is optional. MCP-only proxy evidence does not satisfy this host policy. Try --expect model,host,mcp --min-answer-occurrences 1 for a host recording.")
        if type(min_activity) is not dict or not min_activity or any(type(name) is not str or name not in ACTIVITY for name in min_activity):
            invalid("invalid_activity", "Supply named minimum activity counts for gate; for example --min-answer-occurrences 1.")
        for value in min_activity.values():
            validate_int(value, 0, 2**63 - 1, "min_activity")
        if not any(value > 0 for name, value in min_activity.items() if name != "turns"):
            invalid("invalid_activity", "At least one non-turn activity minimum must be positive; for example --min-answer-occurrences 1.")

    return run_read("gate", path, {} if run_id is None else {"run_id": run_id},
                    lambda ctx: _gate(ctx, expect, min_activity, answers), max_output_bytes=max_output_bytes,
                    no_content=True, timeout=timeout, validate=validate)
