"""Bounded, descriptive diagnostics over one owned reader snapshot.

This is deliberately not an integrity gate. A displayed page cannot establish
that a run is complete, or identify an unobserved host call.
"""
from ._inspect_common import InspectionError, encode, error_envelope, json_object, json_value, run_read, validate_int
from ._inspect_records import event_complete, run_record
from ._inspect_relations import (
    _slot_pointer_state, answer_rows, artifact_row, association_diagnostics_reason,
    compare_tokens, event_row, pointer_exists, pointer_parts,
    relation_state, request_info,
)


FORMAT = "mfr.diagnose/1"
_BINDING_HINTS = {
    "binding_key_missing": "Pass the same explicit message key when binding an answer and replaying rebuilt history.",
    "binding_conflict": "Use a distinct key for a different selected invocation; a conflicting association cannot overwrite the earlier binding.",
    "binding_limit": "Reduce retained history bindings or raise the appropriate conversation bound; refused bindings remain unknown.",
    "capture_unavailable": "Check the selected invocation's capture receipt and health. An earlier accepted retry cannot stand in for the selected result.",
}


def _source_state(ctx, run_id, link):
    # Check the endpoint independently: legacy relation classification may
    # report missing pointer content before noticing a missing source.
    source = event_row(ctx, run_id, link["source_event_id"], observed=True)
    if source is None:
        return "source_unavailable"
    state, reason = relation_state(ctx, run_id, link)
    if state != "pass":
        return reason
    if event_complete(source) is not True:
        return "source_incomplete"
    artifact = artifact_row(ctx, run_id, source["payload_ref" if link["source_slot"] == "payload" else "catalog_ref"])
    if artifact is None:
        return "source_artifact_unavailable"
    pointer = link["source_pointer"]
    if pointer == "":
        return None  # The root exists even when its retained body is omitted.
    if link["source_slot"] == "payload":
        state, reason = _slot_pointer_state(ctx, run_id, source, pointer)
        return reason if state != "pass" else None
    if artifact["body_text"] is None or artifact["redacted"] or artifact["truncated"]:
        return "source_pointer_unavailable"
    if not pointer_exists(json_value(artifact["body_text"]), pointer):
        return "source_pointer_absent"
    return None


def _occurrence_uncertainty(ctx, event, pointer, structure):
    """Scope recorded association uncertainty to this explicit occurrence.

    Missing locations and omitted diagnostics cannot be assigned to a different
    message. A replay's first witness has its own message path and position.
    """
    details = json_object(event["details_json"])
    selected = pointer_parts(pointer)
    message_path = pointer_parts(structure["manifest"]["message_path"])
    for field in ("unresolved_answers", "message_identity_unavailable"):
        omitted = details.get(field + "_omitted_count", 0)
        if type(omitted) is not int or omitted != 0:
            return "association_diagnostics_omitted"
        entries = details.get(field, [])
        if type(entries) is not list:
            return "association_diagnostics_unavailable"
        for entry in entries:
            ctx.check_deadline()
            if type(entry) is not dict:
                return "association_diagnostics_unavailable"
            if field == "unresolved_answers":
                location = pointer_parts(entry.get("pointer"))
                if location is None:
                    return "association_diagnostics_unavailable"
            else:
                index = entry.get("message_index")
                if type(index) is not int or index < 0:
                    return "association_diagnostics_unavailable"
                location = [*message_path, str(index)]
            if location[:len(selected)] == selected or selected[:len(location)] == location:
                reason = association_diagnostics_reason({field: [entry]})
                if reason:
                    return reason
    return None


def _preparation(ctx, run_id, insertion):
    """Validate a declaration, then compare compatible original text tokens.

    No retained-body fallback: redacted or truncated bytes are not originals.
    Every declared source is visited even after finding one unavailable source.
    """
    prepared = event_row(ctx, run_id, insertion["source_event_id"], observed=True)
    request = event_row(ctx, run_id, insertion["target_event_id"], observed=True)
    result = {"insertion_link_id": insertion["link_id"],
              "prepared_event_id": insertion["source_event_id"],
              "request_event_id": insertion["target_event_id"],
              "target_pointer": insertion["target_pointer"],
              "state": "unknown", "reason": None, "prepared_text_equal": None,
              "raw_text_equal": None, "source_count": 0, "unavailable_sources": 0,
              "semantic_equivalence": "not_verified"}
    reason = None
    state, relation_reason = relation_state(ctx, run_id, insertion)
    if state != "pass" and relation_reason != "pointer_content_unavailable":
        reason = relation_reason
    if prepared is None or request is None:
        reason = "endpoint_unavailable"
    elif prepared["kind"] != "host.tool_result" or request["kind"] != "model.request":
        reason = "endpoint_kind_mismatch"
    elif event_complete(prepared) is not True or event_complete(request) is not True:
        reason = "observation_incomplete"
    elif artifact_row(ctx, run_id, prepared["payload_ref"]) is None:
        reason = "prepared_artifact_unavailable"
    for source in ctx.connection.execute("SELECT * FROM links WHERE run_id=? AND relation='transformed_from' AND target_event_id=? ORDER BY link_id",
                                         (run_id, insertion["source_event_id"])):
        ctx.check_deadline()
        result["source_count"] += 1
        source_reason = _source_state(ctx, run_id, source)
        if source_reason:
            result["unavailable_sources"] += 1
            reason = reason or source_reason
    if result["source_count"] == 0:
        reason = reason or "declared_sources_unavailable"
    structure = request_info(ctx, run_id, insertion["target_event_id"])
    if structure["status"] != "pass":
        reason = reason or structure["reason"]

    # Select by explicit occurrence membership and exact insertion location,
    # never by text, provider IDs, tool name or time proximity.
    selected = []
    for answer in answer_rows(ctx, run_id, request_ids=[insertion["target_event_id"]]):
        if answer["target_pointer"] == insertion["target_pointer"]:
            selected.append(answer)
            if len(selected) > 1:
                break
    if len(selected) != 1:
        reason = reason or "submitted_comparison_unavailable"
    else:
        answer = selected[0]
        result["raw_text_equal"] = answer["text_equal"]
        if answer["text_equal"] is None:
            reason = reason or answer["comparison_reason"] or "original_comparison_unavailable"
        if ctx.connection.execute("SELECT 1 FROM links WHERE run_id=? AND relation='transformed_from' AND target_event_id=? AND source_event_id=? LIMIT 1",
                                  (run_id, insertion["source_event_id"], answer["result_event_id"])).fetchone() is None:
            reason = reason or "selected_result_not_declared_source"
        link = ctx.connection.execute("SELECT * FROM links WHERE run_id=? AND link_id=?", (run_id, answer["link_id"])).fetchone()
        binding_state, binding_reason = relation_state(ctx, run_id, link)
        if binding_state != "pass" or answer["association_state"] != "linked":
            reason = reason or binding_reason or "binding_unavailable"
        witness = event_row(ctx, run_id, link["witness_event_id"], observed=True)
        if reason is None:
            reason = _occurrence_uncertainty(ctx, request, insertion["target_pointer"], structure)
            witness_structure = request_info(ctx, run_id, link["witness_event_id"])
            if witness_structure["status"] != "pass":
                reason = reason or witness_structure["reason"]
            else:
                reason = reason or _occurrence_uncertainty(ctx, witness, link["target_pointer"], witness_structure)
        if reason is None:
            equal, comparison_reason = compare_tokens(
                run_id, "text_blocks_json/1", json_object(prepared["details_json"]).get("result_text_digest"),
                json_object(link["details_json"]).get("submitted_text_digest"), (prepared, request, witness))
            result.update(prepared_text_equal=equal,
                          state="matches" if equal is True else "differs" if equal is False else "unknown",
                          reason=comparison_reason)
    if reason:
        result["reason"] = reason
    return result


def diagnose(path, run_id=None, *, turn_id=None, limit=50, max_output_bytes=65536,
             no_content=True, timeout=10.0):
    """Describe bounded recorded gaps; ``ok`` means the read completed only.

    ``limit`` (1..200) bounds inspected events, displayed diagnostics and
    preparations separately. Source validation for a selected preparation is
    complete or times out; a source sample never earns a positive explanation.
    Omitting ``run_id`` requires exactly one recorded run.
    """
    scope = {}
    if run_id is not None:
        scope["run_id"] = run_id
    if turn_id is not None:
        scope["turn_id"] = turn_id

    def work(ctx):
        selected_run = run_id
        if selected_run is None:
            rows = ctx.connection.execute("SELECT run_id FROM runs LIMIT 2").fetchall()
            if len(rows) != 1:
                raise InspectionError("invalid_request", "ambiguous_run", "The recording does not contain exactly one run; use fr runs and select --run explicitly.")
            selected_run = rows[0]["run_id"]
            ctx.scope["run_id"] = ctx.run_id = selected_run
        run = ctx.connection.execute("SELECT * FROM runs WHERE run_id=?", (selected_run,)).fetchone()
        if turn_id is not None and ctx.connection.execute("SELECT 1 FROM turns WHERE run_id=? AND turn_id=?", (selected_run, turn_id)).fetchone() is None:
            raise InspectionError("unknown_turn", "unknown_turn", "The selected turn is unavailable in this run.")
        data = {"run": run_record(ctx, run, "/run"), "diagnostics": [], "preparations": [],
                "evaluated_events": 0, "event_limit": limit, "events_remaining": False,
                "diagnostics_omitted": 0, "preparations_omitted": 0,
                "whole_run_verdict": "not_evaluated"}

        def add(code, observed, hint, event=None, **location):
            if len(data["diagnostics"]) == limit:
                data["diagnostics_omitted"] += 1
                return
            where = {"run_id": selected_run}
            if event is not None:
                where.update(turn_id=event["turn_id"], event_id=event["event_id"], model_call_id=event["model_call_id"])
            where.update(location)
            data["diagnostics"].append({"code": code, "location": where, "observed": observed, "hint": hint})

        health = data["run"]["health"]["reason_counts"]
        if (type(run["writer_errors"]) is int and run["writer_errors"] > 0) or health.get("writer_failed", 0):
            add("writer_failure", "Recorded writer failures indicate storage loss.", "Inspect host health/error logs, stop this recorder, and start a fresh path after fixing storage. Lost events cannot be reconstructed here.")
        if (type(run["dropped_events"]) is int and run["dropped_events"] > 0) or any(health.get(key, 0) for key in ("capacity", "prerequisite_lost", "closed", "invalid_scope")):
            add("capture_rejected", "Recorded counters report rejected or lost capture.", "Inspect live receipt/health reasons and configured limits; this file does not identify every rejected call.")
        if health.get("invalid_input", 0):
            add("unsupported_input", "Recorded health includes invalid capture input.", "Check the supported JSON/native adapter shapes and facade development diagnostics; do not change host execution to satisfy capture.")
        if run["close_state"] != "clean":
            add("run_not_clean", "The recorded run is not marked cleanly closed.", "Check host-owned shutdown and writer health. Recorded open state alone does not establish whether a writer is currently alive.")
        query, params = "SELECT * FROM observed_events WHERE run_id=?", [selected_run]
        if turn_id is not None:
            query += " AND turn_id=?"
            params.append(turn_id)
        events = ctx.connection.execute(query + " ORDER BY sequence,event_id LIMIT ?", [*params, limit + 1]).fetchall()
        data["events_remaining"] = len(events) > limit
        if data["events_remaining"]:
            ctx.cutoff("event_limit", "", count=None)
        for event in events[:limit]:
            ctx.check_deadline()
            data["evaluated_events"] += 1
            details = json_object(event["details_json"])
            if event_complete(event) is not True:
                add("observation_incomplete", "This observation is incomplete or has unknown completeness.", "Check native stream consumption/close and capture health; absence cannot identify a unique missing host call.", event)
            if event["kind"] == "model.request":
                info = request_info(ctx, selected_run, event["event_id"])
                if info["status"] != "pass":
                    add("unsupported_input", "The request structure is unavailable or unsupported for inspection.", "Use the supported message-array adapter or ordinary JSON boundary and inspect capture diagnostics.", event)
                for field in ("unresolved_answers", "insertion_diagnostics", "message_identity_unavailable"):
                    values = details.get(field, [])
                    if values:
                        entries = values if type(values) is list else [None]
                        for entry in entries:
                            ctx.check_deadline()
                            entry = entry if type(entry) is dict else {}
                            recorded_reason = entry.get("reason")
                            hint = _BINDING_HINTS.get(recorded_reason) if type(recorded_reason) is str else None
                            location = {key: entry[key] for key in ("pointer", "message_index")
                                        if type(entry.get(key)) is (str if key == "pointer" else int)}
                            if hint:
                                location["recorded_reason"] = recorded_reason
                            add("unknown_binding" if field == "unresolved_answers" else "preparation_unavailable" if field == "insertion_diagnostics" else "message_identity_unavailable",
                                "The request records unresolved " + field.replace("_", " ") + ".",
                                hint or "Bind the host-selected invocation and check supported prepared text and message-key configuration. Check live diagnostics; a missing binding does not prove which call was omitted.", event, **location)
                    if details.get(field + "_omitted_count", 0):
                        add("recorded_diagnostics_omitted", "Capture omitted additional request diagnostics.", "Inspect live health and increase only the relevant configured bound; omitted reasons cannot be recovered from this page.", event)
                for reference in info["artifact_ids"]:
                    artifact = artifact_row(ctx, selected_run, reference)
                    if artifact is not None and artifact["omitted_reason"] == "capture_metadata":
                        add("content_omitted", "Content was intentionally omitted by metadata retention.", "Original text tokens may still support selected comparisons. Use content retention only when appropriate; missing endpoints remain unknown.", event, artifact_id=reference)
                        break
                for insertion in ctx.connection.execute("SELECT * FROM links WHERE run_id=? AND target_event_id=? AND relation='inserted_into' ORDER BY link_id", (selected_run, event["event_id"])):
                    if len(data["preparations"]) == limit:
                        data["preparations_omitted"] += 1
                    else:
                        data["preparations"].append(_preparation(ctx, selected_run, insertion))
            if event["kind"] == "mcp.request" and event["method"] == "tools/call":
                operation = ctx.connection.execute("SELECT link_basis FROM operations WHERE run_id=? AND operation_id=?", (selected_run, event["operation_id"])).fetchone()
                if operation is None or operation["link_basis"] == "none":
                    add("unattributed_operation", "This tool invocation has no recorded model/host selection attribution.", "Record the host's selected choice or explicit host-initiated invocation. Do not infer a choice from equal text, tool name or timing.", event, operation_id=event["operation_id"])
        if data["diagnostics_omitted"] or data["preparations_omitted"]:
            ctx.cutoff("item_limit", "", count=data["diagnostics_omitted"] + data["preparations_omitted"])

        def shrink():
            for key in ("diagnostics", "preparations"):
                if data[key]:
                    data[key].pop()
                    data[key + "_omitted"] += 1
                    return True
            return False
        return data, shrink

    result = run_read("diagnose", path, scope, work, max_output_bytes=max_output_bytes,
                      no_content=no_content, timeout=timeout,
                      validate=lambda: validate_int(limit, 1, 200, "limit"))
    result["format"] = FORMAT  # Same bounded transport fields; separate contract.
    if result["ok"] and len(encode(result)) > max_output_bytes:
        result = error_envelope("diagnose", "limit_exceeded", "required_output", "The required intact diagnostic output cannot fit its byte budget.")
        result["format"] = FORMAT
    return result
