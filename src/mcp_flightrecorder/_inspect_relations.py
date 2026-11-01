"""Read explicit request membership, binding witnesses and original comparisons."""
from contextlib import closing
import re

from ._inspect_common import display, json_object, json_value
from ._inspect_records import event_complete, member_record


# The guarded iterator and valid-body/format predicates exclude malformed
# manifests before path projection. Keep the direct body argument in projected
# json_extract calls; wrapping it in CASE repeats costly JSON work on SQLite 3.54.
MEMBERS_SQL = """
SELECT e.run_id,e.event_id AS request_event_id,e.turn_id,e.model_call_id,
       e.sequence AS request_sequence,e.payload_ref AS manifest_ref,
       CAST(m.key AS INTEGER) AS message_index,
       json_extract(a.body_text,'$.message_path') AS message_path,
       json_extract(a.body_text,'$.message_path') || '/' || m.key AS message_pointer,
       m.value AS message_artifact_id,p.artifact_id IS NULL AS artifact_missing,
       p.omitted_reason,p.redacted,p.truncated
FROM observed_events e
JOIN artifacts a ON a.run_id=e.run_id AND a.artifact_id=e.payload_ref
JOIN json_each(CASE WHEN json_valid(a.body_text) THEN a.body_text ELSE '{}' END,'$.message_refs') m
LEFT JOIN artifacts p ON p.run_id=e.run_id AND p.artifact_id=m.value
WHERE e.kind='model.request' AND a.representation='request_refs/2' AND json_valid(a.body_text)
  AND json_extract(CASE WHEN json_valid(a.body_text) THEN a.body_text ELSE '{}' END,'$.format')='request_refs/2'
  AND m.type='text'
"""

OCCURRENCES_SQL = """
SELECT m.run_id,m.request_event_id,m.turn_id,m.model_call_id,m.request_sequence,
       m.message_index,m.message_artifact_id,l.link_id,l.source_event_id,
       l.witness_event_id AS binding_witness_event_id,l.target_event_id AS binding_request_event_id,
       m.message_pointer || json_extract(l.details_json,'$.relative_pointer') AS target_pointer,
       CASE WHEN w.event_id IS NULL OR w.kind<>'model.request'
              OR w.event_id<>l.target_event_id OR w.sequence>m.request_sequence
            THEN 'missing_or_unselected_witness'
            WHEN d.event_id IS NULL THEN 'missing_decision' ELSE 'linked' END AS association_state
FROM members m
JOIN links l ON {member_selection}l.run_id=m.run_id AND l.relation='answers'
  AND json_extract(l.details_json,'$.binding_format')='message_binding/2'
  AND json_extract(l.details_json,'$.message_artifact_id')=m.message_artifact_id
LEFT JOIN observed_events w ON w.run_id=l.run_id AND w.event_id=l.witness_event_id
LEFT JOIN observed_events d ON d.run_id=l.run_id AND d.event_id=l.source_event_id AND d.kind='model.tool_call'
UNION ALL
SELECT r.run_id,r.event_id,r.turn_id,r.model_call_id,r.sequence,NULL,NULL,l.link_id,
       l.source_event_id,l.witness_event_id,l.target_event_id,l.target_pointer,
       CASE WHEN d.event_id IS NULL THEN 'missing_decision' ELSE 'linked' END
FROM links l
JOIN observed_events r ON r.run_id=l.run_id AND r.event_id=l.target_event_id AND r.kind='model.request'
JOIN observed_events w ON w.run_id=l.run_id AND w.event_id=l.witness_event_id
LEFT JOIN observed_events d ON d.run_id=l.run_id AND d.event_id=l.source_event_id AND d.kind='model.tool_call'
WHERE l.relation='answers' AND coalesce(json_extract(l.details_json,'$.binding_format'),'')=''
"""


def event_row(ctx, run_id, event_id, *, observed=False):
    ctx.check_deadline()
    if event_id is None:
        return None
    def load():
        table = "observed_events" if observed else "events"
        with closing(ctx.connection.execute("SELECT * FROM " + table + " WHERE run_id=? AND event_id=?",
                                             (run_id, event_id))) as cursor:
            return cursor.fetchone()
    return ctx._cache.get(("event", run_id, event_id, observed), load)


def artifact_row(ctx, run_id, artifact_id):
    ctx.check_deadline()
    if artifact_id is None:
        return None
    def load():
        with closing(ctx.connection.execute("SELECT * FROM artifacts WHERE run_id=? AND artifact_id=?",
                                             (run_id, artifact_id))) as cursor:
            return cursor.fetchone()
    return ctx._cache.get(("artifact", run_id, artifact_id), load,
                          admit=lambda row: row is None or row["representation"] != "request_refs/2")


def pointer_parts(pointer):
    if type(pointer) is not str or (pointer and not pointer.startswith("/")):
        return None
    if re.search(r"~(?:[^01]|$)", pointer):
        return None
    return [part.replace("~1", "/").replace("~0", "~") for part in pointer[1:].split("/")] if pointer else []


def pointer_exists(value, pointer):
    parts = pointer_parts(pointer)
    if parts is None:
        return False
    for part in parts:
        if type(value) is dict and part in value:
            value = value[part]
        elif type(value) is list and re.fullmatch(r"0|[1-9][0-9]*", part):
            # Avoid converting an arbitrarily long identity-bearing pointer.
            if len(part) > len(str(len(value))) or int(part) >= len(value):
                return False
            value = value[int(part)]
        else:
            return False
    return True


def _pointer_value(value, pointer):
    for part in pointer_parts(pointer):
        value = value[int(part)] if type(value) is list else value[part]
    return value


def request_info(ctx, run_id, request_event_id):
    """Validate available structure without requiring retained member bodies."""
    event = event_row(ctx, run_id, request_event_id, observed=True)
    info = {"status": "unknown", "reason": "request_unavailable", "event": event,
            "manifest": None, "artifact_ids": []}
    if event is None or event["kind"] != "model.request":
        return info
    artifact = artifact_row(ctx, run_id, event["payload_ref"])
    if event["payload_ref"]:
        info["artifact_ids"].append(event["payload_ref"])
    if artifact is None:
        info["reason"] = "manifest_missing"
        return info
    if artifact["representation"] != "request_refs/2":
        info["reason"] = "request_representation_unsupported"
        return info
    if artifact["body_text"] is None:
        info["reason"] = "manifest_omitted"
        return info
    if artifact["redacted"] or artifact["truncated"]:
        info["reason"] = "manifest_partial"
        return info
    manifest = json_value(artifact["body_text"])
    info["manifest"] = manifest if type(manifest) is dict else None
    if (type(manifest) is not dict or manifest.get("format") != "request_refs/2"
            or type(manifest.get("template_ref")) is not str or not manifest["template_ref"]
            or not pointer_parts(manifest.get("message_path"))
            or type(manifest.get("message_refs")) is not list
            or type(manifest.get("field_refs")) is not list):
        info.update(status="fail", reason="malformed_request_manifest")
        return info
    references = [manifest["template_ref"]]
    for reference in manifest["message_refs"]:
        ctx.check_deadline()
        if type(reference) is not str or not reference:
            info.update(status="fail", reason="malformed_message_reference")
            return info
        references.append(reference)
    paths = [manifest["message_path"]]
    for field in manifest["field_refs"]:
        ctx.check_deadline()
        if (type(field) is not dict or not pointer_parts(field.get("pointer"))
                or type(field.get("artifact_id")) is not str or not field["artifact_id"]):
            info.update(status="fail", reason="malformed_field_reference")
            return info
        path = field["pointer"]
        if any(path == prior or path.startswith(prior + "/") or prior.startswith(path + "/") for prior in paths):
            info.update(status="fail", reason="overlapping_request_paths")
            return info
        paths.append(path)
        references.append(field["artifact_id"])
    info["artifact_ids"] = list(dict.fromkeys([event["payload_ref"], *references]))
    unknown = "request_incomplete" if event_complete(event) is not True else None
    for reference in dict.fromkeys(references):
        part = artifact_row(ctx, run_id, reference)
        if part is None:
            unknown = "referenced_artifact_missing"
            continue
        if part["representation"] == "request_refs/2":
            info.update(status="fail", reason="nested_request_manifest")
            return info
        if reference == manifest["template_ref"] and part["body_text"] is not None and not part["redacted"] and not part["truncated"]:
            template = json_value(part["body_text"])
            if any(not pointer_exists(template, path) or _pointer_value(template, path) is not None for path in paths):
                info.update(status="fail", reason="contradictory_request_template")
                return info
    info.update(status="unknown" if unknown else "pass", reason=unknown)
    return info


def _selected(column, values, parameters, *, numbered=False):
    if values is None:
        return ""
    values = tuple(values)
    if not values:
        return " AND 0"
    start = len(parameters) + 1
    parameters.extend(values)
    placeholders = ("?" + str(index) for index in range(start, len(parameters) + 1)) if numbered else ("?" for _ in values)
    return " AND " + column + " IN (" + ",".join(placeholders) + ")"


def member_rows(ctx, run_id, request_ids, *, limit=None):
    parameters = [run_id]
    sql = "WITH members AS (" + MEMBERS_SQL + ") SELECT * FROM members WHERE run_id=?"
    sql += _selected("request_event_id", request_ids, parameters)
    sql += " ORDER BY request_sequence IS NULL,request_sequence,request_event_id,message_index"
    if limit is not None:
        sql += " LIMIT ?"
        parameters.append(limit)
    for row in ctx.connection.execute(sql, parameters):
        ctx.check_deadline()
        yield member_record(ctx, row)


def _member_count(ctx, run_id, request_ids):
    """Count the same ordered occurrences without projecting unused identities."""
    parameters = [run_id]
    sql = "WITH members AS (" + MEMBERS_SQL + ") SELECT request_sequence,omitted_reason FROM members WHERE run_id=?"
    sql += _selected("request_event_id", request_ids, parameters)
    sql += " ORDER BY request_sequence IS NULL,request_sequence,request_event_id,message_index"
    count = 0
    with closing(ctx.connection.execute(sql, parameters)) as cursor:
        for row in cursor:
            ctx.check_deadline()
            # These are the only member fields that can emit display cutoffs:
            # the others are identities, CAST integers, or normalized booleans.
            # Keep their original projection order, even for malformed TEXT
            # sequences and omitted-reason strings on a damaged recording.
            display(row["request_sequence"], ctx, "")
            display(row["omitted_reason"], ctx, "")
            count += 1
    return count


def _slot_pointer_state(ctx, run_id, event, pointer):
    """Validate known shape, never reconstruct unavailable original content."""
    if pointer_parts(pointer) is None:
        return "fail", "invalid_slot_pointer"
    artifact = artifact_row(ctx, run_id, event["payload_ref"])
    if artifact is None:
        return "unknown", "pointer_artifact_missing"
    if artifact["body_text"] is None or artifact["redacted"] or artifact["truncated"]:
        return "unknown", "pointer_content_unavailable"
    if artifact["representation"] == "request_refs/2":
        info = request_info(ctx, run_id, event["event_id"])
        if info["status"] == "fail":
            return "fail", info["reason"]
        manifest = info["manifest"]
        if not manifest or info["status"] == "unknown":
            return "unknown", info["reason"]
        path = manifest["message_path"]
        if pointer == path:
            return "pass", None
        if pointer.startswith(path + "/"):
            suffix = pointer[len(path) + 1:]
            index, separator, relative = suffix.partition("/")
            if not re.fullmatch(r"0|[1-9][0-9]*", index) or len(index) > len(str(len(manifest["message_refs"]))) or int(index) >= len(manifest["message_refs"]):
                return "fail", "pointer_absent"
            part = artifact_row(ctx, run_id, manifest["message_refs"][int(index)])
            remainder = "/" + relative if separator else ""
        else:
            field = next((field for field in manifest["field_refs"]
                          if pointer == field["pointer"] or pointer.startswith(field["pointer"] + "/")), None)
            if field is not None:
                part = artifact_row(ctx, run_id, field["artifact_id"])
                remainder = pointer[len(field["pointer"]):]
            else:
                part = artifact_row(ctx, run_id, manifest["template_ref"])
                remainder = pointer
        if part is None or part["body_text"] is None or part["redacted"] or part["truncated"]:
            return "unknown", "pointer_content_unavailable"
        value = json_value(part["body_text"])
        return ("pass", None) if pointer_exists(value, remainder) else ("fail", "pointer_absent")
    value = json_value(artifact["body_text"])
    return ("pass", None) if pointer_exists(value, pointer) else ("fail", "pointer_absent")


def _binding_identity(details):
    if type(details) is not dict or details.get("binding_format") != "message_binding/2":
        return None
    handle, relative = details.get("message_artifact_id"), details.get("relative_pointer")
    if type(handle) is not str or not handle or pointer_parts(relative) is None:
        return None
    return handle, relative


def _conflicting_binding(ctx, run_id, link, identity):
    """Distinct decision claims cannot resolve the same explicit message block."""
    handle, relative = identity
    ctx.check_deadline()
    return ctx.connection.execute("""SELECT 1 FROM links
        WHERE run_id=? AND relation='answers' AND source_event_id IS NOT ?
          AND json_extract(CASE WHEN json_valid(details_json) THEN details_json ELSE '{}' END,'$.binding_format')='message_binding/2'
          AND json_type(CASE WHEN json_valid(details_json) THEN details_json ELSE '{}' END,'$.message_artifact_id')='text'
          AND json_type(CASE WHEN json_valid(details_json) THEN details_json ELSE '{}' END,'$.relative_pointer')='text'
          AND json_extract(CASE WHEN json_valid(details_json) THEN details_json ELSE '{}' END,'$.message_artifact_id')=?
          AND json_extract(CASE WHEN json_valid(details_json) THEN details_json ELSE '{}' END,'$.relative_pointer')=?
        LIMIT 1""", (run_id, link["source_event_id"], handle, relative)).fetchone() is not None


def _prefix_excludes_earlier_member(ctx, run_id, sequence, handle):
    """Prove absence only while every minimum in the covered prefix survives."""
    ctx.check_deadline()
    cache = ctx._cache
    if cache.minima_disabled or type(sequence) is not int:
        return False
    through = ctx._member_through.get(run_id)
    if through is None or sequence > through:
        statement = "WITH members AS (" + MEMBERS_SQL + """
            ) SELECT message_artifact_id,MIN(request_sequence) AS first_sequence
            FROM members WHERE run_id=? AND request_sequence<=?"""
        parameters = [run_id, sequence]
        if through is not None:
            statement += " AND request_sequence>?"
            parameters.append(through)
        statement += " GROUP BY message_artifact_id"
        with closing(ctx.connection.execute(statement, parameters)) as cursor:
            for row in cursor:
                ctx.check_deadline()
                key = ("member_min", run_id, row["message_artifact_id"])
                # Intervals advance monotonically. An already retained minimum
                # precedes this interval, including all equal-sequence rows.
                cache.get(key, lambda row=row: row["first_sequence"])
                if cache.minima_disabled:
                    return False
        ctx.check_deadline()
        ctx._member_through[run_id] = sequence
    if cache.minima_disabled:
        return False
    found, minimum = cache.lookup(("member_min", run_id, handle))
    return not found or (type(minimum) is int and minimum >= sequence)


def _earlier_binding_member(ctx, run_id, witness, handle):
    """Only conclusive authoritative membership can disprove first-witness order."""
    if _prefix_excludes_earlier_member(ctx, run_id, witness["sequence"], handle):
        return False
    parameters = (run_id, handle, witness["sequence"])
    statement = "WITH members AS (" + MEMBERS_SQL + """
        ) SELECT DISTINCT request_event_id,request_sequence FROM members
        WHERE run_id=? AND message_artifact_id=? AND request_sequence<?
        ORDER BY request_sequence,request_event_id"""
    for row in ctx.connection.execute(statement, parameters):
        ctx.check_deadline()
        if request_info(ctx, run_id, row["request_event_id"])["status"] == "pass":
            return True
    return False


def _message_binding_state(ctx, run_id, link, witness, identity, *, witness_info=None):
    if identity is None:
        return "fail", "malformed_binding"
    handle, relative = identity
    if _earlier_binding_member(ctx, run_id, witness, handle):
        return "fail", "contradictory_first_witness_order"
    # The caller may already own this exact current request's validation.
    # Reuse it only in this call; it never enters Context or the bounded cache.
    info = witness_info
    info_event = info.get("event") if type(info) is dict else None
    if (info_event is None or witness["run_id"] != run_id
            or info_event["run_id"] != run_id or info_event["event_id"] != witness["event_id"]):
        info = request_info(ctx, run_id, witness["event_id"])
    if info["status"] == "fail":
        return "fail", "invalid_witness_manifest"
    manifest = info["manifest"]
    if not manifest or type(manifest.get("message_refs")) is not list:
        return "unknown", "witness_manifest_unavailable"
    expected = {manifest["message_path"] + "/" + str(index) + relative
                for index, reference in enumerate(manifest["message_refs"]) if reference == handle}
    if link["target_pointer"] not in expected:
        return "fail", "contradictory_binding_membership"
    part = artifact_row(ctx, run_id, handle)
    if part is not None and part["body_text"] is not None and not part["redacted"] and not part["truncated"]:
        if not pointer_exists(json_value(part["body_text"]), relative):
            return "fail", "contradictory_binding_pointer"
    if info["status"] == "unknown":
        return "unknown", "witness_manifest_unavailable"
    return "pass", None


def _relation_state(ctx, run_id, link, *, witness_info=None):
    """Classify one recorded claim, including disproven original bindings."""
    relation = link["relation"]
    if relation not in {"dispatch_source", "answers", "transformed_from", "inserted_into"}:
        return "fail", "unsupported_relation"
    if link["run_id"] != run_id or link["witness_event_id"] != link["target_event_id"]:
        return "fail", "contradictory_witness_column"
    witness = event_row(ctx, run_id, link["witness_event_id"], observed=True)
    source = event_row(ctx, run_id, link["source_event_id"], observed=True)
    target_kind = {"dispatch_source": "host.dispatch", "answers": "model.request",
                   "transformed_from": "host.tool_result", "inserted_into": "model.request"}[relation]
    if witness is not None and witness["kind"] != target_kind:
        return "fail", "wrong_witness_kind"
    expected_source = {"dispatch_source": "model.tool_call", "answers": "model.tool_call",
                       "inserted_into": "host.tool_result"}.get(relation)
    if source is not None and expected_source and source["kind"] != expected_source:
        return "fail", "wrong_source_kind"
    if source is not None and witness is not None and source["sequence"] >= witness["sequence"]:
        return "fail", "contradictory_witness_order"
    slots = tuple(link[field] for field in ("source_slot", "source_pointer", "target_slot", "target_pointer"))
    if relation == "dispatch_source" and slots != (None, None, None, None):
        return "fail", "invalid_dispatch_shape"
    if relation == "answers" and (slots[:2] != (None, None) or slots[2] != "payload" or pointer_parts(slots[3]) is None):
        return "fail", "invalid_answer_shape"
    if relation == "transformed_from" and (slots[0] not in {"payload", "catalog"} or pointer_parts(slots[1]) is None or slots[2:] != (None, None)):
        return "fail", "invalid_transformation_shape"
    if relation == "inserted_into" and (slots[:3] != ("payload", "", "payload") or pointer_parts(slots[3]) is None):
        return "fail", "invalid_insertion_shape"
    details = json_value(link["details_json"])
    identity = _binding_identity(details) if relation == "answers" else None
    if identity is not None and _conflicting_binding(ctx, run_id, link, identity):
        return "fail", "conflicting_binding_decisions"
    if witness is None:
        return "unknown", "witness_unavailable"
    if type(details) is not dict:
        return "fail", "malformed_link_details"
    if relation == "answers" and details.get("binding_format") == "message_binding/2":
        status, reason = _message_binding_state(ctx, run_id, link, witness, identity, witness_info=witness_info)
        if status != "pass":
            return status, reason
    elif relation == "answers":
        if details.get("binding_format") not in (None, ""):
            return "unknown", "binding_format_unsupported"
        status, reason = _slot_pointer_state(ctx, run_id, witness, link["target_pointer"])
        if status != "pass":
            return status, reason
    elif relation == "inserted_into":
        status, reason = _slot_pointer_state(ctx, run_id, witness, link["target_pointer"])
        if status != "pass":
            return status, reason
    # A transformed source is a declared source location, not an assertion that
    # retained source bytes equal the prepared result. Omission keeps its shape.
    if source is None:
        return "unknown", "source_unavailable"
    return "pass", None


def relation_state(ctx, run_id, link, *, witness_info=None):
    """Reuse a claim's classification only inside this owned read snapshot."""
    return ctx._cache.get(("relation", run_id, link["link_id"]),
                          lambda: _relation_state(ctx, run_id, link, witness_info=witness_info))


def witnessed_binding_failures(ctx, run_id, request_event_id, *, witness_info=None):
    """Retain explicit witness contradictions even when no occurrence joins."""
    for link in ctx.connection.execute("""SELECT * FROM links
            WHERE run_id=? AND relation='answers' AND witness_event_id=?
            ORDER BY link_id""", (run_id, request_event_id)):
        status, reason = relation_state(ctx, run_id, link, witness_info=witness_info)
        if status == "fail":
            yield link, reason


_TOKEN_REASONS = {
    "disabled", "original_incomplete", "unexposed", "unsupported_projection", "capture_limit",
    "unsupported_value", "encoding_unavailable", "conflicting_arguments", "invalid_raw_arguments",
    "text_paths_unavailable", "text_extraction_unavailable",
    # Keep previously accepted spellings readable in existing schema-6 files.
    "arguments_unavailable", "arguments_conflict", "arguments_invalid", "raw_arguments_invalid",
}


def association_diagnostics_reason(details):
    """Return one bounded reason without trusting untyped recorded JSON."""
    if type(details) is not dict:
        return "association_diagnostics_unavailable"
    for field in ("unresolved_answers", "message_identity_unavailable"):
        omitted = details.get(field + "_omitted_count", 0)
        if type(omitted) is not int or omitted != 0:
            return "association_diagnostics_omitted"
        values = details.get(field, [])
        if type(values) is not list:
            return "association_diagnostics_unavailable"
        if field == "unresolved_answers" and values:
            return "unresolved_answer_diagnostics"
        for value in values:
            reason = value.get("reason") if type(value) is dict else None
            if type(reason) is not str or reason not in _TOKEN_REASONS or reason == "original_incomplete":
                return "binding_context_unavailable"
    return None


def _token_value(token, run_id, projection):
    if type(token) is not dict:
        return None, "missing_digest"
    if token.get("scope") != run_id:
        return None, "digest_scope_mismatch"
    if token.get("projection") != projection:
        return None, "digest_projection_mismatch"
    if token.get("basis") != "observed_original":
        return None, "digest_basis_mismatch"
    value = token.get("value")
    if value is None:
        reason = token.get("reason")
        return None, reason if type(reason) is str and reason in _TOKEN_REASONS else "digest_unavailable"
    if type(value) is not str or re.fullmatch("[0-9a-f]{64}", value) is None:
        return None, "malformed_digest"
    return value, None


def compare_tokens(run_id, projection, left, right, observations):
    if any(row is None for row in observations):
        return None, "observation_unavailable"
    if any(event_complete(row) is not True for row in observations):
        return None, "observation_incomplete"
    first, reason = _token_value(left, run_id, projection)
    if reason:
        return None, reason
    second, reason = _token_value(right, run_id, projection)
    if reason:
        return None, reason
    return first == second, None


def _answer_descriptor(ctx, run_id, link_id):
    """Memoize stable evidence, never an occurrence's completeness or diagnostics."""
    def load():
        with closing(ctx.connection.execute("SELECT * FROM links WHERE run_id=? AND link_id=?",
                                             (run_id, link_id))) as cursor:
            link = cursor.fetchone()
        status, reason = relation_state(ctx, run_id, link)
        details = json_object(link["details_json"])
        witness = event_row(ctx, run_id, link["witness_event_id"], observed=True)
        result_id = details.get("result_event_id")
        result_id = result_id if type(result_id) is str else None
        result = event_row(ctx, run_id, result_id, observed=True)
        result_valid = result is not None and result["kind"] == "mcp.response"
        present = witness is not None and result is not None
        complete = present and event_complete(witness) is True and event_complete(result) is True
        left, token_reason = _token_value(details.get("submitted_text_digest"), run_id, "text_blocks_json/1")
        equal = None
        if token_reason is None:
            result_details = json_object(result["details_json"]) if result_valid else {}
            right, token_reason = _token_value(result_details.get("result_text_digest"), run_id, "text_blocks_json/1")
            if token_reason is None:
                equal = left == right
        return status, reason, result_id, result_valid, present, complete, equal, token_reason
    return ctx._cache.get(("answer", run_id, link_id, "text_blocks_json/1"), load)


def _answer_states(ctx, run_id, request_ids=None, decision_ids=None, *, limit=None):
    """Evaluate each ordered occurrence before choosing its output projection."""
    parameters = [run_id]
    request_selection = _selected("request_event_id", request_ids, parameters, numbered=True)
    if decision_ids is not None:
        decision_ids = tuple(decision_ids)
    decision_selection = _selected("source_event_id", decision_ids, parameters, numbered=True)
    member_selection = ""
    if decision_ids is not None:
        # A retained modern occurrence already has a matching selected link.
        # Filter other handles before repeatedly probing links, while keeping
        # whole-run membership and the original join's multiplicity and checks.
        # Reuse the outer bindings so selector capacity does not shrink.
        member_selection = """m.message_artifact_id IN (
            SELECT json_extract(details_json,'$.message_artifact_id') FROM links
            WHERE run_id=?1 AND relation='answers'
              AND json_extract(details_json,'$.binding_format')='message_binding/2'"""
        member_selection += decision_selection + ") AND "
    sql = "WITH members AS (" + MEMBERS_SQL + "), occurrences AS (" + OCCURRENCES_SQL.format(member_selection=member_selection) + ") SELECT * FROM occurrences WHERE run_id=?1"
    sql += request_selection + decision_selection
    sql += " ORDER BY request_sequence IS NULL,request_sequence,message_index IS NULL,message_index,target_pointer,link_id,request_event_id"
    if limit is not None:
        sql += " LIMIT ?" + str(len(parameters) + 1)
        parameters.append(limit)
    # Adjacent occurrences share only their current request's two scalar facts.
    # Stable link facts belong to the single bounded Context cache.
    previous_request = None
    request_present, request_complete = False, None
    for row in ctx.connection.execute(sql, parameters):
        ctx.check_deadline()
        link_id = row["link_id"]
        status, reason, result_id, result_valid, present, complete, token_equal, token_reason = _answer_descriptor(ctx, run_id, link_id)
        association_state = row["association_state"]
        if status == "fail":
            association_state = "invalid_binding"
        # Unknown relation state does not refute the view's explicit membership.
        request_key = (run_id, row["request_event_id"])
        if request_key != previous_request:
            request = event_row(ctx, run_id, row["request_event_id"], observed=True)
            request_present = request is not None
            request_complete = event_complete(request) if request_present else None
            previous_request = request_key
        if association_state != "linked":
            equal, comparison_reason = None, reason if association_state == "invalid_binding" else association_state
        elif not result_valid:
            equal, comparison_reason = None, "result_unavailable"
        elif not request_present or not present:
            equal, comparison_reason = None, "observation_unavailable"
        elif request_complete is not True or not complete:
            equal, comparison_reason = None, "observation_incomplete"
        else:
            equal, comparison_reason = token_equal, token_reason
        if association_state != "linked":
            ctx.unknown(association_state, ids={"request_event_id": row["request_event_id"], "link_id": link_id}, count=1)
        if equal is None:
            ctx.unknown(comparison_reason, ids={"request_event_id": row["request_event_id"], "link_id": link_id}, count=1)
        yield row, association_state, result_id, equal, comparison_reason


def answer_rows(ctx, run_id, request_ids=None, decision_ids=None, *, limit=None):
    for row, association_state, result_id, equal, comparison_reason in _answer_states(
            ctx, run_id, request_ids, decision_ids, limit=limit):
        answer = dict(row)
        answer["association_state"] = association_state
        answer["result_event_id"] = result_id
        answer.update(text_equal=equal, comparison_reason=comparison_reason)
        yield answer


def _answer_summary(ctx, run_id, request_ids=None, decision_ids=None):
    """Consume every occurrence and diagnostic without discarded public rows."""
    count, text_mismatch = 0, False
    for _row, _state, _result_id, equal, _reason in _answer_states(ctx, run_id, request_ids, decision_ids):
        count += 1
        if equal is False:
            text_mismatch = True
    return count, text_mismatch


def dispatch_rows(ctx, run_id, decision_ids=None, dispatch_ids=None):
    parameters = [run_id]
    sql = "SELECT l.* FROM links l LEFT JOIN events w ON w.run_id=l.run_id AND w.event_id=l.witness_event_id WHERE l.run_id=? AND l.relation='dispatch_source'"
    sql += _selected("l.source_event_id", decision_ids, parameters)
    sql += _selected("l.target_event_id", dispatch_ids, parameters)
    sql += " ORDER BY w.sequence IS NULL,w.sequence,l.link_id"
    seen = set()
    for link in ctx.connection.execute(sql, parameters):
        ctx.check_deadline()
        pair = (link["source_event_id"], link["target_event_id"])
        if pair in seen:
            continue
        seen.add(pair)
        decision = event_row(ctx, run_id, pair[0], observed=True)
        dispatch = event_row(ctx, run_id, pair[1], observed=True)
        status, reason = relation_state(ctx, run_id, link)
        if status != "pass":
            equal = None
        else:
            decision_details = json_object(decision["details_json"])
            dispatch_details = json_object(dispatch["details_json"])
            equal, reason = compare_tokens(run_id, "args_json_python/1", decision_details.get("args_digest"),
                                           dispatch_details.get("args_digest"), (decision, dispatch))
        if equal is None:
            ctx.unknown(reason, ids={"decision_event_id": pair[0], "dispatch_event_id": pair[1]}, count=1)
        yield {"decision_event_id": pair[0], "dispatch_event_id": pair[1],
               "forwarded_changed": None if equal is None else not equal, "comparison_reason": reason}
