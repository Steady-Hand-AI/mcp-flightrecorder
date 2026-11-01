"""Bounded breadth-first ancestry of one submitted model request."""
from collections import deque
from itertools import chain

from ._inspect_common import InspectionError, json_value, run_read, validate_int
from ._inspect_records import artifact_record, event_record, link_record, operation_record, run_record, turn_record
from ._inspect_relations import answer_rows, artifact_row, association_diagnostics_reason, event_row, member_rows, relation_state, request_info, witnessed_binding_failures


TABLES = ("runs", "turns", "events", "operations", "links", "artifacts", "members", "answers")


def _slice(ctx, request_event_id, limit, max_depth):
    root = event_row(ctx, ctx.run_id, request_event_id, observed=True)
    if root is None:
        raise InspectionError("unknown_event", "unknown_event", "The selected authoritative event is unavailable in this run.")
    if root["kind"] != "model.request":
        raise InspectionError("invalid_request", "request_event_required", "Slice requires an authoritative model.request event.")
    output = {table: {} for table in TABLES}
    pending = deque()
    queued = {}
    seen = {}
    frontier = []
    frontier_keys = set()
    complete, count, halted = True, 0, False

    def problem(kind, identity, depth, reason, *, cutoff=False):
        nonlocal complete
        complete = False
        key = (kind, identity, depth, reason)
        if key in frontier_keys:
            return
        if len(frontier) < 20:
            frontier_keys.add(key)
            frontier.append({"kind": kind, "id": identity, "depth": depth, "reason": reason})
        else:
            ctx.diagnostics_omitted["cutoffs" if cutoff else "unknowns"] += 1
        diagnostic = ctx.cutoff if cutoff else ctx.unknown
        id_field = {"run": "run_id", "turn": "turn_id", "event": "event_id", "operation": "operation_id",
                    "artifact": "artifact_id", "link": "link_id", "member": "request_event_id",
                    "answer": "request_event_id"}[kind]
        diagnostic(reason, ids={id_field: identity}, count=1)

    def keep(table, key, row, depth, identity=None):
        nonlocal count, halted
        if key in output[table]:
            return True
        if count >= limit:
            problem(table.rstrip("s"), identity or str(key), depth, "record_limit", cutoff=True)
            halted = True
            return False
        output[table][key] = row
        count += 1
        return True

    def queue(kind, identity, depth, expand):
        if identity is None or halted:
            return
        key = (kind, identity)
        if key in seen and (seen[key] or not expand):
            return
        if depth > max_depth:
            problem(kind, identity, depth, "depth_limit", cutoff=True)
            return
        if key in queued:
            old_depth, old_expand = queued[key]
            queued[key] = (min(depth, old_depth), expand or old_expand)
            return
        if len(pending) >= limit:
            problem(kind, identity, depth, "pending_limit", cutoff=True)
            return
        queued[key] = (depth, expand)
        pending.append(key)

    def artifacts(ids, depth):
        # Expand one structural level lazily. The retained manifest itself is
        # parsed once; its references are never copied into an unbounded queue.
        for artifact_id, expand in ids:
            ctx.check_deadline()
            if not artifact_id or artifact_id in output["artifacts"]:
                continue
            row = artifact_row(ctx, ctx.run_id, artifact_id)
            if row is None:
                problem("artifact", artifact_id, depth, "missing_artifact")
                continue
            if not keep("artifacts", artifact_id, artifact_record(ctx, row), depth):
                return
            if row["representation"] != "request_refs/2":
                continue
            if not expand:
                problem("artifact", artifact_id, depth, "nested_request_manifest")
                continue
            if row["body_text"] is None or row["redacted"] or row["truncated"]:
                problem("artifact", artifact_id, depth, "request_membership_unavailable")
                continue
            manifest = json_value(row["body_text"])
            if (type(manifest) is not dict or manifest.get("format") != "request_refs/2"
                    or type(manifest.get("template_ref")) is not str
                    or type(manifest.get("message_refs")) is not list
                    or type(manifest.get("field_refs")) is not list):
                problem("artifact", artifact_id, depth, "invalid_request_manifest")
                continue
            refs = chain((manifest["template_ref"],), manifest["message_refs"],
                         (field.get("artifact_id") if type(field) is dict else None for field in manifest["field_refs"]))
            for reference in refs:
                if type(reference) is not str or not reference:
                    problem("artifact", artifact_id, depth, "invalid_request_reference")
                    continue
                artifacts(((reference, False),), depth)
                if halted:
                    return

    def metadata(event, depth):
        if ctx.run_id not in output["runs"]:
            run = ctx.connection.execute("SELECT * FROM runs WHERE run_id=?", (ctx.run_id,)).fetchone()
            if not keep("runs", ctx.run_id, run_record(ctx, run), depth):
                return
        references = [(event["payload_ref"], True), (event["catalog_ref"], True)]
        if event["turn_id"]:
            if event["turn_id"] in output["turns"]:
                references.append((output["turns"][event["turn_id"]]["prompt_ref"], True))
            else:
                turn = ctx.connection.execute("SELECT * FROM turns WHERE run_id=? AND turn_id=?", (ctx.run_id, event["turn_id"])).fetchone()
                if turn is None:
                    problem("turn", event["turn_id"], depth, "missing_turn")
                elif keep("turns", turn["turn_id"], turn_record(ctx, turn), depth):
                    references.append((turn["prompt_ref"], True))
                else:
                    return
        artifacts(references, depth)
        if halted or event["kind"] != "model.request":
            return
        for member in member_rows(ctx, ctx.run_id, [event["event_id"]], limit=limit - count + 1):
            key = (member["request_event_id"], member["message_index"], member["message_artifact_id"])
            if not keep("members", key, member, depth, member["request_event_id"]):
                return
            if member["artifact_missing"]:
                problem("artifact", member["message_artifact_id"], depth, "missing_artifact")
        if not halted:
            info = request_info(ctx, ctx.run_id, event["event_id"])
            if info["status"] != "pass":
                problem("event", event["event_id"], depth, info["reason"])

    def keep_link(link, depth):
        if link["link_id"] not in output["links"] and not keep("links", link["link_id"], link_record(ctx, link), depth):
            return False
        status, reason = relation_state(ctx, ctx.run_id, link)
        if status != "pass":
            problem("link", link["link_id"], depth, reason)
        return True

    def request_answers(event, depth):
        details = json_value(event["details_json"])
        reason = association_diagnostics_reason(details)
        if reason:
            problem("event", event["event_id"], depth, reason)
        for answer in answer_rows(ctx, ctx.run_id, [event["event_id"]], limit=limit - count + 1):
            key = (answer["request_event_id"], answer["message_index"], answer["target_pointer"], answer["link_id"])
            if not keep("answers", key, answer, depth, answer["request_event_id"]):
                return
            link = ctx.connection.execute("SELECT * FROM links WHERE run_id=? AND link_id=?", (ctx.run_id, answer["link_id"])).fetchone()
            if link is None:
                problem("link", answer["link_id"], depth, "missing_link")
                continue
            if not keep_link(link, depth):
                return
            if answer["association_state"] != "linked":
                problem("link", link["link_id"], depth, answer["association_state"])
            queue("event", link["source_event_id"], depth + 1, True)
            queue("event", link["witness_event_id"], depth + 1, False)
        # A failed explicit first-witness claim remains evidence even when its
        # handle no longer joins a member. This does not invent an answer row or
        # expand historical claims of a context-only witness request.
        for link, _reason in witnessed_binding_failures(ctx, ctx.run_id, event["event_id"]):
            if not keep_link(link, depth):
                return
            queue("event", link["source_event_id"], depth + 1, True)
            queue("event", link["witness_event_id"], depth + 1, False)

    def visit_event(event, depth, expand):
        if event["event_id"] not in output["events"] and not keep("events", event["event_id"], event_record(ctx, event), depth):
            return
        metadata(event, depth)
        if halted:
            return
        follow_incoming = event["kind"] != "model.request" or expand
        if event["kind"] == "model.request" and expand:
            request_answers(event, depth)
        if halted:
            return
        if follow_incoming:
            incoming = ctx.connection.execute("""SELECT l.* FROM links l
                LEFT JOIN events w ON w.run_id=l.run_id AND w.event_id=l.witness_event_id
                WHERE l.run_id=? AND l.target_event_id=? AND
                  (l.relation<>'answers' OR coalesce(json_extract(l.details_json,'$.binding_format'),'')='')
                ORDER BY w.sequence IS NULL,w.sequence,l.link_id LIMIT ?""", (ctx.run_id, event["event_id"], limit + 1))
            for link in incoming:
                ctx.check_deadline()
                if not keep_link(link, depth):
                    return
                queue("event", link["source_event_id"], depth + 1, expand if link["relation"] == "dispatch_source" else True)
                queue("event", link["witness_event_id"], depth + 1, False)
        if expand:
            operations = ctx.connection.execute("""SELECT o.* FROM operations o
                LEFT JOIN events s ON s.run_id=o.run_id AND s.event_id=o.start_event_id
                WHERE o.run_id=? AND (o.operation_id=? OR o.start_event_id=? OR o.end_event_id=?
                  OR o.dispatch_event_id=? OR (?='model.tool_call' AND (o.decision_id=? OR EXISTS(
                    SELECT 1 FROM links l JOIN observed_events w ON w.run_id=l.run_id AND w.event_id=l.witness_event_id
                    WHERE l.run_id=o.run_id AND l.source_event_id=? AND l.relation='dispatch_source'
                      AND l.target_event_id=o.dispatch_event_id))))
                ORDER BY s.sequence IS NULL,s.sequence,o.operation_id LIMIT ?""",
                (ctx.run_id, event["operation_id"], event["event_id"], event["event_id"], event["event_id"],
                 event["kind"], event["decision_id"], event["event_id"], limit + 1))
            for operation in operations:
                queue("operation", operation["operation_id"], depth + 1, True)
        if event["kind"] in {"model.tool_call", "model.response"} and event["model_call_id"]:
            for related in ctx.connection.execute("SELECT event_id FROM observed_events WHERE run_id=? AND model_call_id=? AND kind IN ('model.request','model.response') ORDER BY sequence,event_id LIMIT ?",
                                                  (ctx.run_id, event["model_call_id"], limit + 1)):
                queue("event", related["event_id"], depth + 1, False)

    def visit_operation(operation, depth):
        if not keep("operations", operation["operation_id"], operation_record(ctx, operation), depth):
            return
        for name in ("start_event_id", "end_event_id", "dispatch_event_id"):
            queue("event", operation[name], depth + 1, False)
        for related in ctx.connection.execute("""SELECT event_id FROM observed_events WHERE run_id=? AND
              (operation_id=? OR (kind='model.tool_call' AND decision_id=?) OR
               (model_call_id=? AND kind IN ('model.request','model.response')))
            ORDER BY sequence,event_id LIMIT ?""", (ctx.run_id, operation["operation_id"], operation["decision_id"], operation["model_call_id"], limit + 1)):
            queue("event", related["event_id"], depth + 1, False)

    queue("event", request_event_id, 0, True)
    while pending and not halted:
        ctx.check_deadline()
        key = pending.popleft()
        depth, expand = queued.pop(key)
        if key in seen and (seen[key] or not expand):
            continue
        seen[key] = expand
        kind, identity = key
        if kind == "event":
            event = event_row(ctx, ctx.run_id, identity, observed=True)
            if event is None:
                problem(kind, identity, depth, "missing_or_unselected_event")
                continue
            visit_event(event, depth, expand)
        else:
            operation = ctx.connection.execute("SELECT * FROM operations WHERE run_id=? AND operation_id=?", (ctx.run_id, identity)).fetchone()
            if operation is None:
                problem(kind, identity, depth, "missing_operation")
                continue
            visit_operation(operation, depth)
    if pending:
        for key in pending:
            depth, _ = queued[key]
            problem(key[0], key[1], depth, "record_limit", cutoff=True)
    if request_event_id not in output["events"]:
        raise InspectionError("limit_exceeded", "required_root", "The intact root event cannot fit the slice limit.")

    def sequence(event_id):
        event = event_row(ctx, ctx.run_id, event_id)
        return (event is None, None if event is None else event["sequence"])

    data = {"root_request_event_id": request_event_id}
    for table in TABLES:
        data[table] = list(output[table].values())
    data["events"].sort(key=lambda row: (row["sequence"] is None, row["sequence"], row["event_id"]))
    data["operations"].sort(key=lambda row: (*sequence(row["start_event_id"]), row["operation_id"]))
    data["links"].sort(key=lambda row: (*sequence(row["witness_event_id"]), row["link_id"]))
    data["artifacts"].sort(key=lambda row: row["artifact_id"])
    data["turns"].sort(key=lambda row: (row["started_at"] is None, row["started_at"], row["turn_id"]))
    data["members"].sort(key=lambda row: (row["request_sequence"] is None, row["request_sequence"], row["message_index"], row["request_event_id"]))
    data["answers"].sort(key=lambda row: (row["request_sequence"] is None, row["request_sequence"], row["message_index"] is None, row["message_index"], row["target_pointer"] or "", row["link_id"], row["request_event_id"]))
    data.update(complete=complete, frontier=frontier)

    def shrink():
        data["complete"] = False
        for table in reversed(TABLES):
            rows = data[table]
            candidates = [index for index, row in enumerate(rows) if table != "events" or row["event_id"] != request_event_id]
            if candidates:
                row = rows.pop(candidates[-1])
                identity = row.get("artifact_id") or row.get("operation_id") or row.get("event_id") or row.get("link_id") or row.get("request_event_id") or row.get("turn_id") or row["run_id"]
                problem(table.rstrip("s"), identity, 0, "byte_limit", cutoff=True)
                return True
        if frontier:
            frontier.pop()
            ctx.diagnostics_omitted["cutoffs"] += 1
            return True
        return False

    return data, shrink


def slice(path, run_id, request_event_id, *, limit=1000, max_depth=12, max_output_bytes=65536, no_content=False, timeout=10.0):
    def validate():
        validate_int(limit, 1, 1000, "limit")
        validate_int(max_depth, 0, 12, "max_depth")

    return run_read("slice", path, {"run_id": run_id, "request_event_id": request_event_id},
                    lambda ctx: _slice(ctx, request_event_id, limit, max_depth),
                    max_output_bytes=max_output_bytes, no_content=no_content, timeout=timeout, validate=validate)
