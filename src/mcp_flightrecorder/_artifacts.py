"""Retained artifact and ordered-request preparation, without database writes."""

import hashlib
import json
import uuid

from ._config import boolean, fields
from ._values import (
    MISSING, InvalidInput, MESSAGE_PROJECTION, canonical_bytes, freeze, json_text,
    original_digest, pointer_get, pointer_tokens, text_digest,
    validate_control, validate_pointer,
)


def _ordered(value, name):
    if type(value) is not list:
        raise InvalidInput(name + " must be an ordered list")
    return value


def _encoded(text):
    try:
        return text.encode("utf-8")
    except UnicodeError as error:
        raise InvalidInput("retained JSON must be valid UTF-8") from error


def _artifact_row(run_id, fields, identity=None):
    """Preserve the reference field order and identity encoding exactly."""
    material = fields if identity is None else [identity, fields]
    prefix = run_id + (":" if identity is None else ":m:")
    artifact_id = prefix + hashlib.sha256(_encoded(json_text(material))).hexdigest()
    return {"artifact_id": artifact_id, **fields}


def _retention(capture, override):
    if override is None:
        return {"policy_id": capture, "retained": MISSING, "redacted": False,
                "truncated": False,
                "omitted_reason": "capture_metadata" if capture == "metadata" else None}
    fields(override, ("policy_id", "retained", "redacted", "truncated", "omitted_reason"),
            ("policy_id",))
    policy = validate_control(override["policy_id"], "retention policy", nonempty=True)
    retained = override.get("retained", MISSING)
    omitted = override.get("omitted_reason")
    if retained is MISSING:
        validate_control(omitted, "omitted reason", nonempty=True)
    elif omitted is not None:
        raise InvalidInput("retained content cannot also have an omission reason")
    return {"policy_id": policy, "retained": retained,
            "redacted": boolean(override.get("redacted", False), "redacted"),
            "truncated": boolean(override.get("truncated", False), "truncated"),
            "omitted_reason": "capture_metadata" if capture == "metadata" else omitted}


def _retained_request(original, policy, message_path, fields):
    """Keep the existing positional request contract for core and policy preflight."""
    retained = policy["retained"]
    if retained is MISSING:
        return original
    for path in (message_path, *fields):
        pointer_get(retained, path)
    originals = pointer_get(original, message_path)
    replacements = pointer_get(retained, message_path)
    if type(replacements) is not list or len(replacements) != len(originals):
        raise InvalidInput("retention must preserve every message array position")
    original_encodings = [canonical_bytes(item) for item in originals]
    original_encoding_set = set(original_encodings)
    for index, replacement in enumerate(replacements):
        encoding = canonical_bytes(replacement)
        if encoding != original_encodings[index] and encoding in original_encoding_set:
            raise InvalidInput("retention cannot reorder original messages")
    return retained


def _retained_text(original, original_text, policy):
    if policy["omitted_reason"] is not None:
        return None
    if original is MISSING:
        return None
    retained = policy["retained"]
    return original_text if retained is MISSING or retained is original else json_text(retained)


def _fields(run_id, original, policy, representation):
    original_text = None if original is MISSING else json_text(original)
    original_data = None if original_text is None else _encoded(original_text)
    observed_bytes = None if original_data is None else len(original_data)
    body = _retained_text(original, original_text, policy)
    data = b"" if body is None else (original_data if body is original_text else _encoded(body))
    return {"run_id": run_id, "media_type": "application/json", "representation": representation,
            "body_text": body,
            "stored_sha256": None if body is None else hashlib.sha256(data).hexdigest(),
            "stored_bytes": len(data), "observed_bytes": observed_bytes,
            "redacted": int(policy["redacted"]), "truncated": int(policy["truncated"]),
            "omitted_reason": (policy["omitted_reason"] or "unexposed") if body is None else None,
            "policy_id": policy["policy_id"]}


def _part_policy(policy, retained):
    return {**policy, "retained": retained}


def _template_copy(value, paths):
    """Copy path containers; borrow other frozen descendants until serialization."""
    template = value.copy()
    for path in paths:
        tokens = pointer_tokens(path)
        parent = template
        for token in tokens[:-1]:
            key = token if type(parent) is dict else int(token)
            parent[key] = parent[key].copy()
            parent = parent[key]
        key = tokens[-1] if type(parent) is dict else int(tokens[-1])
        parent[key] = None
    return template


def _unique_rows(rows):
    return list({row["artifact_id"]: row for row in rows}.values())


def _link_fields(run_id, witness, source, relation, *, target_pointer,
                 source_slot=None, source_pointer=None, details=None):
    return {"run_id": run_id, "witness_event_id": witness, "source_event_id": source,
            "target_event_id": witness, "relation": relation, "source_slot": source_slot,
            "source_pointer": source_pointer, "target_slot": "payload",
            "target_pointer": target_pointer, "details_json": json_text(details or {})}


def _link(run_id, witness, source, relation, *, target_pointer,
          source_slot=None, source_pointer=None, details=None):
    values = _link_fields(run_id, witness, source, relation, target_pointer=target_pointer,
                          source_slot=source_slot, source_pointer=source_pointer, details=details)
    identity = hashlib.sha256(_encoded(json_text(values))).hexdigest()
    return {"link_id": run_id + ":l:" + identity, **values}


def _binding_link(run_id, event_id, message_id, binding, message_pointer):
    details = {**binding["details"], "binding_format": "message_binding/2",
               "message_artifact_id": message_id, "relative_pointer": binding["pointer"]}
    values = _link_fields(run_id, event_id, binding["decision_event_id"], "answers",
                          target_pointer=message_pointer + binding["pointer"], details=details)
    material = json_text([message_id, binding])
    return {"link_id": run_id + ":b:" + hashlib.sha256(_encoded(material)).hexdigest(), **values}


class Artifacts:
    """Prepare immutable-by-ownership SQL row values for the single writer.

    The caller owns admission and all persistence. No state in this helper
    publishes a binding claim or survives independently of its prepared bundle.
    """

    def __init__(self, run_id, key, capture, comparisons):
        self.run_id = run_id
        self.key = key
        self.capture = capture
        self.comparisons = comparisons

    def artifact(self, original, retention=None):
        """Consume original and optional retention frozen by the preparation owner."""
        policy = _retention(self.capture, retention)
        return _artifact_row(self.run_id, _fields(self.run_id, original, policy, "json"))

    def _artifact(self, original, policy, identity=None, representation="json"):
        return _artifact_row(self.run_id, _fields(self.run_id, original, policy, representation), identity)

    def _digest(self, value, *, complete, utf8_size):
        return original_digest(value, key=self.key, run_id=self.run_id,
                               projection=MESSAGE_PROJECTION, complete=complete,
                               enabled=self.comparisons, utf8_size=utf8_size)

    def _text_digest(self, value, paths, *, complete):
        return text_digest(value, paths, key=self.key, run_id=self.run_id,
                           complete=complete, enabled=self.comparisons)

    def _binding(self, original, binding, complete, lookup):
        fields(binding, ("decision_event_id", "pointer", "result_event_id", "text_paths"),
                ("decision_event_id", "pointer"))
        decision = validate_control(binding["decision_event_id"], "decision event", nonempty=True)
        pointer = validate_pointer(binding["pointer"])
        block = pointer_get(original, pointer)
        result_id = validate_control(binding.get("result_event_id"), "result event", nullable=True,
                                     nonempty=True)
        details = {"result_event_id": result_id,
                   "submitted_text_digest": self._text_digest(
                       block, binding.get("text_paths"), complete=complete)}
        unresolved = []
        decision_event = lookup("event", decision)
        if decision_event is None or decision_event.get("kind") != "model.tool_call":
            unresolved.append({"pointer": pointer, "reason": "decision_unavailable"})
        if result_id is not None:
            result = lookup("event", result_id)
            if result is None or result.get("kind") != "mcp.response":
                unresolved.append({"pointer": pointer, "reason": "result_unavailable"})
        return {"decision_event_id": decision, "pointer": pointer, "details": details}, unresolved

    def _unresolved(self, original, values):
        result = []
        for item in _ordered(values, "unresolved bindings"):
            fields(item, ("pointer", "reason", "provider_ref"), ("pointer", "reason"))
            pointer = validate_pointer(item["pointer"])
            pointer_get(original, pointer)
            reason = validate_control(item["reason"], "unresolved reason", nonempty=True)
            output = {"pointer": pointer, "reason": reason}
            if "provider_ref" in item:
                output["provider_ref"] = validate_control(item["provider_ref"], "provider reference")
            result.append(output)
        return result

    def _message_annotation(self, original, annotation, complete, lookup):
        if annotation is MISSING:
            return None, [], complete
        fields(annotation, ("bindings", "unresolved", "complete"), ("bindings",))
        message_complete = boolean(annotation.get("complete", True), "message complete")
        complete = complete and message_complete
        unresolved = self._unresolved(original, annotation.get("unresolved", []))
        values = annotation["bindings"]
        if values is None:
            return None, unresolved, complete
        known, pointers = [], set()
        for value in _ordered(values, "bindings"):
            binding, unknown = self._binding(original, value, complete, lookup)
            if binding["pointer"] in pointers:
                raise InvalidInput("one message block must have one decision binding")
            pointers.add(binding["pointer"])
            known.append(binding)
            unresolved.extend(unknown)
        # Match the reference's request-independent canonical declaration order.
        known = [json.loads(text) for text in sorted(
            json.dumps(item, sort_keys=True, separators=(",", ":"), allow_nan=False)
            for item in known)]
        return known, unresolved, complete

    def _message(self, original, retained, annotation, complete, policy, lookup):
        bindings, unresolved, message_complete = self._message_annotation(
            original, annotation, complete, lookup)
        row_fields = _fields(self.run_id, original, _part_policy(policy, retained), "json")
        token = self._digest(original, complete=message_complete,
                             utf8_size=row_fields["observed_bytes"])
        reason = token.get("reason") or ("binding_context_unavailable" if bindings is None else None)
        if unresolved:
            reason = reason or "unresolved_bindings"
        identity = ({"original": token["value"], "bindings": bindings or []}
                    if reason is None else {"occurrence": uuid.uuid4().hex})
        artifact = _artifact_row(self.run_id, row_fields, identity)
        return {"row": artifact, "identity": identity, "bindings": bindings or [],
                "unresolved": unresolved, "reason": reason,
                "complete": message_complete}

    def _request_paths(self, original, facts):
        message_path = facts.get("message_path", "/messages")
        field_paths = _ordered(facts.get("field_paths", []), "field paths")
        catalog_path = facts.get("catalog_path")
        paths = field_paths if message_path is None else [message_path, *field_paths]
        validated = set()
        message_value = None
        for path in paths:
            validate_pointer(path, allow_root=False)
            value = pointer_get(original, path)
            if path in validated:
                raise InvalidInput("extracted request paths cannot overlap")
            validated.add(path)
            if path == message_path:
                message_value = value
        for path in paths:
            ancestor = path.rpartition("/")[0]
            while ancestor:
                if ancestor in validated:
                    raise InvalidInput("extracted request paths cannot overlap")
                ancestor = ancestor.rpartition("/")[0]
        if message_path is not None and type(message_value) is not list:
            raise InvalidInput("request messages must be an ordered array")
        if catalog_path is not None:
            validate_pointer(catalog_path, allow_root=False)
            if catalog_path not in field_paths:
                raise InvalidInput("catalog path must be one of the extracted fields")
        return message_path, field_paths, catalog_path

    def _annotations(self, facts, message_count):
        annotations = facts.get("messages")
        if annotations is None:
            return [MISSING] * message_count
        _ordered(annotations, "message annotations")
        if len(annotations) != message_count:
            raise InvalidInput("annotations must match every ordered message")
        return annotations

    def _insertions(self, original, facts, event_id, lookup):
        links, diagnostics = [], []
        for insertion in _ordered(facts.get("insertions", []), "insertions"):
            fields(insertion, ("source_event_id", "target_pointer", "content"),
                    ("source_event_id", "target_pointer", "content"))
            source = validate_control(insertion["source_event_id"], "insertion source", nonempty=True)
            pointer = validate_pointer(insertion["target_pointer"])
            try:
                destination = pointer_get(original, pointer)
            except InvalidInput:
                diagnostics.append({"pointer": pointer, "reason": "insertion_destination_absent"})
                continue
            if canonical_bytes(destination) != canonical_bytes(insertion["content"]):
                diagnostics.append({"pointer": pointer, "reason": "insertion_destination_mismatch"})
                continue
            known = lookup("event", source)
            if known is None or known.get("kind") != "host.tool_result":
                diagnostics.append({"pointer": pointer, "reason": "insertion_source_unavailable"})
            links.append(_link(self.run_id, event_id, source, "inserted_into",
                               source_slot="payload", source_pointer="", target_pointer=pointer))
        return links, diagnostics

    def _message_parts(self, original, retained, facts, policy, message_path, event_id, lookup):
        messages = pointer_get(original, message_path)
        replacements = pointer_get(retained, message_path)
        annotations = self._annotations(facts, len(messages))
        complete = facts.get("complete", True)
        rows, refs, links, details, identities = [], [], [], {}, {}
        losses = []
        for index, message in enumerate(messages):
            handle = self._message(message, replacements[index], annotations[index], complete, policy, lookup)
            row = handle["row"]
            rows.append(row)
            refs.append(row["artifact_id"])
            identities[row["artifact_id"]] = handle["identity"]
            location = message_path + "/" + str(index)
            links.extend(_binding_link(self.run_id, event_id, row["artifact_id"], binding, location)
                         for binding in handle["bindings"])
            if handle["reason"]:
                details.setdefault("message_identity_unavailable", []).append(
                    {"message_index": index, "reason": handle["reason"]})
            if handle["unresolved"]:
                details.setdefault("unresolved_answers", []).extend(
                    {**item, "pointer": location + item["pointer"]} for item in handle["unresolved"])
                losses.append("unresolved")
            if annotations[index] is MISSING or annotations[index].get("bindings") is None:
                losses.append("unresolved")
            if not handle["complete"]:
                losses.append("partial")
        return rows, refs, links, details, identities, losses

    def _template_parts(self, original, retained, policy, message_path, field_paths):
        paths = (message_path, *field_paths)
        template = _template_copy(original, paths)
        retained_template = template if retained is original else _template_copy(retained, paths)
        rows, fields = [], []
        for path in field_paths:
            value = pointer_get(original, path)
            replacement = value if retained is original else pointer_get(retained, path)
            row = self._artifact(value, _part_policy(policy, replacement))
            rows.append(row)
            fields.append({"pointer": path, "artifact_id": row["artifact_id"]})
        row = self._artifact(template, _part_policy(policy, retained_template))
        rows.append(row)
        return rows, fields, row["artifact_id"]

    def _manifest(self, manifest, policy):
        structural = {**policy, "retained": MISSING, "omitted_reason": None,
                      "redacted": False, "truncated": False}
        return self._artifact(manifest, structural, representation="request_refs/2")

    def _direct_fields(self, original, policy, field_paths):
        retained = original if policy["retained"] is MISSING else policy["retained"]
        rows, refs = [], []
        for path in field_paths:
            value, replacement = pointer_get(original, path), pointer_get(retained, path)
            row = self._artifact(value, _part_policy(policy, replacement))
            rows.append(row)
            refs.append({"pointer": path, "artifact_id": row["artifact_id"]})
        return rows, refs

    def _direct_request(self, original, policy, facts, event_id, lookup, field_paths, catalog_path):
        row = self._artifact(original, policy)
        field_rows, refs = self._direct_fields(original, policy, field_paths)
        catalog = next((item["artifact_id"] for item in refs if item["pointer"] == catalog_path), None)
        links, diagnostics = self._insertions(original, facts, event_id, lookup)
        details = {"request_limitation": "message_array_unavailable"}
        if refs:
            details["direct_field_refs"] = refs
        if diagnostics:
            details["insertion_diagnostics"] = diagnostics
        losses = ["unresolved"]
        if row["body_text"] is None:
            losses.append("body_omitted")
        if not facts.get("complete", True):
            losses.append("partial")
        return {"artifacts": _unique_rows([row, *field_rows]), "payload_ref": row["artifact_id"], "catalog_ref": catalog,
                "links": links, "details": details, "losses": tuple(dict.fromkeys(losses))}

    def request(self, original, facts, event_id, lookup):
        """Consume payload and facts already frozen by Preparation.observe."""
        policy = _retention(self.capture, facts.get("retention"))
        message_path, field_paths, catalog_path = self._request_paths(original, facts)
        if message_path is None:
            return self._direct_request(original, policy, facts, event_id, lookup, field_paths, catalog_path)
        retained = _retained_request(original, policy, message_path, field_paths)
        rows, refs, links, details, identities, losses = self._message_parts(
            original, retained, facts, policy, message_path, event_id, lookup)
        templates, fields, template_ref = self._template_parts(
            original, retained, policy, message_path, field_paths)
        manifest = {"format": "request_refs/2", "template_ref": template_ref,
                    "message_path": message_path, "message_refs": refs, "field_refs": fields}
        manifest_row = self._manifest(manifest, policy)
        rows.extend([*templates, manifest_row])
        insertions, diagnostics = self._insertions(original, facts, event_id, lookup)
        links.extend(insertions)
        if diagnostics:
            details["insertion_diagnostics"] = diagnostics
            losses.append("unresolved")
        if any(row["body_text"] is None for row in rows):
            losses.append("body_omitted")
        catalog = next((item["artifact_id"] for item in fields if item["pointer"] == catalog_path), None)
        return {"artifacts": _unique_rows(rows), "payload_ref": manifest_row["artifact_id"],
                "catalog_ref": catalog, "links": links, "details": details,
                "losses": tuple(dict.fromkeys(losses)),
                "_identities": identities}

    def omit_artifact(self, row, *, identity=None):
        """Create the retention-distinct omitted row without retaining its body."""
        if row["body_text"] is None:
            return dict(row)
        fields = {key: value for key, value in row.items() if key != "artifact_id"}
        fields.update(body_text=None, stored_sha256=None, stored_bytes=0, omitted_reason="capacity")
        return _artifact_row(self.run_id, fields, identity)

    def _remap_manifest(self, row, mapping):
        manifest = json.loads(row["body_text"])
        manifest["template_ref"] = mapping.get(manifest["template_ref"], manifest["template_ref"])
        manifest["message_refs"] = [mapping.get(item, item) for item in manifest["message_refs"]]
        manifest["field_refs"] = [{**item, "artifact_id": mapping.get(item["artifact_id"], item["artifact_id"])}
                                  for item in manifest["field_refs"]]
        policy = {"policy_id": row["policy_id"], "retained": MISSING,
                  "redacted": False, "truncated": False, "omitted_reason": None}
        return self._manifest(manifest, policy)

    def _remap_binding(self, link, mapping):
        if link["relation"] != "answers":
            return dict(link)
        details = json.loads(link["details_json"])
        message_id = mapping.get(details["message_artifact_id"], details["message_artifact_id"])
        relative = details["relative_pointer"]
        stable = {key: value for key, value in details.items()
                  if key not in ("message_artifact_id", "relative_pointer", "binding_format")}
        # Reference bind_message consumes canonicalized binding dictionaries.
        binding = json.loads(json.dumps({"decision_event_id": link["source_event_id"],
                                         "pointer": relative, "details": stable},
                                        sort_keys=True, separators=(",", ":"), allow_nan=False))
        location = link["target_pointer"][:-len(relative)] if relative else link["target_pointer"]
        return _binding_link(self.run_id, link["witness_event_id"], message_id, binding, location)

    def omit_prepared(self, prepared, *, include_manifest):
        """Omit optional bodies with reference repair; never shorten membership.

        If even structural membership cannot be admitted, include_manifest
        replaces it with an explicitly omitted artifact and removes its answer
        candidates. The envelope records that loss instead of asserting a
        complete request or a binding that has no stored membership witness.
        """
        identities = prepared.get("_identities", {})
        mapping, rows, retained_manifests = {}, [], []
        for row in prepared["artifacts"]:
            if row["representation"] == "request_refs/2" and not include_manifest:
                retained_manifests.append(row)
                continue
            omitted = self.omit_artifact(row, identity=identities.get(row["artifact_id"]))
            rows.append(omitted)
            mapping[row["artifact_id"]] = omitted["artifact_id"]
        for row in retained_manifests:
            remapped = self._remap_manifest(row, mapping)
            rows.append(remapped)
            mapping[row["artifact_id"]] = remapped["artifact_id"]
        details = freeze(prepared["details"])
        if "direct_field_refs" in details:
            details["direct_field_refs"] = [
                {**item, "artifact_id": mapping.get(item["artifact_id"], item["artifact_id"])}
                for item in details["direct_field_refs"]]
        if include_manifest:
            details["request_limitation"] = ("manifest_omitted" if any(
                row["representation"] == "request_refs/2" for row in prepared["artifacts"])
                else "direct_request_omitted")
            # These entries describe absent membership. Preserve their counts
            # and explicit omission instead of keeping an unbounded shadow
            # manifest in control metadata.
            for field in ("unresolved_answers", "message_identity_unavailable", "direct_field_refs"):
                if field in details:
                    details[field + "_omitted_count"] = len(details.pop(field))
        links = [self._remap_binding(link, mapping) for link in prepared["links"]
                 if not include_manifest or link["relation"] != "answers"]
        payload_ref = mapping.get(prepared["payload_ref"], prepared["payload_ref"])
        catalog_ref = mapping.get(prepared["catalog_ref"], prepared["catalog_ref"])
        if include_manifest:
            # Once membership itself is unavailable, orphan placeholders do
            # not contribute evidence. Retain only the envelope's direct
            # references, with an explicit lost-manifest diagnostic above.
            rows = [row for row in rows if row["artifact_id"] in (payload_ref, catalog_ref)]
        losses = (*prepared["losses"], "capacity", "body_omitted", "partial")
        return {"artifacts": _unique_rows(rows), "links": links,
                "payload_ref": payload_ref, "catalog_ref": catalog_ref,
                "details": details, "losses": tuple(dict.fromkeys(losses))}
