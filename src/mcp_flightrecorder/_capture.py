"""Prepare observed rows and immutable bundles before atomic admission."""
from dataclasses import replace
import hashlib
import json
import math

from ._config import boolean, choice, fields, integer, sequence
from ._types import Bundle, CaptureResult, Identity, Ticket
from ._values import (
    MISSING, InvalidInput, arguments_digest, freeze, json_text, text_digest,
    validate_control, validate_pointer,
)


COMMON_FACTS = ("complete", "retention", "instance_id")
OBSERVATIONS = {
    "model.request": ("model", ("message_path", "messages", "field_paths", "catalog_path", "insertions")),
    "model.tool_call": ("model", ("name", "model_call_id", "provider_ref")),
    "model.response": ("model", ("model_call_id", "state", "usage")),
    "host.tool_result": ("host", ("sources", "text_paths")),
    "cache.served": (None, ("owner", "operation_id", "client_id", "cache_id", "cache_scope", "ttl_ms")),
}


def _normalize_usage(usage):
    """Detach only the three optional reported counts; discard other values."""
    normalized = {}
    for name in ("prompt_tokens", "cached_tokens", "cache_write_tokens"):
        value = usage.get(name) if type(usage) is dict else None
        normalized[name] = value if type(value) is int and 0 <= value <= 2**63 - 1 else None
    return normalized


def encode(value):
    try:
        return json_text(value).encode("utf-8")
    except UnicodeError as error:
        raise InvalidInput("captured JSON must encode as UTF-8") from error


def link_row(run_id, witness, source, relation, *, source_slot=None, source_pointer=None):
    row = {"run_id": run_id, "witness_event_id": witness, "source_event_id": source,
           "target_event_id": witness, "relation": relation, "source_slot": source_slot,
           "source_pointer": source_pointer, "target_slot": None,
           "target_pointer": None, "details_json": "{}"}
    return {"link_id": run_id + ":l:" + hashlib.sha256(encode(row)).hexdigest(), **row}


def event_metadata(event):
    """Retain only control attribution needed by later producers and fallback."""
    result = {name: event.get(name) for name in ("kind", "model_call_id")}
    details = json.loads(event["details_json"])
    selected = {name: details[name] for name in ("decision_event_ids",) if name in details}
    result.update(selected)
    return result


def identities_for(rows):
    entries = []
    if "event" in rows:
        event = rows["event"]
        metadata = event_metadata(event)
        entries.append(("event", event["event_id"], metadata))
        if event["kind"] == "model.request":
            entries.append(("model", event["model_call_id"], metadata))
        if event["kind"] == "model.tool_call":
            entries.append(("decision", event["decision_id"], metadata))
    return tuple(Identity(category, key, encode(value)) for category, key, value in entries)


def body_loss(artifacts):
    return any(row["body_text"] is None for row in artifacts)


class Prepared:
    """Mutable only during one producer call; no caller objects are retained."""
    def __init__(self, recorder, kind, rows, receipt, *, payload=None,
                 losses=(), partial=False, ticket=None, terminal=False):
        self.recorder, self.kind, self.rows, self.receipt = recorder, kind, rows, receipt
        self.payload = payload
        self.losses = tuple(dict.fromkeys(losses))
        self.partial, self.ticket, self.terminal = partial, ticket, terminal

    def _payload_variant(self, omission):
        if self.payload is None:
            return None
        if omission == 0:
            return self.payload
        return self.recorder._artifacts.omit_prepared(self.payload, include_manifest=omission > 1)

    def _plain_variant(self, rows, omission):
        artifacts = self.rows.get("artifacts", [])
        if not omission:
            rows["artifacts"] = artifacts
            return ()
        if self.terminal and omission > 1:
            rows["artifacts"] = []
            rows["event"]["payload_ref"] = None
            details = json.loads(rows["event"]["details_json"])
            details["payload_omitted_reason"] = "capacity"
            rows["event"]["details_json"] = json_text(details)
            return ("body_omitted", "capacity", "partial")
        replacements = [self.recorder._artifacts.omit_artifact(row) if row["body_text"] is not None else row
                        for row in artifacts]
        mapping = {old["artifact_id"]: new["artifact_id"] for old, new in zip(artifacts, replacements)}
        rows["artifacts"] = replacements
        if "event" in rows:
            for name in ("payload_ref", "catalog_ref"):
                rows["event"][name] = mapping.get(rows["event"].get(name), rows["event"].get(name))
        if "turn" in rows:
            rows["turn"]["prompt_ref"] = mapping.get(rows["turn"]["prompt_ref"], rows["turn"]["prompt_ref"])
        return ("body_omitted", "capacity", "partial") if any(old != new for old, new in zip(artifacts, replacements)) else ()

    def bundle(self, omission=0):
        rows = {name: dict(value) if type(value) is dict else value for name, value in self.rows.items()}
        payload = self._payload_variant(omission)
        losses = list(self.losses)
        if payload is None:
            losses.extend(self._plain_variant(rows, omission))
        else:
            rows["artifacts"], rows["links"] = payload["artifacts"], payload["links"]
            rows["event"]["payload_ref"], rows["event"]["catalog_ref"] = payload["payload_ref"], payload["catalog_ref"]
            details = {**json.loads(rows["event"]["details_json"]), **payload["details"]}
            rows["event"]["details_json"] = json_text(details)
            losses.extend(payload.get("losses", ()))
        if body_loss(rows.get("artifacts", ())):
            losses.append("body_omitted")
        losses = tuple(dict.fromkeys(losses))
        partial = self.partial or any(reason in ("capacity", "partial", "unresolved") for reason in losses)
        if partial and "partial" not in losses:
            losses += ("partial",)
        if "event" in rows:
            details = json.loads(rows["event"]["details_json"])
            if losses:
                details["capture_losses"] = list(losses)
            rows["event"]["details_json"] = json_text(details)
        identities = identities_for(rows)
        envelope, bodies = dict(rows), []
        envelope["artifacts"] = []
        for index, artifact in enumerate(rows.get("artifacts", ())):
            row = dict(artifact)
            if row["body_text"] is not None:
                bodies.append((index, row["body_text"].encode("utf-8")))
                row["body_text"] = None
            envelope["artifacts"].append(row)
        receipt = replace(self.receipt, losses=losses)
        optional = b""
        if self.terminal and envelope["artifacts"]:
            optional = encode(envelope["artifacts"])
            envelope["artifacts"] = []
        return Bundle(encode(envelope), tuple(bodies), self.kind, receipt.turn_id,
                      identities, receipt,
                      "event" in rows, losses, partial, self.ticket, self.terminal, optional=optional)


class Preparation:
    def __init__(self, recorder, *, lookup):
        self.rec = recorder
        self.lookup = lookup

    def _digest_args(self, payload, complete):
        return arguments_digest(payload, key=self.rec._key, run_id=self.rec._run_id,
                                complete=complete, enabled=self.rec._comparisons)

    def _digest_text(self, payload, paths, complete):
        return text_digest(payload, paths, key=self.rec._key, run_id=self.rec._run_id,
                           complete=complete, enabled=self.rec._comparisons)

    def _event(self, kind, boundary, scope, *, complete, instance_id=None, **values):
        self.rec._require_boundary(boundary)
        return {"event_id": self.rec._new_id("e"), "run_id": self.rec._run_id,
                "turn_id": scope.turn_id, "event_version": 1, "kind": kind,
                "boundary": boundary, "provenance": "observed",
                "source_id": self.rec._coverage["producers"][boundary],
                "observed_at": self.rec._utc_now(), "observed_mono_ms": self.rec._mono_ms(),
                "model_call_id": None, "decision_id": None, "operation_id": None,
                "payload_ref": None, "catalog_ref": None,
                "details_json": json_text({"complete": complete, "instance_id": instance_id}), **values}

    def _reference(self, category, identity, *, kind=None):
        validate_control(identity, category + " reference", nullable=True, nonempty=True)
        found = None if identity is None else self.lookup(category, identity)
        if found is not None and (kind is None or found.get("kind") == kind):
            return found, ()
        return None, ("unresolved",)

    def _simple(self, event, payload, retention, *, losses=(), partial,
                links=(), operation=None, update=None, ticket=None, terminal=False):
        artifact = self.rec._artifacts.artifact(payload, retention)
        event["payload_ref"] = artifact["artifact_id"]
        rows = {"run_id": self.rec._run_id, "event": event, "artifacts": [artifact], "links": list(links)}
        if operation is not None:
            rows["operation"] = operation
        if update is not None:
            rows["operation_update"] = update
        receipt = CaptureResult("accepted", event_id=event["event_id"], turn_id=event["turn_id"],
                                model_call_id=event.get("model_call_id"), decision_id=event.get("decision_id"),
                                operation_id=event.get("operation_id"), ticket=ticket)
        return Prepared(self.rec, event["kind"], rows, receipt,
                        losses=losses, partial=partial, ticket=ticket, terminal=terminal)

    def observe(self, kind, scope, payload, facts):
        if type(kind) is not str or kind not in OBSERVATIONS:
            raise InvalidInput("unsupported observation kind")
        if kind == "model.response" and type(facts) is dict:
            facts = facts.copy()
            usage = facts.pop("usage", None)
            if usage is not None:
                facts["usage"] = _normalize_usage(usage)
        facts = freeze({} if facts is None else facts)
        boundary, specific = OBSERVATIONS[kind]
        fields(facts, (*COMMON_FACTS, *specific))
        complete = boolean(facts.get("complete", True), "complete")
        validate_control(facts.get("instance_id"), "instance ID", nullable=True)
        payload = freeze(payload)
        if kind == "cache.served":
            owner = facts.get("owner")
            if owner not in ("host", "client"):
                raise InvalidInput("cache observation requires host or client owner")
            boundary = "host" if owner == "host" else "mcp"
        event = self._event(kind, boundary, scope, complete=complete, instance_id=facts.get("instance_id"))
        if kind == "model.request":
            return self._request(event, payload, facts, complete)
        if kind == "model.tool_call":
            return self._decision(event, payload, facts, complete)
        if kind == "model.response":
            return self._response(event, payload, facts, complete)
        if kind == "host.tool_result":
            return self._prepared_result(event, payload, facts, complete)
        return self._cache(event, payload, facts, complete)

    def _request(self, event, payload, facts, complete):
        event["model_call_id"] = self.rec._new_id("m")
        prepared = self.rec._artifacts.request(payload, facts, event["event_id"], self.lookup)
        rows = {"run_id": self.rec._run_id, "event": event}
        receipt = CaptureResult("accepted", event_id=event["event_id"], turn_id=event["turn_id"],
                                model_call_id=event["model_call_id"])
        return Prepared(self.rec, event["kind"], rows, receipt, payload=prepared, partial=not complete)

    def _decision(self, event, payload, facts, complete):
        if "name" not in facts:
            raise InvalidInput("tool decision requires an explicit name or null")
        validate_control(facts["name"], "tool name", nullable=True)
        validate_control(facts.get("provider_ref"), "provider reference", nullable=True)
        if payload is not MISSING and type(payload) is not dict:
            raise InvalidInput("a model tool call exposes an object")
        _, losses = self._reference("model", facts.get("model_call_id"))
        event.update(model_call_id=facts.get("model_call_id"), decision_id=self.rec._new_id("d"), name=facts["name"])
        details = json.loads(event["details_json"])
        details.update(args_digest=self._digest_args(payload, complete), provider_ref=facts.get("provider_ref"))
        if losses:
            details["unavailable_context"] = "model_request_unavailable"
        event["details_json"] = json_text(details)
        return self._simple(event, payload, facts.get("retention"),
                            losses=losses, partial=not complete)

    def _response(self, event, payload, facts, complete):
        state = facts.get("state", "complete")
        if state not in ("complete", "partial", "error", "cancelled"):
            raise InvalidInput("unsupported model response state")
        _, losses = self._reference("model", facts.get("model_call_id"))
        event.update(model_call_id=facts.get("model_call_id"), state=state)
        details = json.loads(event["details_json"])
        if losses:
            details["unavailable_context"] = "model_request_unavailable"
        if "usage" in facts:
            details["usage"] = facts["usage"]
        event["details_json"] = json_text(details)
        return self._simple(event, payload, facts.get("retention"),
                            losses=losses, partial=not complete or state != "complete")

    def _prepared_result(self, event, payload, facts, complete):
        sources = facts.get("sources", [])
        if type(sources) is not list:
            raise InvalidInput("sources must be a list")
        links, losses = [], []
        for source in sources:
            fields(source, ("event_id", "slot", "pointer"), ("event_id",))
            validate_control(source["event_id"], "source event", nonempty=True)
            slot, pointer = source.get("slot", "payload"), source.get("pointer", "")
            if slot not in ("payload", "catalog"):
                raise InvalidInput("invalid source slot")
            validate_pointer(pointer)
            _, unknown = self._reference("event", source["event_id"])
            losses.extend(unknown)
            links.append(link_row(self.rec._run_id, event["event_id"], source["event_id"],
                                  "transformed_from", source_slot=slot, source_pointer=pointer))
        details = json.loads(event["details_json"])
        details["result_text_digest"] = self._digest_text(payload, facts.get("text_paths"), complete)
        event["details_json"] = json_text(details)
        return self._simple(event, payload, facts.get("retention"),
                            losses=losses, partial=not complete, links=links)

    def _cache(self, event, payload, facts, complete):
        details = json.loads(event["details_json"])
        for name in ("operation_id", "client_id", "cache_id", "cache_scope"):
            validate_control(facts.get(name), name, nullable=True)
            details[name] = facts.get(name)
        ttl = facts.get("ttl_ms")
        if ttl is not None and (type(ttl) not in (int, float) or ttl < 0
                                or type(ttl) is float and not math.isfinite(ttl)):
            raise InvalidInput("cache TTL must be finite nonnegative milliseconds")
        details.update(owner=facts["owner"], ttl_ms=ttl)
        event.update(operation_id=facts.get("operation_id"), client_id=facts.get("client_id"),
                     details_json=json_text(details))
        return self._simple(event, payload, facts.get("retention"), partial=not complete)

    def dispatch(self, scope, action, name, arguments, decision_event_ids, complete):
        choice(action, ("execute", "refuse", "local", "defer"), "dispatch action")
        validate_control(name, "dispatch name", nullable=True)
        boolean(complete, "complete")
        arguments = freeze(arguments)
        event = self._event("host.dispatch", "host", scope, complete=complete, state=action, name=name)
        sources = list(sequence(decision_event_ids, "decision sources"))
        links, losses = [], []
        for source in sources:
            validate_control(source, "decision event", nonempty=True)
            _, unknown = self._reference("event", source, kind="model.tool_call")
            losses.extend(unknown)
            links.append(link_row(self.rec._run_id, event["event_id"], source, "dispatch_source"))
        payload = MISSING if arguments is MISSING else {"arguments": arguments}
        details = json.loads(event["details_json"])
        details.update(args_digest=self._digest_args(payload, complete), decision_event_ids=sources)
        event["details_json"] = json_text(details)
        return self._simple(event, payload, None, losses=losses,
                            partial=not complete, links=links)

    def _attribution(self, scope, decision_id, dispatch_event_id, link_basis):
        validate_control(decision_id, "decision", nullable=True, nonempty=True)
        validate_control(dispatch_event_id, "dispatch", nullable=True, nonempty=True)
        choice(link_basis, ("host_hook", "propagated", "none"), "link basis")
        if link_basis == "none":
            if decision_id is not None or dispatch_event_id is not None:
                raise InvalidInput("unassigned operation cannot carry explicit attribution")
            return None, None, ()
        if link_basis == "propagated" and (decision_id is None or dispatch_event_id is not None):
            raise InvalidInput("propagated attribution requires only a decision occurrence")
        if link_basis == "host_hook":
            if dispatch_event_id is not None and scope.dispatch_event_id not in (None, dispatch_event_id):
                raise InvalidInput("explicit and scoped dispatch disagree")
            dispatch_event_id = dispatch_event_id or scope.dispatch_event_id
            if dispatch_event_id is None:
                raise InvalidInput("host_hook attribution requires a dispatch")
        losses, model_call_id = [], None
        if decision_id is not None:
            decision, unknown = self._reference("decision", decision_id)
            losses.extend(unknown)
            if decision is not None:
                model_call_id = decision.get("model_call_id")
        if dispatch_event_id is not None:
            dispatch, unknown = self._reference("event", dispatch_event_id, kind="host.dispatch")
            losses.extend(unknown)
            if decision_id is None and dispatch is not None:
                known = []
                for source in dict.fromkeys(dispatch.get("decision_event_ids", ())):
                    found = self.lookup("event", source)
                    if found is not None and found.get("kind") == "model.tool_call":
                        known.append(found)
                if len(known) == 1:
                    model_call_id = known[0].get("model_call_id")
        return dispatch_event_id, model_call_id, tuple(losses)

    def start(self, scope, *, client_id, method, arguments, server_key, name,
              decision_id, dispatch_event_id, link_basis, complete, retention):
        for field, value in (("client_id", client_id), ("method", method)):
            validate_control(value, field, nonempty=True)
        validate_control(server_key, "server_key", nullable=True)
        validate_control(name, "name", nullable=True)
        boolean(complete, "complete")
        arguments = freeze(arguments)
        retention = None if retention is None else freeze(retention)
        dispatch, model, losses = self._attribution(scope, decision_id, dispatch_event_id, link_basis)
        operation_id = self.rec._new_id("o")
        event = self._event("mcp.request", "mcp", scope, complete=complete,
                            model_call_id=model, decision_id=decision_id, operation_id=operation_id,
                            client_id=client_id, server_key=server_key, method=method, name=name)
        payload = MISSING if arguments is MISSING else {"arguments": arguments}
        details = json.loads(event["details_json"])
        details["args_digest"] = self._digest_args(payload, complete)
        event["details_json"] = json_text(details)
        operation = {"run_id": self.rec._run_id, "operation_id": operation_id,
                     "turn_id": scope.turn_id, "model_call_id": model, "decision_id": decision_id,
                     "dispatch_event_id": dispatch, "link_basis": link_basis,
                     "client_id": client_id, "server_key": server_key, "method": method, "name": name,
                     "start_event_id": event["event_id"], "started_at": event["observed_at"]}
        ticket_data = {field: operation[field] for field in
                       ("turn_id", "dispatch_event_id", "model_call_id", "decision_id",
                        "operation_id", "client_id", "server_key", "method", "name")}
        ticket_data["started_mono_ms"] = event["observed_mono_ms"]
        ticket = Ticket(self.rec._ticket_owner, operation_id, encode(ticket_data))
        self.rec._validate_reservation(ticket, self)
        return self._simple(event, payload, retention, losses=losses,
                            partial=not complete, operation=operation, ticket=ticket)

    def _terminal_error(self, error):
        if error is None:
            return None
        error = freeze(error)
        fields(error, ("origin", "type", "code"), ("origin", "type", "code"))
        validate_control(error["origin"], "error origin", max_bytes=64)
        validate_control(error["type"], "error type", max_bytes=64)
        integer(error["code"], "error code")
        return error

    def finish(self, ticket, *, status, result, resolution, input_rounds, error, text_paths, complete, retention):
        choice(status, ("ok", "tool_error", "protocol_error", "transport_error", "timeout", "cancelled", "local_error"),
               "terminal outcome")
        choice(resolution, ("remote", "client_cache", "host_cache", "local", "mixed", "unknown"), "resolution")
        integer(input_rounds, "input_rounds", nonnegative=True)
        boolean(complete, "complete")
        error, result = self._terminal_error(error), freeze(result)
        data = json.loads(ticket.encoded)
        scope = self.rec._issue_scope(data["turn_id"], data["dispatch_event_id"])
        identity = {name: data[name] for name in
                    ("model_call_id", "decision_id", "operation_id", "client_id", "server_key", "method", "name")}
        event = self._event("mcp.response", "mcp", scope, complete=complete, state=status, **identity)
        details = json.loads(event["details_json"])
        details.update(result_text_digest=self._digest_text(result, text_paths, complete), error=error)
        event["details_json"] = json_text(details)
        update = {"run_id": self.rec._run_id, "operation_id": data["operation_id"], "status": status,
                  "resolution": resolution, "input_rounds": input_rounds, "end_event_id": event["event_id"],
                  "ended_at": event["observed_at"],
                  "duration_ms": max(0.0, event["observed_mono_ms"] - data["started_mono_ms"]),
                  "error_code": None if error is None else error["code"]}
        retention = None if retention is None else freeze(retention)
        return self._simple(event, result, retention, partial=not complete,
                            update=update, ticket=ticket, terminal=True)
