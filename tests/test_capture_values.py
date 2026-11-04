"""Public capture freezing, retention and original-comparison evidence controls."""
from collections.abc import Mapping
from contextlib import closing
import hashlib
import hmac
import json
import sqlite3
import subprocess
import sys
import threading

import pytest
from pytest_check import check
from mcp_flightrecorder import MISSING, Recorder
from mcp_flightrecorder import _values as comparison_values

COVERAGE = {"producers": {"model": "test.model", "host": "test.host", "mcp": "test.mcp"},
            "model_complete": True}


class HostileValue:
    def __repr__(self):
        raise AssertionError("capture must reject unsupported objects without invoking their representation")


def accept(receipt, why="observation is the prerequisite for persisted-fact checks"):
    assert receipt.status == "accepted", f"{why}: expected accepted, got {receipt.status}/{receipt.reason}"
    return receipt


def observe(recorder, kind, payload=MISSING, **facts):
    return accept(recorder.observe(kind, scope=recorder.capture_scope(), payload=payload, facts=facts))


def request(recorder, value, **facts):
    receipt = observe(recorder, "model.request", value, **facts)
    observe(recorder, "model.response", {}, model_call_id=receipt.model_call_id)
    return receipt


def event_details(db, event_id):
    row = db.execute("SELECT details_json FROM events WHERE event_id=?", (event_id,)).fetchone()
    assert row is not None, "accepted event must persist before its comparison evidence can be inspected"
    return json.loads(row[0])


def payload_artifact(db, event_id):
    db.row_factory = sqlite3.Row
    row = db.execute("SELECT a.* FROM artifacts a JOIN events e ON e.payload_ref=a.artifact_id WHERE e.event_id=?", (event_id,)).fetchone()
    assert row is not None, "an admitted observed payload or its omission envelope must be represented by an artifact"
    return row


def test_submission_freezes_payload_annotations_prompt_and_conditions(tmp_path, monkeypatch):
    entered, release = threading.Event(), threading.Event()
    original_persist = Recorder._persist_bundle

    def hold_first(self, connection, bundle):
        if not entered.is_set():
            entered.set()
            if not release.wait(10):
                raise RuntimeError("test-owned writer barrier was not released")
        return original_persist(self, connection, bundle)

    monkeypatch.setattr(Recorder, "_persist_bundle", hold_first)
    path = tmp_path / "freeze.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    prompt = {"prompt": ["original prompt"]}
    conditions = {"z": {"limit": 1}, "a": [True]}
    value = {"messages": [{"role": "user", "content": "original message"}], "tools": [{"name": "original"}]}
    annotations = [{"bindings": [], "unresolved": [], "complete": True}]
    retention = {"policy_id": "frozen-policy", "retained": {"visible": "before"}, "redacted": True}
    original_request = {
        "messages": [{"role": "user", "content": "original one"},
                     {"role": "user", "content": "original two"}],
        "tools": [{"name": "original tool", "enabled": True}],
        "untouched": {"nested": ["original sibling"], "flags": [1, True, 1.0]},
    }
    retained_request = {
        "messages": [{"content": "masked", "role": "user"},
                     {"content": "masked", "role": "user"}],
        "tools": [{"enabled": False, "name": "retained"}],
        "untouched": {"nested": ["retained"], "flags": [1.0, False, 1]},
    }
    original_request_bytes = (b'{"messages":[{"role":"user","content":"original one"},'
        b'{"role":"user","content":"original two"}],"tools":[{"name":"original tool","enabled":true}],'
        b'"untouched":{"nested":["original sibling"],"flags":[1,true,1.0]}}')
    retained_request_bytes = (b'{"messages":[{"content":"masked","role":"user"},'
        b'{"content":"masked","role":"user"}],"tools":[{"enabled":false,"name":"retained"}],'
        b'"untouched":{"nested":["retained"],"flags":[1.0,false,1]}}')
    request_annotations = [{"bindings": [], "unresolved": [], "complete": True} for _ in range(2)]
    request_retention = {"policy_id": "frozen-request", "retained": retained_request,
                         "redacted": True, "truncated": True}
    try:
        with recorder.turn(prompt, conditions=conditions) as turn:
            accept(turn.receipt)
            assert entered.wait(5), "writer barrier must hold admitted work before producer mutation"
            receipt = request(recorder, value, messages=annotations, field_paths=["/tools"], catalog_path="/tools")
            retained_receipt = observe(recorder, "host.tool_result", {"secret": "original"}, retention=retention)
            replacement_receipt = request(recorder, original_request, messages=request_annotations,
                field_paths=["/tools"], catalog_path="/tools", retention=request_retention)
            assert not replacement_receipt.losses, "The retained request requires complete admitted evidence."
            check.equal(json.dumps(original_request, ensure_ascii=False, separators=(",", ":"),
                                   allow_nan=False).encode(), original_request_bytes,
                        msg="public capture leaves the original caller tree unchanged while the writer is held")
            check.equal(json.dumps(retained_request, ensure_ascii=False, separators=(",", ":"),
                                   allow_nan=False).encode(), retained_request_bytes,
                        msg="public capture leaves the distinct retained caller tree unchanged while the writer is held")
            retention["retained"]["visible"] = "after"
            retention["policy_id"] = "mutated-policy"
            prompt["prompt"][0] = "mutated prompt"
            conditions["z"]["limit"] = 999
            value["messages"][0]["content"] = "mutated message"
            value["tools"][0]["name"] = "mutated catalog"
            annotations[0]["complete"] = False
            annotations.append({"bindings": None})
            original_request["messages"][0]["content"] = "changed original one"
            original_request["messages"][1]["content"] = "changed original two"
            original_request["tools"][0]["name"] = "changed original field"
            original_request["untouched"]["nested"][0] = "changed original sibling"
            original_request["untouched"]["flags"].reverse()
            retained_request["messages"][0]["content"] = "changed retained one"
            retained_request["messages"][1]["content"] = "changed retained two"
            retained_request["tools"][0]["name"] = "changed retained field"
            retained_request["untouched"]["nested"][0] = "changed retained sibling"
            retained_request["untouched"]["flags"].reverse()
            request_retention.update(policy_id="changed-request", redacted=False, truncated=False)
            request_annotations[0]["complete"] = False
            request_annotations[1]["bindings"] = None
            request_annotations.append({"bindings": None})
    finally:
        release.set()
        closed = recorder.close()
    assert closed.writer_stopped and closed.portable, "The released writer must join and close portably before reading frozen values."
    check.equal(closed.run_close_state, "clean", msg="caller mutation cannot turn complete admitted requests into a discarded or partial run")
    with closing(sqlite3.connect(path)) as db:
        db.row_factory = sqlite3.Row
        turn_row = db.execute("SELECT * FROM turns").fetchone()
        assert turn_row is not None, "accepted turn must persist its frozen original metadata"
        check.equal(json.loads(turn_row["conditions_json"]), {"z": {"limit": 1}, "a": [True]},
                    msg="post-entry condition mutation must not rewrite captured declared conditions")
        expected_key = hashlib.sha256(json.dumps({"z": {"limit": 1}, "a": [True]}, sort_keys=True,
            ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()).hexdigest()
        check.equal(turn_row["condition_key"], expected_key, msg="condition key belongs to frozen canonical conditions")
        stored_prompt = db.execute("SELECT body_text FROM artifacts WHERE artifact_id=?", (turn_row["prompt_ref"],)).fetchone()
        assert stored_prompt is not None, "turn prompt reference must resolve before interpreting it"
        check.equal(json.loads(stored_prompt[0]), {"prompt": ["original prompt"]}, msg="turn prompt freezes at entry")
        manifest = json.loads(payload_artifact(db, receipt.event_id)["body_text"])
        member = db.execute("SELECT body_text FROM artifacts WHERE artifact_id=?", (manifest["message_refs"][0],)).fetchone()
        assert member is not None, "the admitted message must remain present after caller mutation"
        check.equal(json.loads(member[0]), {"role": "user", "content": "original message"}, msg="submitted message freezes before admission returns")
        catalog = db.execute("SELECT a.body_text FROM events e JOIN artifacts a ON e.catalog_ref=a.artifact_id WHERE e.event_id=?", (receipt.event_id,)).fetchone()
        assert catalog is not None, "the actual submitted catalog must have a resolvable per-event artifact"
        check.equal(json.loads(catalog[0]), [{"name": "original"}], msg="catalog bytes must not follow later caller edits")
        retained = payload_artifact(db, retained_receipt.event_id)
        check.equal((json.loads(retained["body_text"]), retained["policy_id"]), ({"visible": "before"}, "frozen-policy"),
                    msg="retention replacement and policy are frozen with the original observation")
        check.equal(turn_row["capture_state"], "complete", msg="mutating caller annotation objects cannot retroactively mark captured evidence partial")

        replacement_manifest = json.loads(payload_artifact(db, replacement_receipt.event_id)["body_text"])
        check.equal((replacement_manifest["format"], replacement_manifest["message_path"]),
                    ("request_refs/2", "/messages"), msg="retained capture keeps the original request representation and path")
        assert len(replacement_manifest["field_refs"]) == 1, "The retained request has exactly its one declared extracted field."
        field = replacement_manifest["field_refs"][0]
        check.equal(field["pointer"], "/tools", msg="the extracted retained field keeps its literal pointer and order")
        template = db.execute("SELECT * FROM artifacts WHERE artifact_id=?", (replacement_manifest["template_ref"],)).fetchone()
        field_row = db.execute("SELECT * FROM artifacts WHERE artifact_id=?", (field["artifact_id"],)).fetchone()
        assert template is not None and field_row is not None, "Both retained template and extracted field must resolve."
        original_template_bytes = b'{"messages":null,"tools":null,"untouched":{"nested":["original sibling"],"flags":[1,true,1.0]}}'
        retained_template_bytes = b'{"messages":null,"tools":null,"untouched":{"nested":["retained"],"flags":[1.0,false,1]}}'
        original_field_bytes = b'[{"name":"original tool","enabled":true}]'
        retained_field_bytes = b'[{"enabled":false,"name":"retained"}]'
        members = db.execute("""SELECT m.message_index,m.message_pointer,m.message_artifact_id,a.*
            FROM request_members m JOIN artifacts a ON a.artifact_id=m.message_artifact_id
            WHERE m.request_event_id=? ORDER BY m.message_index""", (replacement_receipt.event_id,)).fetchall()
        assert len(members) == 2, "Caller mutation must not remove either original message position."
        check.equal([(row["message_index"], row["message_pointer"]) for row in members],
                    [(0, "/messages/0"), (1, "/messages/1")], msg="retained messages keep their ordered original positions")
        check.not_equal(members[0]["message_artifact_id"], members[1]["message_artifact_id"],
                        msg="distinct original messages keep distinct handles even when retained bytes and policy are equal")
        check.equal(replacement_manifest["message_refs"], [row["message_artifact_id"] for row in members],
                    msg="both distinct original identities remain in the retained structural manifest")
        original_message_bytes = [b'{"role":"user","content":"original one"}',
                                  b'{"role":"user","content":"original two"}']
        retained_message_bytes = b'{"content":"masked","role":"user"}'
        artifact_cases = [(template, original_template_bytes, retained_template_bytes),
                          (field_row, original_field_bytes, retained_field_bytes)]
        artifact_cases.extend((row, original_message_bytes[index], retained_message_bytes)
                              for index, row in enumerate(members))
        for row, original_bytes, retained_bytes in artifact_cases:
            check.equal(row["body_text"], retained_bytes.decode(),
                        msg="durable retained content preserves its exact pre-mutation keys, values, scalar types and order")
            check.equal((row["observed_bytes"], row["stored_bytes"], row["stored_sha256"]),
                        (len(original_bytes), len(retained_bytes), hashlib.sha256(retained_bytes).hexdigest()),
                        msg="observed length describes the original part while stored length and hash describe the retained bytes")
            check.equal((row["policy_id"], row["redacted"], row["truncated"], row["omitted_reason"]),
                        ("frozen-request", 1, 1, None), msg="each retained part keeps the declared pre-mutation policy and flags")
        check.equal(db.execute("SELECT catalog_ref FROM events WHERE event_id=?", (replacement_receipt.event_id,)).fetchone()[0],
                    field["artifact_id"], msg="the request catalog still resolves to its retained extracted field")
        check.equal(event_details(db, replacement_receipt.event_id), {"complete": True, "instance_id": None},
                    msg="later annotation mutation cannot add incomplete, unavailable-identity or loss diagnostics")
        check.equal([tuple(row) for row in db.execute("SELECT model_call_id,state FROM events WHERE kind='model.response' ORDER BY sequence")],
                    [(receipt.model_call_id, "complete"), (replacement_receipt.model_call_id, "complete")],
                    msg="both captured model requests retain their actual matching complete responses")
        check.equal(tuple(db.execute("SELECT close_state,dropped_events,writer_errors FROM runs").fetchone()),
                    ("clean", 0, 0), msg="durable health remains clean after both caller trees and their metadata are mutated")
    health = recorder.health()
    check.equal((health.dropped_events, health.writer_errors), (0, 0),
                msg="detached retained requests introduce no dropped observation or writer error")


def test_condition_key_ignores_dictionary_insertion_order(tmp_path):
    path = tmp_path / "conditions.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    try:
        with recorder.turn("first", conditions={"z": 1, "a": {"two": 2, "one": 1}}):
            pass
        with recorder.turn("second", conditions={"a": {"one": 1, "two": 2}, "z": 1}):
            pass
    finally:
        recorder.close()
    with closing(sqlite3.connect(path)) as db:
        keys = [row[0] for row in db.execute("SELECT condition_key FROM turns")]
    assert len(keys) == 2, "both admitted turns are needed to compare equivalent condition encodings"
    check.equal(keys[0], keys[1], msg="canonical condition hashing sorts keys at every object depth")


@pytest.mark.parametrize("usage,expected", [
    ({"prompt_tokens": 0, "cached_tokens": 0, "cache_write_tokens": 0}, (0, 0, 0)),
    ({"prompt_tokens": 2 ** 63 - 1, "cached_tokens": 2 ** 63 - 1,
      "cache_write_tokens": 2 ** 63 - 1}, (2 ** 63 - 1,) * 3),
    ({"prompt_tokens": 1, "cached_tokens": 2, "cache_write_tokens": 3}, (1, 2, 3)),
    ({}, (None, None, None)),
    ({"prompt_tokens": 10, "cached_tokens": 8}, (10, 8, None)),
    ({"cached_tokens": 0}, (None, 0, None)),
    ({"prompt_tokens": True, "cached_tokens": 8, "cache_write_tokens": 2}, (None, 8, 2)),
    ({"prompt_tokens": 10, "cached_tokens": False, "cache_write_tokens": 2}, (10, None, 2)),
    ({"prompt_tokens": 10, "cached_tokens": 8, "cache_write_tokens": 2.0}, (10, 8, None)),
    ({"prompt_tokens": "10", "cached_tokens": 8, "cache_write_tokens": None}, (None, 8, None)),
    ({"prompt_tokens": float("nan"), "cached_tokens": -1,
      "cache_write_tokens": 2 ** 63}, (None, None, None)),
    ({"prompt_tokens": HostileValue(), "cached_tokens": 8, "cache_write_tokens": 2}, (None, 8, 2)),
    ([HostileValue()], (None, None, None)),
], ids=["explicit-zero", "int64-maximum", "inconsistent-valid-reports", "empty-object",
        "missing-write", "missing-prompt-and-write", "boolean-prompt", "boolean-cached",
        "float-write", "string-prompt", "nonfinite-negative-overflow", "unsupported-count",
        "non-dictionary"])
def test_response_usage_normalizes_counts_without_rejecting_response(tmp_path, usage, expected):
    path = tmp_path / "response-usage.sqlite"
    recorder = Recorder(path, capture="metadata", coverage=COVERAGE)
    original = dict(usage) if type(usage) is dict else None
    try:
        with recorder.turn("usage counts") as turn:
            accept(turn.receipt)
            source = observe(recorder, "model.request", {"messages": []}, messages=[])
            facts = {"model_call_id": source.model_call_id, "instance_id": "usage-model", "usage": usage}
            response = accept(recorder.observe("model.response", scope=recorder.capture_scope(),
                payload={"secret": "unretained-response-body"}, facts=facts))
            assert facts["usage"] is usage, "Normalization must not replace the caller's usage value."
            if original is not None:
                assert set(usage) == set(original) and all(usage[key] is value for key, value in original.items()), \
                    "Missing and invalid counts must remain unchanged in the caller's dictionary."
    finally:
        closed = recorder.close()
    assert closed.writer_stopped and closed.portable
    check.equal(closed.run_close_state, "clean", msg="Unavailable optional counts do not make valid capture partial.")
    with closing(sqlite3.connect(path)) as db:
        details = event_details(db, response.event_id)
        check.equal(details, {"complete": True, "instance_id": "usage-model", "capture_losses": ["body_omitted"],
                              "usage": dict(zip(("prompt_tokens", "cached_tokens", "cache_write_tokens"), expected))},
                    msg="Only the three exact int64-or-null counts are merged with existing response details.")
        check.is_not_in("unretained-response-body", "\n".join(db.iterdump()),
                        msg="Preserving usage never requires storing the omitted response body.")
        check.equal(db.execute("SELECT capture_state FROM turns").fetchone()[0], "complete",
                    msg="Invalid or missing optional usage does not lose the response observation.")
    check.equal((recorder.health().dropped_events, recorder.health().writer_errors), (0, 0),
                msg="Optional normalization does not drop an otherwise valid response.")


def test_response_usage_ignores_extra_trees_and_detaches_before_admission(tmp_path, monkeypatch):
    entered, release = threading.Event(), threading.Event()
    original_persist = Recorder._persist_bundle

    def hold_first(self, connection, bundle):
        if not entered.is_set():
            entered.set()
            if not release.wait(10):
                raise RuntimeError("test-owned usage barrier was not released")
        return original_persist(self, connection, bundle)

    monkeypatch.setattr(Recorder, "_persist_bundle", hold_first)
    path = tmp_path / "usage-snapshot.sqlite"
    recorder = Recorder(path, capture="metadata", coverage=COVERAGE)
    discarded = []
    discarded.append(discarded)
    hostile = HostileValue()
    usage = {"prompt_tokens": 100, "cached_tokens": 80, "cache_write_tokens": 20,
             "cycle": discarded, "unsupported": hostile, 7: float("inf")}
    try:
        with recorder.turn("usage ownership") as turn:
            accept(turn.receipt)
            assert entered.wait(5), "Writer must be held before mutating admitted caller facts."
            source = observe(recorder, "model.request", {"messages": []}, messages=[])
            facts = {"model_call_id": source.model_call_id, "instance_id": "before", "usage": usage}
            response = accept(recorder.observe("model.response", scope=recorder.capture_scope(),
                payload={}, facts=facts))
            assert facts["usage"] is usage and facts["instance_id"] == "before"
            assert (usage["prompt_tokens"], usage["cached_tokens"], usage["cache_write_tokens"]) == (100, 80, 20)
            assert usage["cycle"] is discarded and discarded[0] is discarded
            assert usage["unsupported"] is hostile and usage[7] == float("inf")
            usage.update(prompt_tokens=999, cached_tokens=998, cache_write_tokens=997)
            facts.update(instance_id="after", usage={"prompt_tokens": 1})
    finally:
        release.set()
        closed = recorder.close()
    assert closed.writer_stopped and closed.portable
    with closing(sqlite3.connect(path)) as db:
        check.equal(event_details(db, response.event_id),
                    {"complete": True, "instance_id": "before", "capture_losses": ["body_omitted"],
                     "usage": {"prompt_tokens": 100, "cached_tokens": 80, "cache_write_tokens": 20}},
                    msg="Discarded extras are never frozen and post-admission mutation cannot change selected counts.")
    check.equal(closed.run_close_state, "clean", msg="Discarded malformed extras are not evidence loss.")


def test_response_usage_does_not_invoke_custom_mapping_or_integer_hooks(tmp_path):
    touches = []

    def forbidden(self, *args, **kwargs):
        touches.append(type(self).__name__)
        raise AssertionError("optional usage must not invoke custom hooks")

    class CustomDictionary(dict):
        __getitem__ = __iter__ = __len__ = get = items = keys = forbidden

    class CustomMapping(Mapping):
        __getitem__ = __iter__ = __len__ = forbidden

    class CustomInteger(int):
        __int__ = __index__ = __lt__ = __le__ = __gt__ = __ge__ = forbidden

    path = tmp_path / "usage-hooks.sqlite"
    recorder = Recorder(path, capture="metadata", coverage=COVERAGE)
    receipts = []
    try:
        with recorder.turn("usage hooks") as turn:
            accept(turn.receipt)
            source = observe(recorder, "model.request", {"messages": []}, messages=[])
            for usage in (CustomDictionary(prompt_tokens=10), CustomMapping(),
                          {"prompt_tokens": CustomInteger(10), "cached_tokens": 8, "cache_write_tokens": 2}):
                receipts.append(observe(recorder, "model.response", {},
                    model_call_id=source.model_call_id, usage=usage))
    finally:
        closed = recorder.close()
    assert closed.writer_stopped and closed.portable
    check.equal(touches, [], msg="Nonordinary dictionaries and nonexact integers are treated as unavailable without hooks.")
    with closing(sqlite3.connect(path)) as db:
        check.equal([event_details(db, receipt.event_id)["usage"] for receipt in receipts],
                    [{"prompt_tokens": None, "cached_tokens": None, "cache_write_tokens": None}] * 2
                    + [{"prompt_tokens": None, "cached_tokens": 8, "cache_write_tokens": 2}],
                    msg="Custom usage containers normalize to nulls and a custom integer does not erase valid siblings.")


def test_response_usage_none_is_absent_and_unrelated_inputs_remain_strict(tmp_path):
    class CustomFacts(dict):
        pass

    path = tmp_path / "usage-boundary.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    absent = []
    rejected = []
    usage = {"prompt_tokens": 10, "cached_tokens": 8, "cache_write_tokens": 2}
    try:
        with recorder.turn("usage boundary") as turn:
            accept(turn.receipt)
            source = observe(recorder, "model.request", {"messages": []}, messages=[])
            for provided in (False, True):
                facts = {"model_call_id": source.model_call_id}
                if provided:
                    facts["usage"] = None
                absent.append(accept(recorder.observe("model.response", scope=recorder.capture_scope(),
                    payload={}, facts=facts)))
                assert ("usage" in facts) is provided, "Removing direct None must not mutate caller facts."
            for facts, payload in (
                ({"model_call_id": source.model_call_id, "usage": usage, "complete": "yes"}, {}),
                ({"model_call_id": source.model_call_id, "usage": usage, "state": "invented"}, {}),
                ({"model_call_id": source.model_call_id, "usage": usage, "unexpected": 1}, {}),
                ({"model_call_id": source.model_call_id, "usage": usage}, HostileValue()),
                ({"model_call_id": source.model_call_id, "usage": usage,
                  "retention": {"retained": HostileValue()}}, {}),
            ):
                rejected.append(recorder.observe("model.response", scope=recorder.capture_scope(),
                    payload=payload, facts=facts))
            rejected.append(recorder.observe("host.tool_result", scope=recorder.capture_scope(),
                payload={}, facts={"usage": usage}))
            rejected.append(recorder.observe("model.request", scope=recorder.capture_scope(),
                payload={"messages": []}, facts={"messages": [], "usage": usage}))
            rejected.append(recorder.observe("model.response", scope=recorder.capture_scope(),
                payload={}, facts=CustomFacts(model_call_id=source.model_call_id, usage=usage)))
    finally:
        closed = recorder.close()
    assert closed.writer_stopped and closed.portable
    for receipt in rejected:
        check.equal((receipt.status, receipt.reason, receipt.event_id), ("rejected", "invalid_input", None),
                    msg="Usage is optional only for response facts; unrelated admission stays strict.")
    with closing(sqlite3.connect(path)) as db:
        for receipt in absent:
            check.is_not_in("usage", event_details(db, receipt.event_id),
                            msg="Absent usage and an explicit public None both omit the key.")
        check.equal(db.execute("SELECT COUNT(*) FROM events WHERE kind='model.response'").fetchone()[0], 2,
                    msg="Invalid unrelated facts or payloads never become persisted responses.")


def test_metadata_omission_preserves_original_comparison_and_clean_capture(tmp_path):
    path = tmp_path / "metadata.sqlite"
    secret = "comparison-secret-unretained"
    recorder = Recorder(path, capture="metadata", coverage=COVERAGE)
    try:
        with recorder.turn(secret):
            source = observe(recorder, "model.request", {"messages": []}, messages=[])
            decision = observe(recorder, "model.tool_call", {"arguments": {"secret": secret}}, name="tool", model_call_id=source.model_call_id)
            observe(recorder, "model.response", {"secret": secret}, model_call_id=source.model_call_id)
            with recorder.dispatch(action="execute", name="tool", arguments={"secret": secret}, decision_event_ids=[decision.event_id]) as dispatch:
                accept(dispatch.receipt)
                start = accept(recorder.start_operation(scope=recorder.capture_scope(), client_id="c", method="tools/call",
                    arguments={"secret": secret}, name="tool", decision_id=decision.decision_id, link_basis="host_hook"))
                terminal = accept(recorder.finish_operation(start.ticket, status="ok", result=secret, text_paths=[""]))
            prepared = observe(recorder, "host.tool_result", secret, sources=[{"event_id": terminal.event_id}], text_paths=[""])
    finally:
        closed = recorder.close()
    health = recorder.health()
    check.equal(closed.run_close_state, "clean", msg="declared metadata retention omits bodies without losing observations")
    check.equal(health.dropped_events, 0, msg="configured body omission does not count as a dropped event")
    check.greater(health.reason_counts.get("body_omitted", 0), 0, msg="retention omission remains visible in bounded health diagnostics")
    with closing(sqlite3.connect(path)) as db:
        tokens = [event_details(db, event_id)["args_digest"] for event_id in (decision.event_id, dispatch.receipt.event_id, start.event_id)]
        for token in tokens:
            check.equal((token.get("scope"), token.get("projection"), token.get("basis")),
                        (recorder.capture_scope().run_id, "args_json_python/1", "observed_original"),
                        msg="metadata comparisons remain named, run-scoped original projections")
        check.is_true(all(token.get("value") for token in tokens), msg="complete originals retain HMAC comparisons despite body omission")
        check.equal(len({token["value"] for token in tokens}), 1, msg="equal model/host/MCP originals compare equal before retention")
        result_token = event_details(db, terminal.event_id)["result_text_digest"]
        check.is_true(bool(result_token.get("value")), msg="result-text token survives metadata capture")
        check.is_not_in(secret, "\n".join(db.iterdump()), msg="omitted originals cannot leak into artifact bodies, control metadata or diagnostics")
        check.is_not_in(secret, repr((decision, start, terminal, prepared)), msg="receipts cannot contain omitted payloads")
        check.equal(db.execute("SELECT capture_state FROM turns").fetchone()[0], "complete", msg="configured retention alone preserves turn completeness")


@pytest.mark.parametrize("payload,complete,available,rejected", [
    ({"arguments": {"k": 1}}, True, True, False),
    ({"arguments_raw": '{"k":1}'}, True, True, False),
    ({"arguments": {"k": 1}, "arguments_raw": '{"k":1}'}, True, True, False),
    ({"arguments": {"k": 2}, "arguments_raw": '{"k":1,"k":2}'}, True, False, False),
    ({"arguments": True, "arguments_raw": "1"}, True, False, False),
    ({"arguments": {"k": 1}, "arguments_raw": "{"}, True, False, False),
    ({}, True, False, False),
    ({"arguments": {"k": 1}}, False, False, False),
    ({"arguments": "x" * 1_048_577}, True, False, False),
    ({"arguments_raw": '{"k":NaN}'}, True, False, False),
    ({"arguments_raw": '{"k":1e999}'}, True, False, False),
    ({"arguments": {1: "coerced key"}}, True, False, True),
    ({"arguments": float("nan")}, True, False, True),
    ({"arguments": float("inf")}, True, False, True),
    ({"arguments": HostileValue()}, True, False, True),
], ids=["parsed-dictionary", "raw-only", "raw-parsed-agree", "duplicate-raw-keys", "typed-conflict",
        "invalid-raw", "unexposed-arguments", "partial-original", "oversized-projection",
        "raw-nan", "raw-overflow", "nonstring-key", "nan", "infinity", "hostile-sdk-object"])
def test_argument_projection_uses_exposed_complete_typed_values(tmp_path, payload, complete, available, rejected):
    path = tmp_path / "arguments.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    try:
        baseline = observe(recorder, "model.tool_call", {"arguments": {"k": 1}}, name="tool")
        receipt = recorder.observe("model.tool_call", scope=recorder.capture_scope(), payload=payload,
                                   facts={"name": "tool", "complete": complete})
        if rejected:
            check.equal((receipt.status, receipt.reason), ("rejected", "invalid_input"),
                        msg="non-JSON and hostile values must be rejected at the public capture boundary without coercion")
            check.is_none(receipt.event_id, msg="an invalid original cannot publish an event or usable comparison evidence")
        else:
            accept(receipt)
    finally:
        recorder.close()
    with closing(sqlite3.connect(path)) as db:
        if rejected:
            check.equal(db.execute("SELECT event_id FROM events").fetchall(), [(baseline.event_id,)],
                        msg="only the valid neighboring original may persist after unsupported input rejection")
            check.is_true(bool(event_details(db, baseline.event_id)["args_digest"].get("value")),
                          msg="the valid neighboring original must retain its usable comparison token")
            return
        token = event_details(db, receipt.event_id)["args_digest"]
        if available:
            check.equal(token.get("value"), event_details(db, baseline.event_id)["args_digest"].get("value"),
                        msg="equivalent parsed JSON has the same original argument token in this run")
            check.is_true(bool(token.get("value")), msg="valid complete argument projection must expose its token")
        else:
            check.is_none(token.get("value"), msg="ambiguous, partial, unexposed or oversized arguments cannot produce a comparable digest")
            check.is_true(bool(token.get("reason")), msg="unavailable projection must explain the comparison limitation")
        artifact = payload_artifact(db, receipt.event_id)
        check.equal(json.loads(artifact["body_text"]), payload, msg="raw and parsed observations are retained separately without syntax repair")


@pytest.mark.parametrize("result,text_paths,complete,available", [
    ("value", [""], True, True), ("value", [], True, True), ("value", None, True, False),
    ({"field": 7}, ["/field"], True, False), ({"field": "v"}, ["/missing"], True, False),
    ("value", [""], False, False),
    ({"field": ["x"]}, ["/field"], True, False),
], ids=["string-root", "observed-empty-blocks", "unavailable-projection", "nonstring-block", "missing-block", "partial-result", "nested-block"])
def test_text_projection_never_coerces_unavailable_evidence(tmp_path, result, text_paths, complete, available):
    path = tmp_path / "text.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    try:
        start = accept(recorder.start_operation(scope=recorder.capture_scope(), client_id="c", method="m", arguments={}))
        receipt = accept(recorder.finish_operation(start.ticket, status="ok", result=result,
                         text_paths=text_paths, complete=complete))
    finally:
        recorder.close()
    with closing(sqlite3.connect(path)) as db:
        token = event_details(db, receipt.event_id)["result_text_digest"]
        check.equal(bool(token.get("value")), available, msg="only complete ordered string projections support text comparisons")
        if not available:
            check.is_true(bool(token.get("reason")), msg="missing/nonstring/partial extraction must explain its unavailable token")


def test_retained_sha_and_loss_flags_describe_only_retained_bytes(tmp_path):
    path = tmp_path / "retention.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    try:
        with recorder.turn("retention"):
            retained = observe(recorder, "host.tool_result", {"secret": "original"}, retention={
                "policy_id": "mask", "retained": {"secret": "…"}, "redacted": True, "truncated": True})
            omitted = observe(recorder, "host.tool_result", {"secret": "original"}, retention={
                "policy_id": "remove", "omitted_reason": "policy_removed", "redacted": True, "truncated": True})
    finally:
        closed = recorder.close()
    with closing(sqlite3.connect(path)) as db:
        kept = payload_artifact(db, retained.event_id)
        expected = json.dumps({"secret": "…"}, ensure_ascii=False, separators=(",", ":")).encode()
        check.equal((kept["stored_sha256"], kept["stored_bytes"]), (hashlib.sha256(expected).hexdigest(), len(expected)),
                    msg="stored digest and size describe retained UTF-8 bytes only")
        check.equal((kept["redacted"], kept["truncated"], kept["omitted_reason"]), (1, 1, None),
                    msg="redaction and truncation remain independent of omission")
        lost = payload_artifact(db, omitted.event_id)
        check.equal((lost["body_text"], lost["stored_sha256"], lost["stored_bytes"], lost["omitted_reason"], lost["redacted"], lost["truncated"]),
                    (None, None, 0, "policy_removed", 1, 1), msg="configured omission preserves its independent retained-loss metadata")
    check.equal(closed.run_close_state, "clean", msg="declared retention losses do not imply missing observations")
    check.equal(recorder.health().dropped_events, 0, msg="admitted retention envelopes are not dropped event attempts")


def test_capacity_only_body_omission_admits_envelope_and_marks_capture_partial(tmp_path):
    path = tmp_path / "capacity.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE, limits={"ordinary_bytes": 16_384})
    try:
        with recorder.turn("capacity"):
            receipt = observe(recorder, "host.tool_result", "sensitive-body-" * 15_000, text_paths=[""])
    finally:
        closed = recorder.close()
    check.equal(recorder.health().dropped_events, 0, msg="body-capacity omission keeps the accepted event envelope without counting a dropped event")
    check.equal(closed.run_close_state, "incomplete", msg="capacity-driven evidence loss prevents a clean-run claim")
    check.greater(recorder.health().reason_counts.get("body_omitted", 0), 0, msg="capacity omission is observable in bounded health")
    with closing(sqlite3.connect(path)) as db:
        artifact = payload_artifact(db, receipt.event_id)
        check.is_none(artifact["body_text"], msg="an over-capacity body must not bypass the encoded ordinary-byte bound")
        check.is_true(bool(artifact["omitted_reason"]), msg="capacity omission must retain an explicit artifact reason")
        check.equal(db.execute("SELECT capture_state FROM turns").fetchone()[0], "partial", msg="affected turn records evidence loss despite envelope acceptance")
        check.is_not_in("sensitive-body-", "\n".join(db.iterdump()), msg="omitted body cannot be hidden in control metadata")


@pytest.mark.parametrize("retained", [
    {"messages": []}, {"missing": [{"content": "masked"}]},
], ids=["removed-array-member", "removed-message-path"])
def test_request_retention_cannot_change_observed_membership(tmp_path, retained):
    path = tmp_path / "invalid-retention.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    try:
        receipt = recorder.observe("model.request", scope=recorder.capture_scope(),
            payload={"messages": [{"content": "original"}]}, facts={"messages": [{"bindings": []}],
            "retention": {"policy_id": "invalid-structure", "retained": retained, "redacted": True}})
    finally:
        recorder.close()
    check.equal((receipt.status, receipt.reason), ("rejected", "invalid_input"),
                msg="retention cannot fabricate a different submitted message membership")
    check.is_none(receipt.model_call_id, msg="rejected structural retention cannot publish a request prerequisite")


def test_comparison_tokens_preserve_json_types_key_order_and_text_framing(tmp_path, monkeypatch):
    typed_values = (True, 1, 1.0, "1", {"b": 2, "a": 1}, {"a": 1, "b": 2},
                    [2, 3], [3, 2], ["x"])
    canonical_messages = (b'true', b'1', b'1.0', b'"1"', b'{"a":1,"b":2}',
                          b'{"a":1,"b":2}', b'[2,3]', b'[3,2]', b'["x"]')
    escaped = {"\x7f": "\x00\t\n\r\x1f\"\\\x7f", "é": "漢字", "😀": "\U00010000\U0010ffff"}
    escaped_canonical = (b'{"\\u007f":"\\u0000\\t\\n\\r\\u001f\\"\\\\\\u007f",'
                         b'"\\u00e9":"\\u6f22\\u5b57","\\ud83d\\ude00":"\\ud800\\udc00\\udbff\\udfff"}')
    hints = []
    encoded_digest = comparison_values._encoded_digest

    def traced(value, key, projection, **options):
        hints.append((projection, options.get("utf8_size")))
        return encoded_digest(value, key, projection, **options)

    monkeypatch.setattr(comparison_values, "_encoded_digest", traced)
    recordings = []
    for index in range(2):
        path = tmp_path / f"typed-comparisons-{index}.sqlite"
        recorder = Recorder(path, capture="content", coverage=COVERAGE)
        try:
            choices = [observe(recorder, "model.tool_call", {"arguments": value}, name="typed")
                       for value in typed_values]
            for value, canonical in [*zip(typed_values, canonical_messages), (escaped, escaped_canonical)]:
                original_size = len(json.dumps(value, ensure_ascii=False, separators=(",", ":"),
                                               allow_nan=False).encode("utf-8"))
                token = comparison_values.original_digest(value, key=recorder._key,
                    run_id=recorder.capture_scope().run_id, projection="message_json/1",
                    complete=True, enabled=True, utf8_size=original_size)
                expected = hmac.new(recorder._key,
                    b"mcp-flightrecorder\0observed_original\0message_json/1\0" + canonical,
                    hashlib.sha256).hexdigest()
                check.equal(token, {"value": expected, "scope": recorder.capture_scope().run_id,
                    "projection": "message_json/1", "basis": "observed_original"},
                    msg="hinted messages preserve independently specified typed, sorted ASCII JSON and HMAC framing")
            terminals = []
            for value in (["ab", "c"], ["a", "bc"], ["ab", "c"], ["e\u0301"], ["\u00e9"], ["x"]):
                start = accept(recorder.start_operation(scope=recorder.capture_scope(), client_id="c", method="m", arguments={}))
                terminals.append(accept(recorder.finish_operation(start.ticket, status="ok", result=value,
                    text_paths=[f"/{block}" for block in range(len(value))])))
        finally:
            recorder.close()
        with closing(sqlite3.connect(path)) as db:
            tokens = [event_details(db, receipt.event_id)["args_digest"] for receipt in choices]
            args = [token["value"] for token in tokens]
            texts = [event_details(db, receipt.event_id)["result_text_digest"]["value"] for receipt in terminals]
        assert all(args) and all(texts), "every complete valid public original must yield a token before its distinctions can be compared"
        check.equal(len(set(args[:4])), 4, msg="boolean, integer, float and string observations retain distinct JSON types")
        check.equal(args[4], args[5], msg="argument projection canonicalizes object-key order without changing JSON types")
        check.not_equal(args[6], args[7], msg="JSON array order is observed content and cannot be canonicalized away")
        check.not_equal(texts[0], texts[1], msg="ordered text blocks are framed instead of ambiguously concatenated")
        check.equal(texts[0], texts[2], msg="repeated exact ordered text blocks compare equally within the run")
        check.not_equal(texts[3], texts[4], msg="text comparisons preserve distinct Unicode code-point sequences")
        check.not_equal(args[8], texts[5], msg="the same JSON list remains distinct across argument and text projection domains")
        recordings.append(tokens)
    check.not_equal(recordings[0][1]["scope"], recordings[1][1]["scope"],
                    msg="two actual recordings must issue distinct run scopes")
    check.not_equal(recordings[0][1]["value"], recordings[1][1]["value"],
                    msg="fresh run-local keys prevent identical originals from becoming a cross-recording content identifier")
    assert any(projection == "message_json/1" for projection, _ in hints)
    assert {projection for projection, _ in hints} == {"message_json/1", "args_json_python/1", "text_blocks_json/1"}
    check.is_true(all(size is None for projection, size in hints if projection != "message_json/1"),
                  msg="existing argument and text consumers remain unhinted")


def test_missing_payload_is_distinct_from_observed_json_null(tmp_path):
    path = tmp_path / "missing-null.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    try:
        missing = observe(recorder, "host.tool_result")
        null = observe(recorder, "host.tool_result", None)
    finally:
        recorder.close()
    with closing(sqlite3.connect(path)) as db:
        db.row_factory = sqlite3.Row
        missing_row = db.execute("SELECT e.payload_ref,a.body_text,a.omitted_reason FROM events e LEFT JOIN artifacts a ON e.payload_ref=a.artifact_id WHERE e.event_id=?", (missing.event_id,)).fetchone()
        assert missing_row is not None, "the admitted MISSING observation still needs its event envelope"
        null_artifact = payload_artifact(db, null.event_id)
        check.is_none(missing_row["body_text"], msg="MISSING records unexposed content rather than JSON null")
        check.is_true(missing_row["payload_ref"] is None or bool(missing_row["omitted_reason"]),
                      msg="unexposed content has either no payload reference or an explicitly omitted artifact")
        check.equal(null_artifact["body_text"], "null", msg="observed JSON null remains an actual retained original value")
        check.is_none(null_artifact["omitted_reason"], msg="observed JSON null is not a retention or visibility omission")


@pytest.mark.parametrize("maximal", [False, True], ids=["normal-linked-terminal", "maximum-controls-and-escaped-error"])
def test_reserved_terminal_accepts_bounded_metadata_under_ordinary_pressure(tmp_path, monkeypatch, maximal):
    path = tmp_path / "reserved-metadata.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE,
                        limits={"ordinary_items": 4, "ordinary_bytes": 32_768})
    entered, release = threading.Event(), threading.Event()
    original = Recorder._persist_bundle

    def hold_filler(self, connection, bundle):
        if bundle.kind == "host.tool_result" and not entered.is_set():
            entered.set()
            if not release.wait(10):
                raise RuntimeError("test-owned full ordinary pool was not released")
        return original(self, connection, bundle)

    def drained():
        with recorder._condition:
            assert recorder._condition.wait_for(lambda: recorder.health().ordinary.used_slots == 0, timeout=5), \
                "prior admitted setup must commit before establishing the controlled full ordinary pool"

    try:
        with recorder.turn("terminal metadata") as turn:
            accept(turn.receipt)
            source = observe(recorder, "model.request", {"messages": []}, messages=[])
            choice = observe(recorder, "model.tool_call", {"arguments": {}}, name="echo",
                             model_call_id=source.model_call_id, provider_ref="provider")
            observe(recorder, "model.response", {}, model_call_id=source.model_call_id)
            drained()
            with recorder.dispatch(action="execute", name="echo", arguments={},
                                   decision_event_ids=[choice.event_id]) as dispatch:
                accept(dispatch.receipt)
                scope = recorder.capture_scope()
                start = accept(recorder.start_operation(scope=scope, client_id="client",
                    method="tools/call", server_key="server",
                    name="echo", arguments={}, decision_id=choice.decision_id, link_basis="host_hook"),
                    "short fixed linked controls must admit a start with a usable terminal reservation")
            drained()
            monkeypatch.setattr(Recorder, "_persist_bundle", hold_filler)
            for index in range(4):
                observe(recorder, "host.tool_result", {"index": index, "content": "x" * 3000})
                if index == 0:
                    assert entered.wait(5), "writer must hold the first filler before the remaining ordinary slots are occupied"
            before = recorder.health()
            check.equal(before.ordinary.used_slots, 4, msg="queued and writer-held fillers occupy every ordinary slot")
            options = {}
            if maximal:
                options = {"error": {"origin": "\x01" * 64, "type": "\x02" * 64, "code": 2**63 - 1},
                           "retention": {"policy_id": "\x03" * 256, "omitted_reason": "\x04" * 256},
                           "input_rounds": 2**63 - 1}
            terminal = recorder.finish_operation(start.ticket, status="protocol_error" if maximal else "ok",
                result="large optional result" * 10_000, text_paths=[""], **options)
            check.equal((terminal.status, terminal.reason), ("accepted", None),
                        msg="an admitted operation must fit its bounded terminal metadata into its reserved pool while ordinary slots are full")
            after = recorder.health()
            check.equal(after.ordinary.used_slots, before.ordinary.used_slots, msg="reserved completion consumes no ordinary slot")
            check.equal((after.completion.used_slots, after.completion.used_bytes, after.open_operations),
                        (1, 4096, 1), msg="the held terminal retains exactly one full completion reservation until COMMIT")
            check.equal(after.dropped_events, before.dropped_events, msg="accepted terminal omission does not become a dropped observation")
            release.set()
            drained()
    finally:
        release.set()
        recorder.close()
    with closing(sqlite3.connect(path)) as db:
        db.row_factory = sqlite3.Row
        operation = db.execute("SELECT * FROM operations").fetchone()
        assert operation is not None, "the accepted start must retain its operation row after pressure"
        start_event = db.execute("SELECT * FROM events WHERE event_id=?", (start.event_id,)).fetchone()
        terminal_event = db.execute("SELECT * FROM events WHERE event_id=?", (terminal.event_id,)).fetchone()
        assert start_event is not None and terminal_event is not None, \
            "both admitted operation boundaries must persist before comparing their attribution and times"
        expected_attribution = (scope.run_id, turn.receipt.turn_id, source.model_call_id,
                                choice.decision_id, start.operation_id)
        columns = ("run_id", "turn_id", "model_call_id", "decision_id", "operation_id")
        check.equal(tuple(operation[name] for name in columns), expected_attribution,
                    msg="slimming the opaque ticket must preserve every persisted operation attribution field")
        check.equal(tuple(start_event[name] for name in columns), expected_attribution,
                    msg="the start event and operation retain the same public setup identities")
        check.equal(tuple(terminal_event[name] for name in columns), expected_attribution,
                    msg="the terminal uses the frozen start attribution after dispatch scope exits")
        check.equal(operation["dispatch_event_id"], dispatch.receipt.event_id,
                    msg="the persisted operation keeps the exact admitted dispatch witness")
        check.equal(tuple(operation[name] for name in ("client_id", "server_key", "method", "name")),
                    ("client", "server", "tools/call", "echo"), msg="all original start control values remain stored")
        check.equal(operation["link_basis"], "host_hook", msg="persisted explicit host attribution is not removed with unread ticket bytes")
        check.equal(operation["start_event_id"], start.event_id, msg="the operation retains its admitted start receipt")
        check.equal(operation["started_at"], start_event["observed_at"], msg="operation start time equals its observed start event")
        check.equal(operation["status"], "protocol_error" if maximal else "ok", msg="the real terminal atomically replaces running despite ordinary pressure")
        check.equal(operation["end_event_id"], terminal.event_id, msg="operation terminal identity is the admitted completion event")
        check.equal(operation["ended_at"], terminal_event["observed_at"], msg="operation end time equals its observed terminal event")
        check.greater_equal(operation["duration_ms"], 0, msg="the terminal retains a nonnegative measured duration")
        if maximal:
            check.equal((operation["error_code"], operation["input_rounds"]), (2**63 - 1, 2**63 - 1),
                        msg="largest allowed SQLite integer metadata is preserved without overflow or truncation")
            detail = event_details(db, terminal.event_id)
            check.equal(detail.get("error"), options["error"], msg="bounded escaped error origin/type survive exact terminal capture")
    health = recorder.health()
    check.equal((health.completion.used_bytes, health.completion.used_slots, health.open_operations), (0, 0, 0),
                msg="both ordinary and maximum-metadata terminal paths release their reservation after commit/close")


def test_generic_request_paths_preserve_literals_order_and_run_scope(tmp_path):
    recordings = []
    for index in range(2):
        path = tmp_path / f"generic-request-{index}.sqlite"
        recorder = Recorder(path, capture="content", coverage=COVERAGE)
        payload = {"request": {"input": [{"content": "same"}, {"content": "same"}]},
                   "a/b": {"~key": ["second", "first"]}}
        annotations = [{"bindings": [], "unresolved": [], "complete": True} for _ in range(2)]
        try:
            submitted = [request(recorder, payload, message_path="/request/input",
                                 field_paths=["/a~1b/~0key"], messages=annotations)
                         for _ in range(2)]
        finally:
            closed = recorder.close()
        assert closed.writer_stopped and closed.portable, \
            "both real requests and their matching responses must close portably before inspecting generic-path facts"
        health = recorder.health()
        check.equal((health.dropped_events, health.writer_errors), (0, 0),
                    msg="valid generic requests do not lose observations or fail the writer")
        handles = []
        with closing(sqlite3.connect(path)) as db:
            run = db.execute("SELECT run_id FROM runs").fetchone()
            assert run is not None, "each real recording supplies its own stored run identity"
            for receipt in submitted:
                manifest = json.loads(payload_artifact(db, receipt.event_id)["body_text"])
                check.equal((manifest["format"], manifest["message_path"]),
                            ("request_refs/2", "/request/input"), msg="the public path preserves the explicitly selected message array")
                template = db.execute("SELECT body_text FROM artifacts WHERE artifact_id=?", (manifest["template_ref"],)).fetchone()
                assert template is not None, "the structural manifest must resolve its retained template"
                check.equal(json.loads(template[0]), {"request": {"input": None}, "a/b": {"~key": None}},
                            msg="only the literal message and escaped field locations are replaced in the template")
                assert len(manifest["field_refs"]) == 1, "the one declared extracted field must have exactly one ordered reference"
                field = manifest["field_refs"][0]
                check.equal(field["pointer"], "/a~1b/~0key", msg="the stored field pointer retains its exact escape spelling")
                field_body = db.execute("SELECT body_text FROM artifacts WHERE artifact_id=?", (field["artifact_id"],)).fetchone()
                assert field_body is not None, "the escaped field reference must resolve before inspecting its literal array"
                check.equal(json.loads(field_body[0]), ["second", "first"], msg="extracted field arrays retain submitted order")
                members = db.execute("""SELECT m.message_index,m.message_pointer,m.message_artifact_id,a.body_text
                    FROM request_members m JOIN artifacts a ON a.artifact_id=m.message_artifact_id
                    WHERE m.request_event_id=? ORDER BY m.message_index""", (receipt.event_id,)).fetchall()
                assert len(members) == 2, "each public request must retain both duplicate message positions"
                check.equal([(row[0], row[1], json.loads(row[3])) for row in members],
                            [(0, "/request/input/0", {"content": "same"}),
                             (1, "/request/input/1", {"content": "same"})],
                            msg="membership preserves exact positions, generic pointers and literal message bodies")
                check.equal(members[0][2], members[1][2], msg="known duplicate originals share a reusable handle within the request")
                check.equal(manifest["message_refs"], [row[2] for row in members],
                            msg="the structural manifest and ordered membership rows agree without deduplicating positions")
                handles.append(members[0][2])
            check.equal(handles[0], handles[1], msg="the second submission reuses the same known handle in this real run")
            recordings.append((run[0], handles[0]))
    check.not_equal(recordings[0][0], recordings[1][0], msg="two real recordings issue distinct run identities")
    check.not_equal(recordings[0][1], recordings[1][1], msg="known reusable handles remain distinct across actual recording scopes")


def test_operation_reservation_rejects_oversized_metadata_without_publishing_identity(tmp_path):
    path = tmp_path / "oversized-operation.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    try:
        scope = recorder.capture_scope()
        oversized = "\x00" * 256
        rejected = recorder.start_operation(scope=scope, client_id=oversized, method=oversized,
                                            server_key=oversized, name=oversized, arguments={})
        check.equal((rejected.status, rejected.reason), ("rejected", "invalid_input"),
                    msg="individually bounded controls still reject when their escaped terminal envelope exceeds the fixed reservation")
        check.equal(tuple(getattr(rejected, field) for field in
                          ("event_id", "turn_id", "model_call_id", "decision_id", "operation_id", "ticket")),
                    (None,) * 6, msg="an oversized unscoped start must publish neither identities nor a usable ticket")
        health = recorder.health()
        check.equal((health.ordinary.used_slots, health.ordinary.used_bytes,
                     health.completion.used_slots, health.completion.used_bytes, health.open_operations),
                    (0, 0, 0, 0, 0), msg="failed preparation must acquire no ordinary or terminal ownership")
        start = accept(recorder.start_operation(scope=scope, client_id="c", method="m", arguments={}),
                       "a short valid neighbor must remain admissible after reservation-size refusal")
        health = recorder.health()
        check.equal((health.completion.used_slots, health.completion.used_bytes, health.open_operations),
                    (1, 4096, 1), msg="the valid neighbor alone owns exactly one fixed reservation")
        terminal = accept(recorder.finish_operation(start.ticket, status="ok", result={}),
                          "the valid neighbor must complete through its own admitted ticket")
    finally:
        closed = recorder.close()
    assert closed.writer_stopped and closed.portable, "the valid neighboring operation must close before inspecting refused-start isolation"
    health = recorder.health()
    check.equal((health.completion.used_slots, health.completion.used_bytes, health.open_operations),
                (0, 0, 0), msg="valid completion and close release the only reservation exactly once")
    check.equal((health.dropped_events, health.writer_errors), (1, 0),
                msg="oversized metadata is exactly one rejected attempt rather than a writer failure")
    with closing(sqlite3.connect(path)) as db:
        check.equal(db.execute("SELECT operation_id,start_event_id,end_event_id,status FROM operations").fetchall(),
                    [(start.operation_id, start.event_id, terminal.event_id, "ok")],
                    msg="only the valid neighbor may have a persisted operation row")
        check.equal(db.execute("SELECT event_id,kind FROM events ORDER BY sequence").fetchall(),
                    [(start.event_id, "mcp.request"), (terminal.event_id, "mcp.response")],
                    msg="reservation-size rejection must not persist an orphan start or terminal identity")


def test_start_retention_preserves_original_comparisons_and_snapshots(tmp_path):
    path = tmp_path / "start-retention.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    arguments = {"secret": "original-π", "count": 3}
    start_policy = {"policy_id": "request-mask", "retained": {"arguments": {"visible": "masked"}},
                    "redacted": True, "truncated": True}
    result = {"text": "result-original-λ"}
    finish_policy = {"policy_id": "response-mask", "retained": {"text": "masked"},
                     "redacted": True}
    try:
        scope = recorder.capture_scope()
        default = accept(recorder.start_operation(scope=scope, client_id="c", method="tools/call",
                                                   name="tool", arguments=arguments))
        retained = accept(recorder.start_operation(scope=scope, client_id="c", method="tools/call",
            name="tool", arguments=arguments, retention=start_policy))
        arguments.update(secret="changed original", count=99)
        start_policy["retained"]["arguments"]["visible"] = "changed retained"
        start_policy.update(policy_id="changed policy", redacted=False, truncated=False)
        default_end = accept(recorder.finish_operation(default.ticket, status="ok", result=result,
                                                         text_paths=["/text"]))
        retained_end = accept(recorder.finish_operation(retained.ticket, status="ok", result=result,
            text_paths=["/text"], retention=finish_policy))
        result["text"] = "changed result"
        finish_policy["retained"]["text"] = "changed result replacement"
        finish_policy.update(policy_id="changed response policy", redacted=False)
    finally:
        closed = recorder.close()
    assert closed.writer_stopped and closed.portable, "joined close precedes durable retained-content checks"
    check.equal(closed.run_close_state, "clean", msg="declared retention preserves complete capture")
    check.not_equal(default.operation_id, retained.operation_id, msg="equal originals remain distinct operations")
    original_request = '{"arguments":{"secret":"original-π","count":3}}'.encode("utf-8")
    original_result = '{"text":"result-original-λ"}'.encode("utf-8")
    expected = [
        (default, original_request, original_request, "content", 0, 0),
        (retained, b'{"arguments":{"visible":"masked"}}', original_request, "request-mask", 1, 1),
        (default_end, original_result, original_result, "content", 0, 0),
        (retained_end, b'{"text":"masked"}', original_result, "response-mask", 1, 0),
    ]
    with closing(sqlite3.connect(path)) as db:
        for receipt, body, original, policy, redacted, truncated in expected:
            artifact = payload_artifact(db, receipt.event_id)
            check.equal((artifact["body_text"], artifact["stored_sha256"], artifact["stored_bytes"],
                         artifact["observed_bytes"], artifact["policy_id"], artifact["redacted"],
                         artifact["truncated"], artifact["omitted_reason"]),
                        (body.decode("utf-8"), hashlib.sha256(body).hexdigest(), len(body), len(original),
                         policy, redacted, truncated, None),
                        msg="literal stored bytes and policy survive mutation; observed length describes the original")
        for receipts, key, projection in [
            ((default, retained), "args_digest", "args_json_python/1"),
            ((default_end, retained_end), "result_text_digest", "text_blocks_json/1"),
        ]:
            tokens = [event_details(db, receipt.event_id)[key] for receipt in receipts]
            assert all(token.get("value") for token in tokens), "available original comparisons cannot pass as two missing tokens"
            check.equal(tokens[0], tokens[1], msg="equal original projections compare within the same run despite different retained bodies")
            check.equal((tokens[0].get("scope"), tokens[0].get("basis"), tokens[0].get("reason")),
                        (scope.run_id, "observed_original", None), msg="tokens retain original provenance and availability")
            check.equal(tokens[0].get("projection"), projection, msg="comparison retains its declared projection")
        check.equal([tuple(row) for row in db.execute("SELECT event_id,kind,operation_id FROM events ORDER BY sequence")],
            [(default.event_id, "mcp.request", default.operation_id),
             (retained.event_id, "mcp.request", retained.operation_id),
             (default_end.event_id, "mcp.response", default.operation_id),
             (retained_end.event_id, "mcp.response", retained.operation_id)],
            msg="each accepted operation has exactly its own request and terminal")
        check.equal({tuple(row) for row in db.execute("SELECT operation_id,start_event_id,end_event_id,status FROM operations")},
            {(default.operation_id, default.event_id, default_end.event_id, "ok"),
             (retained.operation_id, retained.event_id, retained_end.event_id, "ok")},
            msg="retention preserves operation identity and completion")
    health = recorder.health()
    check.equal((health.open_operations, health.completion.used_slots, health.completion.used_bytes,
                 health.dropped_events, health.writer_errors), (0, 0, 0, 0, 0),
                msg="both operations release all ownership without an observation loss")


def test_start_retention_null_omission_and_invalid_neighbor(tmp_path):
    path = tmp_path / "start-retention-values.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    try:
        scope = recorder.capture_scope()
        invalid = recorder.start_operation(scope=scope, client_id="c", method="tools/call",
            arguments={"value": "original"}, retention={"policy_id": "invalid", "retained": None, "unknown": True})
        check.equal((invalid.status, invalid.reason, invalid.event_id, invalid.operation_id, invalid.ticket),
                    ("rejected", "invalid_input", None, None, None), msg="malformed direct retention publishes no usable start")
        health = recorder.health()
        check.equal((health.ordinary.used_slots, health.ordinary.used_bytes, health.open_operations,
                     health.completion.used_slots, health.completion.used_bytes), (0, 0, 0, 0, 0),
                    msg="invalid direct-core retention consumes no admission or completion reservation")
        null = accept(recorder.start_operation(scope=scope, client_id="c", method="tools/call",
            arguments={"value": "original"}, retention={"policy_id": "null", "retained": None}))
        null_end = accept(recorder.finish_operation(null.ticket, status="ok", result={"text": "done"}, text_paths=["/text"]))
        omitted = accept(recorder.start_operation(scope=scope, client_id="c", method="tools/call",
            arguments={"value": "original"}, retention={"policy_id": "omit", "omitted_reason": "application_choice", "redacted": True}))
        omitted_end = accept(recorder.finish_operation(omitted.ticket, status="ok", result={"text": "done"}, text_paths=["/text"]))
    finally:
        closed = recorder.close()
    assert closed.writer_stopped and closed.portable, "valid neighbors commit before checking rejection isolation"
    check.not_equal(null.operation_id, omitted.operation_id, msg="null and omission are distinct recording occurrences")
    with closing(sqlite3.connect(path)) as db:
        null_body = payload_artifact(db, null.event_id)
        omitted_body = payload_artifact(db, omitted.event_id)
        check.equal(tuple(null_body[key] for key in ("body_text", "stored_bytes", "stored_sha256", "policy_id", "omitted_reason", "redacted", "truncated")),
                    ("null", 4, hashlib.sha256(b"null").hexdigest(), "null", None, 0, 0),
                    msg="explicit retained null is real stored JSON with its own bytes and digest")
        check.equal(tuple(omitted_body[key] for key in ("body_text", "stored_bytes", "stored_sha256", "policy_id", "omitted_reason", "redacted", "truncated")),
                    (None, 0, None, "omit", "application_choice", 1, 0),
                    msg="omission has no stored body or digest and retains the declared reason")
        check.equal([null_body["observed_bytes"], omitted_body["observed_bytes"]],
                    [len(b'{"arguments":{"value":"original"}}')] * 2,
                    msg="both policies retain the original request-envelope length")
        tokens = [event_details(db, item.event_id)["args_digest"] for item in (null, omitted)]
        assert all(token.get("value") for token in tokens), "both valid original argument projections remain comparable"
        check.equal(tokens[0], tokens[1], msg="null and omitted bodies do not change original comparison tokens")
        check.equal([tuple(row) for row in db.execute("SELECT event_id,kind FROM events ORDER BY sequence")],
                    [(null.event_id, "mcp.request"), (null_end.event_id, "mcp.response"),
                     (omitted.event_id, "mcp.request"), (omitted_end.event_id, "mcp.response")],
                    msg="the invalid direct start creates no request row or synthetic terminal")
        check.equal({tuple(row) for row in db.execute("SELECT operation_id,start_event_id,end_event_id,status FROM operations")},
                    {(null.operation_id, null.event_id, null_end.event_id, "ok"),
                     (omitted.operation_id, omitted.event_id, omitted_end.event_id, "ok")},
                    msg="both valid neighbors finish once through their own tickets")
    health = recorder.health()
    check.equal((health.open_operations, health.completion.used_slots, health.completion.used_bytes,
                 health.dropped_events, health.writer_errors), (0, 0, 0, 1, 0),
                msg="one rejected direct mapping leaves no open operation or reserved capacity")


def test_terminal_retention_freezes_values_and_policy_at_submission(tmp_path, monkeypatch):
    path = tmp_path / "terminal-retention-freeze.sqlite"
    entered, release = threading.Event(), threading.Event()
    original_persist = Recorder._persist_bundle

    def hold_terminal(self, connection, bundle):
        if bundle.kind == "mcp.response" and not entered.is_set():
            entered.set()
            if not release.wait(10):
                raise RuntimeError("test-owned terminal persistence barrier was not released")
        return original_persist(self, connection, bundle)

    monkeypatch.setattr(Recorder, "_persist_bundle", hold_terminal)
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    result = {"visible": ["original"]}
    retention = {"policy_id": "frozen", "retained": {"visible": ["masked"]},
                 "redacted": True, "truncated": True}
    try:
        start = accept(recorder.start_operation(scope=recorder.capture_scope(), client_id="c", method="m", arguments={}))
        terminal = accept(recorder.finish_operation(start.ticket, status="ok", result=result, retention=retention))
        assert entered.wait(5), "the accepted terminal must be held before persistence so post-submission mutation is observable"
        result["visible"][0] = "changed original"
        retention["retained"]["visible"][0] = "changed retained"
        retention.update(policy_id="changed", redacted=False, truncated=False)
    finally:
        release.set()
        closed = recorder.close()
    assert closed.writer_stopped and closed.portable, "the released terminal must persist and finalize before checking detached retention"
    with closing(sqlite3.connect(path)) as db:
        artifact = payload_artifact(db, terminal.event_id)
        retained_bytes = b'{"visible":["masked"]}'
        check.equal(artifact["body_text"], retained_bytes.decode(), msg="nested replacement content freezes at public terminal submission")
        check.equal((artifact["policy_id"], artifact["redacted"], artifact["truncated"], artifact["omitted_reason"]),
                    ("frozen", 1, 1, None), msg="policy and independent loss flags freeze alongside the replacement value")
        check.equal((artifact["observed_bytes"], artifact["stored_bytes"]), (24, 22),
                    msg="observed and retained lengths describe the two literal values before caller mutation")
        check.equal(artifact["stored_sha256"], hashlib.sha256(retained_bytes).hexdigest(),
                    msg="stored SHA authenticates the literal frozen retained bytes")
        check.equal([tuple(row) for row in db.execute("SELECT operation_id,status,end_event_id FROM operations")],
                    [(start.operation_id, "ok", terminal.event_id)], msg="detached retention still completes the real admitted operation")
        check.equal(db.execute("SELECT state FROM events WHERE event_id=?", (terminal.event_id,)).fetchone()[0],
                    "ok", msg="the actual terminal event is persisted with its accepted state")
    health = recorder.health()
    check.equal((health.dropped_events, health.writer_errors), (0, 0), msg="valid retained-value mutation isolation does not lose capture")
    check.equal((health.completion.used_slots, health.completion.used_bytes, health.open_operations),
                (0, 0, 0), msg="the held terminal releases its reservation after persistence and close")


@pytest.mark.parametrize("retained", [{1: "coerced"}, ("tuple",)], ids=["nonstring-key", "tuple"])
def test_invalid_terminal_retention_keeps_ticket_available_for_retry(tmp_path, retained):
    path = tmp_path / "invalid-terminal-retention.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    try:
        start = accept(recorder.start_operation(scope=recorder.capture_scope(), client_id="c", method="m", arguments={}))
        invalid = recorder.finish_operation(start.ticket, status="ok", result={"visible": "original"},
                                            retention={"policy_id": "invalid", "retained": retained})
        check.equal((invalid.status, invalid.reason), ("rejected", "invalid_input"),
                    msg="invalid retained JSON must be rejected rather than coerced into a valid replacement")
        check.is_none(invalid.event_id, msg="the invalid terminal must not publish an event identity")
        health = recorder.health()
        check.equal((health.completion.used_slots, health.completion.used_bytes, health.open_operations),
                    (1, 4096, 1), msg="retention validation failure leaves the admitted ticket's full reservation available")
        with closing(sqlite3.connect(path)) as db:
            check.equal(db.execute("SELECT COUNT(*) FROM events WHERE kind='mcp.response'").fetchone()[0], 0,
                        msg="the invalid override must publish no terminal before the valid retry")
        terminal = accept(recorder.finish_operation(start.ticket, status="ok", result={"visible": "original"},
            retention={"policy_id": "valid", "retained": {"visible": "masked"}, "redacted": True}),
            "the same ticket must accept a valid retained replacement after its invalid attempt")
    finally:
        closed = recorder.close()
    assert closed.writer_stopped and closed.portable, "the valid retry must commit before checking single-terminal persistence"
    with closing(sqlite3.connect(path)) as db:
        check.equal(db.execute("SELECT event_id FROM events WHERE kind='mcp.response'").fetchall(),
                    [(terminal.event_id,)], msg="exactly the valid retry may persist a terminal event")
        artifact = payload_artifact(db, terminal.event_id)
        check.equal((artifact["body_text"], artifact["policy_id"], artifact["redacted"], artifact["omitted_reason"]),
                    ('{"visible":"masked"}', "valid", 1, None), msg="the successful retry stores its own valid replacement and policy")
        check.equal([tuple(row) for row in db.execute("SELECT operation_id,status,end_event_id FROM operations")],
                    [(start.operation_id, "ok", terminal.event_id)], msg="the retry atomically completes the original operation once")
    health = recorder.health()
    check.equal((health.completion.used_slots, health.completion.used_bytes, health.open_operations),
                (0, 0, 0), msg="the valid retry releases every reservation and open-operation owner")
    check.equal((health.dropped_events, health.writer_errors), (1, 0),
                msg="one invalid retained tree records one drop without a writer failure")


def test_request_paths_preserve_nested_escaped_fields_and_order(tmp_path):
    path = tmp_path / "nested-request-paths.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    payload = {"request": {"input": [{"content": "same"}, {"content": "same"}]},
               "a/b": {"~key": {"flag": True}}, "tools": [{"name": "echo"}], "marker": "keep"}
    root_list = [
        {"body": {"mes/sages": [{"content": "same"}, {"content": "same"}],
                  "to~ols": [{"name": "echo"}], "keep": {"nested": ["before"]}}},
        {"config": {"flag": True}, "untouched": [1, True, 1.0]},
    ]
    root_list_bytes = (b'[{"body":{"mes/sages":[{"content":"same"},{"content":"same"}],'
        b'"to~ols":[{"name":"echo"}],"keep":{"nested":["before"]}}},'
        b'{"config":{"flag":true},"untouched":[1,true,1.0]}]')
    try:
        receipt = request(recorder, payload, message_path="/request/input",
                          field_paths=["/tools", "/a~1b/~0key"], catalog_path="/tools",
                          messages=[{"bindings": [], "complete": True} for _ in range(2)])
        list_receipt = request(recorder, root_list, message_path="/0/body/mes~1sages",
            field_paths=["/0/body/to~0ols", "/1/config"], catalog_path="/0/body/to~0ols",
            messages=[{"bindings": [], "unresolved": [], "complete": True} for _ in range(2)])
        assert not receipt.losses and not list_receipt.losses, "Both path cases require full retained content."
        check.equal(json.dumps(root_list, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode(),
                    root_list_bytes, msg="public root-list capture must leave the submitted caller tree unchanged")
    finally:
        closed = recorder.close()
    assert closed.writer_stopped and closed.portable, "the accepted nested request must close before inspecting its ordered structural facts"
    check.equal(closed.run_close_state, "clean", msg="both dict-root and list-root requests must finish with complete retained evidence")
    with closing(sqlite3.connect(path)) as db:
        manifest = json.loads(payload_artifact(db, receipt.event_id)["body_text"])
        check.equal((manifest["format"], manifest["message_path"]), ("request_refs/2", "/request/input"),
                    msg="generic nested message extraction retains its format and exact path")
        check.equal([field["pointer"] for field in manifest["field_refs"]], ["/tools", "/a~1b/~0key"],
                    msg="path validation must preserve the caller's field order rather than sorting the stored manifest")
        fields = []
        for field in manifest["field_refs"]:
            row = db.execute("SELECT body_text FROM artifacts WHERE artifact_id=?", (field["artifact_id"],)).fetchone()
            assert row is not None, "every declared extracted field must resolve to its retained artifact"
            fields.append(json.loads(row[0]))
        check.equal(fields, [[{"name": "echo"}], {"flag": True}], msg="escaped tokens select literal keys without altering their values")
        template = db.execute("SELECT body_text FROM artifacts WHERE artifact_id=?", (manifest["template_ref"],)).fetchone()
        assert template is not None, "the manifest must retain its structural template"
        check.equal(json.loads(template[0]), {"request": {"input": None}, "a/b": {"~key": None},
                                            "tools": None, "marker": "keep"},
                    msg="only selected locations become null and unrelated literal fields remain intact")
        members = db.execute("""SELECT message_index,message_pointer,message_artifact_id FROM request_members
            WHERE request_event_id=? ORDER BY message_index""", (receipt.event_id,)).fetchall()
        assert len(members) == 2, "nested extraction must retain both duplicate message positions"
        check.equal([(row[0], row[1]) for row in members], [(0, "/request/input/0"), (1, "/request/input/1")],
                    msg="both ordered memberships retain their literal nested pointers")
        check.equal(members[0][2], members[1][2], msg="duplicate known messages reuse a handle without collapsing either occurrence")
        check.equal(manifest["message_refs"], [row[2] for row in members], msg="ordered manifest references match persisted membership")
        catalog = db.execute("SELECT catalog_ref FROM events WHERE event_id=?", (receipt.event_id,)).fetchone()
        assert catalog is not None and manifest["field_refs"], "the admitted request and its catalog field must exist"
        check.equal(catalog[0], manifest["field_refs"][0]["artifact_id"], msg="catalog_ref resolves to the explicitly declared tools field")
        list_manifest = json.loads(payload_artifact(db, list_receipt.event_id)["body_text"])
        check.equal((list_manifest["format"], list_manifest["message_path"]),
                    ("request_refs/2", "/0/body/mes~1sages"), msg="list-root extraction preserves its exact escaped message path")
        check.equal([field["pointer"] for field in list_manifest["field_refs"]],
                    ["/0/body/to~0ols", "/1/config"], msg="shared-ancestor and sibling-list fields keep the declared order")
        assert len(list_manifest["field_refs"]) == 2, "Both list-root extracted fields must retain their ordered references."
        list_template = db.execute("SELECT * FROM artifacts WHERE artifact_id=?", (list_manifest["template_ref"],)).fetchone()
        assert list_template is not None, "The root-list request must have its durable template."
        template_bytes = (b'[{"body":{"mes/sages":null,"to~ols":null,"keep":{"nested":["before"]}}},'
                          b'{"config":null,"untouched":[1,true,1.0]}]')
        list_artifacts = [(list_template, template_bytes)]
        for field, expected in zip(list_manifest["field_refs"], [b'[{"name":"echo"}]', b'{"flag":true}']):
            row = db.execute("SELECT * FROM artifacts WHERE artifact_id=?", (field["artifact_id"],)).fetchone()
            assert row is not None, "Each escaped list-root field must resolve to its own retained value."
            list_artifacts.append((row, expected))
        list_members = db.execute("""SELECT m.message_index,m.message_pointer,m.message_artifact_id,a.*
            FROM request_members m JOIN artifacts a ON a.artifact_id=m.message_artifact_id
            WHERE m.request_event_id=? ORDER BY m.message_index""", (list_receipt.event_id,)).fetchall()
        assert len(list_members) == 2, "Equal list-root messages must retain both original occurrences."
        check.equal([(row["message_index"], row["message_pointer"]) for row in list_members],
                    [(0, "/0/body/mes~1sages/0"), (1, "/0/body/mes~1sages/1")],
                    msg="list membership preserves exact indices and literal escaped pointers")
        check.equal(list_members[0]["message_artifact_id"], list_members[1]["message_artifact_id"],
                    msg="equal originals with equal complete bindings share one handle without losing either occurrence")
        check.equal(list_manifest["message_refs"], [row["message_artifact_id"] for row in list_members],
                    msg="the list-root manifest keeps both ordered references to the shared message")
        list_artifacts.extend((row, b'{"content":"same"}') for row in list_members)
        for row, expected in list_artifacts:
            check.equal(row["body_text"], expected.decode(),
                        msg="only selected locations become null; exact bytes preserve siblings, key order and scalar types")
            check.equal((row["stored_sha256"], row["stored_bytes"], row["observed_bytes"]),
                        (hashlib.sha256(expected).hexdigest(), len(expected), len(expected)),
                        msg="list-root retained bytes, observed lengths and stored hashes describe the literal expected parts")
            check.equal((row["policy_id"], row["redacted"], row["truncated"], row["omitted_reason"]),
                        ("content", 0, 0, None), msg="valid list-root extraction retains content without invented loss flags")
        check.equal([type(value) for value in json.loads(list_template["body_text"])[1]["untouched"]],
                    [int, bool, float], msg="unextracted list siblings retain distinct integer, boolean and float types")
        check.equal(db.execute("SELECT catalog_ref FROM events WHERE event_id=?", (list_receipt.event_id,)).fetchone()[0],
                    list_manifest["field_refs"][0]["artifact_id"], msg="the list-root catalog points at the declared escaped field")
    check.equal((recorder.health().dropped_events, recorder.health().writer_errors), (0, 0),
                msg="the valid nested and escaped paths require no observation loss or writer failure")


@pytest.mark.parametrize("message_path,field_paths", [
    ("/messages", ["/a", "/a"]),
    ("/messages", ["/a", "/a/b"]),
    ("/messages", ["/a/b", "/a"]),
    ("/messages", ["/a", "/a!", "/a/b"]),
    ("/messages", ["/a/b", "/a!", "/a"]),
    ("/messages", ["/messages"]),
    (None, ["/a", "/a/b"]),
    (None, ["/a/b", "/a"]),
], ids=["duplicate", "ancestor-first", "descendant-first", "intervening-ancestor-first",
        "intervening-descendant-first", "message-overlap", "direct-ancestor-first", "direct-descendant-first"])
def test_request_path_overlap_rejects_once_without_identity(tmp_path, message_path, field_paths):
    path = tmp_path / "overlapping-request-paths.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    try:
        receipt = recorder.observe("model.request", scope=recorder.capture_scope(),
            payload={"messages": [], "a": {"b": 1}, "a!": 2},
            facts={"message_path": message_path, "field_paths": field_paths, "messages": []})
    finally:
        closed = recorder.close()
    assert closed.writer_stopped and closed.portable, "path-refusal evidence must finalize before counting persisted requests"
    check.equal((receipt.status, receipt.reason), ("rejected", "invalid_input"),
                msg="duplicate or ancestor paths reject regardless of declaration order and lexical interleaving")
    check.equal((receipt.event_id, receipt.model_call_id), (None, None), msg="overlapping extraction publishes no request identities")
    with closing(sqlite3.connect(path)) as db:
        check.equal(db.execute("SELECT COUNT(*) FROM events WHERE kind='model.request'").fetchone()[0], 0,
                    msg="invalid path declarations must not create a persisted request row")
    check.equal((recorder.health().dropped_events, recorder.health().writer_errors), (1, 0),
                msg="each invalid extraction is exactly one dropped attempt and no writer failure")


@pytest.mark.parametrize("originals,replacements,body_texts,distinct_handles", [
    ([{"a": 1, "b": 2}, {"b": 2, "a": 1}], [{"b": 2, "a": 1}, {"a": 1, "b": 2}],
     ['{"b":2,"a":1}', '{"a":1,"b":2}'], None),
    ([{"a": 1}, {"a": 2}], [{"a": 2}, {"a": 1}], None, None),
    ([True, 1], [1, True], None, None),
    ([1, 1.0], [1.0, 1], None, None),
    ([[1, 2], [2, 1]], [[2, 1], [1, 2]], None, None),
    ([{"a": 1}, {"a": 2}], [{"a": "redacted"}, {"a": "redacted"}],
     ['{"a":"redacted"}', '{"a":"redacted"}'], 2),
    ([{"a": 1}, {"a": 1}], [{"a": 1}, {"a": 1}], ['{"a":1}', '{"a":1}'], 1),
    ([True, 1, 1.0], [True, 1, 1.0], ['true', '1', '1.0'], 3),
], ids=["key-order", "distinct-swap", "bool-int", "int-float", "array-order", "redaction", "duplicates", "unchanged-types"])
def test_request_retention_preserves_canonical_value_order(tmp_path, originals, replacements, body_texts, distinct_handles):
    path = tmp_path / "canonical-request-retention.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    try:
        receipt = recorder.observe("model.request", scope=recorder.capture_scope(),
            payload={"messages": originals}, facts={
                "messages": [{"bindings": [], "complete": True} for _ in originals],
                "retention": {"policy_id": "matrix", "retained": {"messages": replacements}, "redacted": True}})
        if body_texts is not None:
            accept(receipt, "the literal retention matrix requires this canonical-equivalent or novel-redaction replacement to remain valid")
            observe(recorder, "model.response", {}, model_call_id=receipt.model_call_id)
    finally:
        closed = recorder.close()
    assert closed.writer_stopped and closed.portable, "each retention case must finalize before inspecting accepted or refused membership"
    with closing(sqlite3.connect(path)) as db:
        if body_texts is None:
            check.equal((receipt.status, receipt.reason), ("rejected", "invalid_input"),
                        msg="retention must not move a different original value into another submitted position")
            check.equal((receipt.event_id, receipt.model_call_id), (None, None), msg="refused reordering publishes no request identity")
            check.equal(db.execute("SELECT COUNT(*) FROM events WHERE kind='model.request'").fetchone()[0], 0,
                        msg="a typed or array-order collision cannot persist as an admitted request")
        else:
            manifest = json.loads(payload_artifact(db, receipt.event_id)["body_text"])
            members = db.execute("""SELECT m.message_index,m.message_artifact_id,a.body_text FROM request_members m
                JOIN artifacts a ON a.artifact_id=m.message_artifact_id
                WHERE m.request_event_id=? ORDER BY m.message_index""", (receipt.event_id,)).fetchall()
            assert len(members) == len(originals), "accepted retention must retain every literal original position"
            check.equal([row[0] for row in members], list(range(len(originals))), msg="accepted membership remains in submitted order")
            check.equal([row[2] for row in members], body_texts,
                        msg="canonical validation must not sort retained JSON keys or coerce boolean/integer/float bodies")
            check.equal(manifest["message_refs"], [row[1] for row in members], msg="structural references preserve ordered duplicate membership")
            if distinct_handles is not None:
                check.equal(len({row[1] for row in members}), distinct_handles,
                            msg="original identity distinguishes typed/redacted originals while exact duplicate originals reuse their known handle")
    check.equal((recorder.health().dropped_events, recorder.health().writer_errors),
                (int(body_texts is None), 0), msg="each refused replacement drops once; each accepted replacement and matching response remain loss-free")


@pytest.mark.parametrize("with_parsed,expected_reason", [
    (False, "unsupported_value"),
    (True, "invalid_raw_arguments"),
], ids=["raw-only", "raw-with-parsed"])
def test_deep_raw_arguments_preserve_unavailable_comparison(
        tmp_path, with_parsed, expected_reason):
    # Isolate the recursion boundary from pytest/plugin stack depth. This uses
    # the installed public API, with no source-path injection or private helpers.
    script = r'''
from contextlib import closing
import json
import sqlite3
import sys
from mcp_flightrecorder import Recorder

sys.setrecursionlimit(1000)
path, with_parsed = sys.argv[1], json.loads(sys.argv[2])
payload = {"arguments_raw": "[" * 500 + "0" + "]" * 500}
if with_parsed:
    payload["arguments"] = 0
recorder = Recorder(path, capture="content", coverage={
    "producers": {"model": "test.model", "host": "test.host", "mcp": "test.mcp"},
    "model_complete": True,
})
try:
    with recorder.turn("deep raw argument comparison"):
        scope = recorder.capture_scope()
        request = recorder.observe("model.request", scope=scope,
            payload={"messages": []}, facts={"messages": []})
        assert request.status == "accepted", request
        decision = recorder.observe("model.tool_call", scope=scope, payload=payload,
            facts={"name": "tool", "model_call_id": request.model_call_id})
        assert decision.status == "accepted", decision
        response = recorder.observe("model.response", scope=scope, payload={},
            facts={"model_call_id": request.model_call_id})
        assert response.status == "accepted", response
finally:
    closed = recorder.close()
assert closed.writer_stopped and closed.portable, closed
with closing(sqlite3.connect(path)) as db:
    row = db.execute("SELECT a.body_text, e.details_json FROM events e "
        "JOIN artifacts a ON a.artifact_id=e.payload_ref WHERE e.event_id=?",
        (decision.event_id,)).fetchone()
assert row is not None, "the accepted decision must remain represented"
assert json.loads(row[0]) == payload, "comparison limits must not change retained input"
print(json.dumps(json.loads(row[1])["args_digest"]))
'''
    completed = subprocess.run(
        [sys.executable, "-I", "-B", "-c", script,
         str(tmp_path / "deep-arguments.sqlite"), json.dumps(with_parsed)],
        text=True, capture_output=True, timeout=15, check=True,
    )
    token = json.loads(completed.stdout)
    assert token["value"] is None, "deep raw arguments keep an unavailable comparison"
    assert token["reason"] == expected_reason


@pytest.mark.parametrize("characters,character,hint_override,one_shot", [
    (1_048_576 // 6 - 15, "x", None, True),
    (1_048_576 // 6 - 14, "x", None, True),
    (1_048_576 // 6 - 13, "x", None, False),
    (1_048_576 - 14, "x", None, False),
    (1_048_576 - 13, "x", None, False),
    (200_000, "\x7f", None, False),
    (1_048_576 - 14, "x", 0, True),
    (1_048_576 - 13, "x", 0, True),
    (4, "x", "absent", False),
], ids=["eligible-below", "eligible-at", "eligible-above", "fallback-at-cap",
        "fallback-over-cap", "del-sixfold", "one-shot-at-cap", "one-shot-over-cap", "unhinted"])
def test_message_comparison_encoding_bounds(monkeypatch, characters, character, hint_override, one_shot):
    value = {"content": character * characters}
    original = b'{"content":"' + character.encode("utf-8") * characters + b'"}'
    canonical = b'{"content":"' + (b'\\u007f' if character == "\x7f" else b'x') * characters + b'"}'
    key = b"test-owned-message-comparison-key"
    calls = []
    iterencode = json.JSONEncoder.iterencode

    def traced(encoder, item, _one_shot=False):
        calls.append(_one_shot)
        return iterencode(encoder, item, _one_shot)

    monkeypatch.setattr(json.JSONEncoder, "iterencode", traced)
    options = {} if hint_override == "absent" else {
        "utf8_size": len(original) if hint_override is None else hint_override}
    token = comparison_values.original_digest(value, key=key, run_id="bounded-run",
        projection="message_json/1", complete=True, enabled=True, **options)
    expected = {"value": None, "scope": "bounded-run", "projection": "message_json/1",
                "basis": "observed_original"}
    if len(canonical) > 1_048_576:
        expected["reason"] = "capture_limit"
    else:
        expected["value"] = hmac.new(key,
            b"mcp-flightrecorder\0observed_original\0message_json/1\0" + canonical,
            hashlib.sha256).hexdigest()
    assert token == expected, "both encoding routes retain the independent canonical HMAC or exact cap reason"
    assert calls == [one_shot], "sixfold original-size eligibility selects one-shot; absent/large hints keep streaming"
    # Deliberately undersized private hints distinguish the actual encoded cap
    # guard; truthful eligible originals cannot reach this defensive branch.


@pytest.mark.parametrize("mode", ["content", "metadata", "replacement", "omission"])
def test_message_preparation_uses_original_size_and_retained_fields(tmp_path, monkeypatch, mode):
    from mcp_flightrecorder import _artifacts

    path = tmp_path / "message-ownership.sqlite"
    originals = [{"content": "small"}, {"content": "small"},
                 {"content": "x" * (1_048_576 // 6)}, {"content": "\x7f" * 200_000}]
    original_bytes = [b'{"content":"small"}', b'{"content":"small"}',
                      b'{"content":"' + b'x' * (1_048_576 // 6) + b'"}',
                      b'{"content":"' + b'\x7f' * 200_000 + b'"}']
    replacement_bytes = [b'{"masked":"' + b'y' * 200_000 + b'"}'] * 2 + [b'{"masked":"tiny"}'] * 2
    facts = {"messages": [{"bindings": [], "unresolved": [], "complete": True} for _ in originals]}
    policy, redacted, truncated, omitted = "content", 0, 0, None
    stored = original_bytes
    if mode == "replacement":
        facts["retention"] = {"policy_id": "replace", "redacted": True, "truncated": True,
            "retained": {"messages": [json.loads(body) for body in replacement_bytes]}}
        policy, redacted, truncated, stored = "replace", 1, 1, replacement_bytes
    elif mode == "omission":
        facts["retention"] = {"policy_id": "omit", "omitted_reason": "application_choice", "redacted": True}
        policy, redacted, omitted, stored = "omit", 1, "application_choice", [None] * len(originals)
    elif mode == "metadata":
        policy, omitted, stored = "metadata", "capture_metadata", [None] * len(originals)
    recorder = Recorder(path, capture="metadata" if mode == "metadata" else "content", coverage=COVERAGE)
    hints, message_fields = [], []
    encoded_digest, fields = comparison_values._encoded_digest, _artifacts._fields

    def traced_digest(value, key, projection, **options):
        assert len(message_fields) == len(hints) + 1, "field preparation precedes each message comparison"
        assert message_fields[-1]["observed_bytes"] == options.get("utf8_size")
        hints.append((projection, options.get("utf8_size")))
        return encoded_digest(value, key, projection, **options)

    def traced_fields(run_id, original, policy, representation):
        result = fields(run_id, original, policy, representation)
        if type(original) is dict and set(original) == {"content"}:
            message_fields.append(result)
        return result

    monkeypatch.setattr(comparison_values, "_encoded_digest", traced_digest)
    monkeypatch.setattr(_artifacts, "_fields", traced_fields)
    try:
        receipts = [request(recorder, {"messages": originals}, **facts) for _ in range(2)]
    finally:
        closed = recorder.close()
    assert closed.writer_stopped and closed.portable and closed.run_close_state == "clean"
    assert receipts[0].event_id != receipts[1].event_id and receipts[0].model_call_id != receipts[1].model_call_id
    sizes = [len(body) for body in original_bytes]
    assert hints == [("message_json/1", size) for size in sizes] * 2, \
        "tiny retained/large original and large retained/small original must use original-size eligibility"
    assert [row["observed_bytes"] for row in message_fields] == sizes * 2, \
        "each message prepares its fields once, before passing that same original size to comparison"
    handles = []
    with closing(sqlite3.connect(path)) as db:
        db.row_factory = sqlite3.Row
        for receipt in receipts:
            rows = db.execute("""SELECT m.message_index,m.message_pointer,m.message_artifact_id,a.*
                FROM request_members m JOIN artifacts a ON a.artifact_id=m.message_artifact_id
                WHERE m.request_event_id=? ORDER BY m.message_index""", (receipt.event_id,)).fetchall()
            assert len(rows) == len(originals)
            check.equal([(row["message_index"], row["message_pointer"]) for row in rows],
                        [(i, f"/messages/{i}") for i in range(len(originals))],
                        msg="every original position remains ordered SQL membership")
            manifest = json.loads(payload_artifact(db, receipt.event_id)["body_text"])
            check.equal(manifest["message_refs"], [row["message_artifact_id"] for row in rows],
                        msg="the structural manifest preserves the full membership, including duplicates")
            for index, row in enumerate(rows):
                body = stored[index]
                check.equal((row["body_text"], row["stored_bytes"], row["stored_sha256"], row["observed_bytes"]),
                            (None if body is None else body.decode("utf-8"), 0 if body is None else len(body),
                             None if body is None else hashlib.sha256(body).hexdigest(), sizes[index]),
                            msg="retained bodies/hash/length and original observed length keep distinct authority")
                check.equal((row["policy_id"], row["redacted"], row["truncated"], row["omitted_reason"]),
                            (policy, redacted, truncated, omitted), msg="the reused fields preserve exact policy flags")
            check.equal(event_details(db, receipt.event_id)["message_identity_unavailable"],
                        [{"message_index": 3, "reason": "capture_limit"}],
                        msg="200,000 original DELs stay unavailable even when retained content is tiny or omitted")
            handles.append([row["message_artifact_id"] for row in rows])
        check.equal(handles[0][0], handles[0][1], msg="available equal originals and equal retained fields may share a handle")
        check.equal(handles[0][:3], handles[1][:3], msg="available identities may recur across separate request events")
        check.not_equal(handles[0][3], handles[1][3], msg="unavailable original comparisons keep occurrence identities")
        check.equal([tuple(row) for row in db.execute("SELECT event_id,kind FROM events ORDER BY sequence") if row["kind"] == "model.request"],
                    [(receipt.event_id, "model.request") for receipt in receipts],
                    msg="sharing message handles never collapses the two admitted request occurrences")
        check.equal(db.execute("SELECT COUNT(*) FROM events WHERE kind='model.response' AND state='complete'").fetchone()[0], 2,
                    msg="both original requests keep their complete terminal observations")
    check.equal((recorder.health().dropped_events, recorder.health().writer_errors), (0, 0))


def test_message_hint_preserves_availability_and_comparison_refreeze(monkeypatch):
    options = {"key": b"comparison-key", "run_id": "availability-run", "utf8_size": 0}

    def forbidden(*args, **kwargs):
        raise AssertionError("unavailable projections must not freeze or encode")

    with monkeypatch.context() as guard:
        guard.setattr(comparison_values, "freeze", forbidden)
        guard.setattr(comparison_values, "_encoded_digest", forbidden)
        for value, complete, enabled, projection, reason in (
            (MISSING, False, False, "message_json/1", "disabled"),
            (MISSING, False, True, "message_json/1", "original_incomplete"),
            (MISSING, True, True, "message_json/1", "unexposed"),
            (MISSING, True, True, "text_blocks_json/1", "unexposed"),
            ([7], True, True, "text_blocks_json/1", "unsupported_projection"),
        ):
            assert comparison_values.original_digest(value, projection=projection,
                complete=complete, enabled=enabled, **options) == {
                "value": None, "scope": "availability-run", "projection": projection,
                "basis": "observed_original", "reason": reason}
    with monkeypatch.context() as guard:
        guard.setattr(comparison_values, "_encoded_digest", forbidden)
        invalid = comparison_values.original_digest({1: "not a JSON key"}, projection="message_json/1",
            complete=True, enabled=True, **options)
    assert invalid["value"] is None and invalid["reason"] == "unsupported_value", \
        "a small hint cannot bypass comparison's ordinary-JSON validation"
    original = {"content": ["before"]}
    snapshots = []
    encoded_digest = comparison_values._encoded_digest

    def inspected(value, key, projection, **kwargs):
        snapshots.append(value)
        assert value is not original and value["content"] is not original["content"]
        return encoded_digest(value, key, projection, **kwargs)

    monkeypatch.setattr(comparison_values, "_encoded_digest", inspected)
    token = comparison_values.original_digest(original, projection="message_json/1",
        complete=True, enabled=True, **options)
    original["content"][0] = "after"
    assert snapshots == [{"content": ["before"]}], "comparison still owns a detached snapshot"
    assert token["value"] == hmac.new(b"comparison-key",
        b'mcp-flightrecorder\0observed_original\0message_json/1\0{"content":["before"]}', hashlib.sha256).hexdigest()


@pytest.mark.parametrize("prefix_first,reason", [(True, "capture_limit"), (False, "encoding_unavailable")],
                         ids=["limit-before-invalid-integer", "invalid-integer-before-limit"])
def test_unhinted_error_order_and_public_message_rejection(tmp_path, monkeypatch, prefix_first, reason):
    previous = sys.get_int_max_str_digits()
    recorder = None
    try:
        sys.set_int_max_str_digits(640)
        value = ["x" * 1_048_576, 10 ** 700]
        if not prefix_first:
            value.reverse()
        token = comparison_values.original_digest(value, key=b"comparison-key", run_id="error-order",
            projection="message_json/1", complete=True, enabled=True)
        assert token["value"] is None and token["reason"] == reason, \
            "unhinted encoding retains the streaming loop's first encountered outcome"
        recorder = Recorder(tmp_path / "invalid-message.sqlite", capture="content", coverage=COVERAGE)

        def forbidden(*args, **kwargs):
            raise AssertionError("public invalid field encoding rejects before comparison")

        monkeypatch.setattr(comparison_values, "_encoded_digest", forbidden)
        receipt = recorder.observe("model.request", scope=recorder.capture_scope(),
            payload={"messages": [value]}, facts={"messages": [{"bindings": [], "complete": True}]})
        assert (receipt.status, receipt.reason) == ("rejected", "invalid_input")
        assert (receipt.event_id, receipt.model_call_id, receipt.decision_id, receipt.operation_id) == (None,) * 4
    finally:
        closed = recorder.close() if recorder is not None else None
        sys.set_int_max_str_digits(previous)
    assert sys.get_int_max_str_digits() == previous
    assert closed is not None and closed.writer_stopped and closed.portable
    with closing(sqlite3.connect(tmp_path / "invalid-message.sqlite")) as db:
        assert db.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM request_members").fetchone()[0] == 0
    assert (recorder.health().dropped_events, recorder.health().writer_errors) == (1, 0)


@pytest.mark.parametrize("annotation", [
    {"bindings": [], "complete": "not-boolean"},
    {"bindings": [{"decision_event_id": "missing-decision", "pointer": "/absent"}]},
], ids=["annotation", "binding"])
def test_message_validation_precedes_field_preparation(tmp_path, monkeypatch, annotation):
    from mcp_flightrecorder import _artifacts

    recorder = Recorder(tmp_path / "invalid-annotation.sqlite", capture="content", coverage=COVERAGE)

    def forbidden(*args, **kwargs):
        raise AssertionError("invalid message annotations/bindings must reject before fields are prepared")

    monkeypatch.setattr(_artifacts, "_fields", forbidden)
    try:
        receipt = recorder.observe("model.request", scope=recorder.capture_scope(),
            payload={"messages": [{"content": "valid"}]}, facts={"messages": [annotation]})
    finally:
        closed = recorder.close()
    assert closed.writer_stopped and closed.portable
    assert (receipt.status, receipt.reason, receipt.event_id, receipt.model_call_id) == ("rejected", "invalid_input", None, None)
    assert (recorder.health().dropped_events, recorder.health().writer_errors) == (1, 0)
    with closing(sqlite3.connect(tmp_path / "invalid-annotation.sqlite")) as db:
        assert db.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0
