"""Installed metadata, retention and encoded-budget acceptance controls.

Every edited database is a test-owned reader-input copy. Producer controls use
only the public Recorder API; expected values do not import reader internals.
"""
import copy
from contextlib import closing
import hashlib
import json
import shutil
import sqlite3

import pytest
from pytest_check import check

from mcp_flightrecorder import Recorder, inspect

from .recordings import integration_recording


FIXTURE, SYMBOLS = integration_recording()
MAX_OUTPUT = 1_048_576
COVERAGE = {"producers": {"model": "reader.model", "host": "reader.host", "mcp": "reader.mcp"}, "model_complete": True}


def encoded(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")


def success(result):
    assert result["ok"] is True, f"A completed read is required before inspecting bounded content: {result['error']}"
    return result["data"]


def error(result, code, reason=None):
    assert result["ok"] is False, f"Expected {code} must be established before its detailed error is inspected"
    check.equal(result["error"]["code"], code, msg="Content/refusal controls retain their precise public error category")
    if reason is not None:
        check.equal(result["error"]["reason"], reason, msg="The expected unavailable content cause remains specific")
    check.is_none(result["data"], msg="No failed content request returns a partial body")
    check.less_equal(len(encoded(result)), 1024, msg="Error diagnostics never echo unbounded content or identity")


def input_copy(tmp_path):
    path = tmp_path / "reader-input.sqlite"
    shutil.copyfile(FIXTURE, path)
    return path


def retained(path, text, *, redacted=False, truncated=False, media_type=None):
    """Replace one retained result in an explicitly edited reader-input copy."""
    with closing(sqlite3.connect(path)) as connection:
        artifact_id = connection.execute("SELECT payload_ref FROM events WHERE event_id=?", (SYMBOLS["E1"],)).fetchone()[0]
        raw = text.encode("utf-8") if text is not None else None
        connection.execute("UPDATE artifacts SET body_text=?,stored_sha256=?,stored_bytes=?,observed_bytes=?,redacted=?,truncated=?,omitted_reason=? WHERE artifact_id=?", (text, hashlib.sha256(raw).hexdigest() if raw is not None else None, len(raw) if raw is not None else 0, len(raw) if raw is not None else 50, int(redacted), int(truncated), None if raw is not None else "capture_metadata", artifact_id))
        if media_type is not None:
            connection.execute("UPDATE artifacts SET media_type=? WHERE artifact_id=?", (media_type, artifact_id))
        connection.commit()
    return artifact_id


def accepted(receipt):
    assert receipt.status == "accepted", f"A public producer observation must be admitted before its retained facts can be used: {receipt.reason}"
    return receipt


def empty_model_pair(recorder):
    request = accepted(recorder.observe("model.request", scope=recorder.capture_scope(), payload={"messages": []}, facts={"messages": []}))
    response = accepted(recorder.observe("model.response", scope=recorder.capture_scope(), payload="response", facts={"model_call_id": request.model_call_id, "state": "complete"}))
    return request, response


def sentinel_recording(path):
    sentinels = {name: "BODY_SENTINEL_" + name.upper() for name in ("prompt", "message", "arguments", "result", "environment", "event", "link", "finding")}
    recorder = Recorder(path, capture="content", coverage=COVERAGE, environment={"hidden": sentinels["environment"]})
    identifiers = {"run": recorder.capture_scope().run_id}
    try:
        with recorder.turn(sentinels["prompt"], conditions={"declared": "allowed-condition"}) as turn:
            accepted(turn.receipt)
            identifiers["turn"] = turn.receipt.turn_id
            user = {"role": "user", "content": sentinels["message"]}
            initial = accepted(recorder.observe("model.request", scope=recorder.capture_scope(), payload={"messages": [user]}, facts={"messages": [{"bindings": [], "unresolved": [], "complete": True}]}))
            arguments = {"text": sentinels["arguments"]}
            decision = accepted(recorder.observe("model.tool_call", scope=recorder.capture_scope(), payload={"arguments": arguments, "arguments_raw": json.dumps(arguments)}, facts={"name": "safe-tool", "model_call_id": initial.model_call_id, "provider_ref": "safe-reference"}))
            accepted(recorder.observe("model.response", scope=recorder.capture_scope(), payload={"done": True}, facts={"model_call_id": initial.model_call_id}))
            with recorder.dispatch(action="execute", name="safe-tool", arguments=arguments, decision_event_ids=[decision.event_id]) as dispatch:
                accepted(dispatch.receipt)
                start = accepted(recorder.start_operation(scope=recorder.capture_scope(), client_id="safe-client", method="tools/call", name="safe-tool", arguments=arguments, decision_id=decision.decision_id, link_basis="host_hook"))
                terminal = accepted(recorder.finish_operation(start.ticket, status="ok", result={"text": sentinels["result"]}, text_paths=["/text"]))
            message = {"role": "tool", "content": sentinels["result"]}
            request = accepted(recorder.observe("model.request", scope=recorder.capture_scope(), payload={"messages": [user, message]}, facts={"messages": [{"bindings": [], "unresolved": [], "complete": True}, {"bindings": [{"decision_event_id": decision.event_id, "pointer": "/content", "result_event_id": terminal.event_id, "text_paths": [""]}], "unresolved": [], "complete": True}]}))
            accepted(recorder.observe("model.response", scope=recorder.capture_scope(), payload={"done": True}, facts={"model_call_id": request.model_call_id}))
            identifiers.update(call=initial.model_call_id, followup=request.model_call_id, request=request.event_id, terminal=terminal.event_id)
    finally:
        closed = recorder.close()
    assert closed.writer_stopped and closed.portable, "The sentinel producer must close portably before reader-input annotations are added"
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("UPDATE events SET details_json=json_set(details_json,'$.arbitrary',?)", (sentinels["event"],))
        connection.execute("UPDATE links SET details_json=json_set(details_json,'$.arbitrary',?)", (sentinels["link"],))
        connection.execute("INSERT INTO findings(finding_id,run_id,turn_id,target_event_id,analysis_id,checker,checker_version,config_json,config_hash,metric,result,score,reason,evidence_ids_json,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", ("f_control", identifiers["run"], identifiers["turn"], None, "analysis", "checker", "1", json.dumps({"secret": sentinels["finding"]}), "hash", "metric", "unknown", None, sentinels["finding"], "[]", "2000-01-01T00:00:00Z"))
        identifiers["artifact"] = connection.execute("SELECT payload_ref FROM events WHERE event_id=?", (terminal.event_id,)).fetchone()[0]
        connection.commit()
    return identifiers, sentinels


def test_metadata_allowlist_excludes_bodies_and_arbitrary_json(tmp_path):
    path = tmp_path / "sentinels.sqlite"
    ids, sentinels = sentinel_recording(path)
    results = [inspect.runs(path), inspect.find_turns(path, ids["run"]), inspect.explain(path, ids["run"], ids["turn"], ids["call"], max_output_bytes=MAX_OUTPUT), inspect.check(path, ids["run"]), inspect.slice(path, ids["run"], ids["request"], max_output_bytes=MAX_OUTPUT)]
    for result in results:
        success(result)
        text = encoded(result).decode("utf-8")
        for name, sentinel in sentinels.items():
            check.is_false(sentinel in text, msg=f"Default {result['operation']} metadata never exposes the {name} body/arbitrary JSON sentinel")
        for field in ("body_text", "details_json", "environment_json", "policy_json", "config_json"):
            check.is_false('"' + field + '":' in text, msg=f"Default {result['operation']} contains only the named allowlisted metadata projections")
    turns = results[1]["data"]["items"]
    check.equal(turns[0]["turn"]["conditions"], {"declared": "allowed-condition"}, msg="A complete small declared conditions value remains allowed in ordinary metadata")
    explicit = success(inspect.payload(path, ids["run"], ids["artifact"]))
    check.is_true(sentinels["result"] in explicit["text"], msg="Retained result content becomes available only through explicit payload access")


@pytest.mark.parametrize("size", [512, 513], ids=["exact-512", "over-513"])
def test_conditions_are_whole_values_with_exact_encoded_cap(tmp_path, size):
    path = input_copy(tmp_path)
    value = "é" * 255 if size == 512 else "é" * 255 + "x"
    assert len(encoded(value)) == size, "The independent control must have the specified compact UTF-8 JSON byte size"
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("UPDATE turns SET conditions_json=? WHERE turn_id=?", (encoded(value).decode(), SYMBOLS["T1"]))
        condition_key = connection.execute("SELECT condition_key FROM turns WHERE turn_id=?", (SYMBOLS["T1"],)).fetchone()[0]
        connection.commit()
    result = inspect.find_turns(path, SYMBOLS["R"], filters={"condition_key": condition_key})
    item = success(result)["items"][0]["turn"]
    check.equal(item["conditions"], value if size == 512 else None, msg="Conditions are either one complete parsed value within 512 bytes or null, never cropped JSON")
    check.equal(any(entry["code"] == "string_limit" for entry in result["cutoffs"]), size == 513, msg="Only the oversized declared conditions value needs a cutoff")
    hidden = inspect.find_turns(path, SYMBOLS["R"], filters={"condition_key": condition_key}, no_content=True)
    check.is_none(success(hidden)["items"][0]["turn"]["conditions"], msg="No-content suppresses the whole declared conditions value")
    check.equal(hidden["data"]["items"][0]["turn"]["condition_key"], condition_key, msg="Condition identity remains usable for exact filtering under no-content")
    check.equal(hidden["cutoffs"], [], msg="Deliberate no-content policy omission is not manufactured capture loss or truncation")


@pytest.mark.parametrize("operation", ["runs", "find_turns", "explain", "check", "slice"], ids=["runs", "turns", "explain", "check", "slice"])
def test_no_content_preserves_metadata_associations_and_comparisons(operation):
    args = {"runs": [FIXTURE], "find_turns": [FIXTURE, SYMBOLS["R"]], "explain": [FIXTURE, SYMBOLS["R"], SYMBOLS["T1"], SYMBOLS["M0"]], "check": [FIXTURE, SYMBOLS["R"]], "slice": [FIXTURE, SYMBOLS["R"], SYMBOLS["W3"]]}[operation]
    ordinary = getattr(inspect, operation)(*args, max_output_bytes=MAX_OUTPUT)
    hidden = getattr(inspect, operation)(*args, max_output_bytes=MAX_OUTPUT, no_content=True)
    success(ordinary)
    success(hidden)
    expected = copy.deepcopy(ordinary["data"])
    def suppress_conditions(value):
        if isinstance(value, dict):
            if "conditions" in value:
                value["conditions"] = None
            for child in value.values():
                suppress_conditions(child)
        elif isinstance(value, list):
            for child in value:
                suppress_conditions(child)
    suppress_conditions(expected)
    check.equal(hidden["data"], expected, msg="No-content changes only declared conditions; counts, retention, manifests, associations and original comparisons remain identical")
    check.equal(hidden["unknowns"], ordinary["unknowns"], msg="Internal manifest validation remains available under output-only policy")
    if operation == "slice":
        check.equal([answer["text_equal"] for answer in hidden["data"]["answers"]], [True, False], msg="No-content output does not disable supported original-token comparisons or turn known differences into unknowns.")


@pytest.mark.parametrize("operation", ["payload", "query"])
def test_no_content_rejection_precedes_recording_access(tmp_path, operation):
    missing = tmp_path / "not-present.sqlite"
    result = inspect.payload(missing, "run", "artifact", no_content=True) if operation == "payload" else inspect.query(missing, "SELECT body_text FROM artifacts", no_content=True)
    error(result, "content_disabled", "no_content")
    check.is_false(missing.exists(), msg="No-content rejection occurs before any source is opened or created")
    bad = inspect.payload(missing, "run", "artifact", offset=True, no_content=True) if operation == "payload" else inspect.query(missing, "SELECT 1", limit=True, no_content=True)
    error(bad, "invalid_request")


@pytest.mark.parametrize("operation", ["find_turns", "explain"], ids=["turns", "explain"])
@pytest.mark.parametrize("no_content", [False, True], ids=["ordinary", "no-content"])
def test_missing_manifest_is_unavailable_membership_not_known_empty(tmp_path, operation, no_content):
    path = input_copy(tmp_path)
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("UPDATE artifacts SET body_text=NULL,stored_sha256=NULL,stored_bytes=0,omitted_reason='capture_limit' WHERE artifact_id=(SELECT payload_ref FROM events WHERE event_id=?)", (SYMBOLS["W3"],))
        connection.commit()
    result = inspect.find_turns(path, SYMBOLS["R"], no_content=no_content, max_output_bytes=MAX_OUTPUT) if operation == "find_turns" else inspect.explain(path, SYMBOLS["R"], SYMBOLS["T2"], SYMBOLS["M3"], no_content=no_content, max_output_bytes=MAX_OUTPUT)
    data = success(result)
    counts = next(row["counts"] for row in data["items"] if row["turn"]["turn_id"] == SYMBOLS["T2"]) if operation == "find_turns" else data["counts"]
    check.equal(counts["request_members"], 8, msg="Only W2's retained memberships remain countable after W3's manifest is unavailable")
    check.equal(counts["input_answers"], 2, msg="Missing W3 membership does not invent a number of unexposed answer occurrences")
    check.is_true(any(item["code"] == "manifest_omitted" and item["ids"].get("request_event_id") == SYMBOLS["W3"] for item in result["unknowns"]), msg="Zero exposed W3 members carries a specific unavailable-manifest diagnostic, including under no-content")


@pytest.mark.parametrize("state", ["omitted", "empty", "redacted", "truncated", "redacted-truncated"], ids=["omitted", "empty", "redacted", "truncated", "redacted-truncated"])
def test_retained_payload_states(tmp_path, state):
    path = input_copy(tmp_path)
    text = None if state == "omitted" else "" if state == "empty" else "retained-only: é😀"
    artifact = retained(path, text, redacted="redacted" in state, truncated="truncated" in state)
    result = inspect.payload(path, SYMBOLS["R"], artifact)
    if state == "omitted":
        error(result, "payload_omitted")
        return
    data = success(result)
    check.equal(data["text"], text, msg="Payload returns exactly retained bytes without reconstructing omitted originals")
    check.equal(data["total_bytes"], len(text.encode()), msg="Payload totals refer to retained UTF-8 bytes, including successful empty strings")
    check.equal(data["returned_bytes"], len(text.encode()), msg="Returned-byte accounting is independent of character count")
    check.equal(data["artifact"]["redacted"], "redacted" in state, msg="Redaction remains a JSON boolean label on retained content")
    check.equal(data["artifact"]["truncated"], "truncated" in state, msg="Truncation remains independent of redaction and omission")
    check.is_none(data["next_offset"], msg="A complete retained body has no continuation, including a retained empty body")


@pytest.mark.parametrize("offset,text,next_offset", [(0, "Aé", 3), (1, "é", 3), (3, "中", 6), (6, "😀", 10), (10, "Z", None), (11, "", None)], ids=["zero", "one", "three", "six", "ten", "end-eleven"])
def test_payload_utf8_boundaries(tmp_path, offset, text, next_offset):
    path = input_copy(tmp_path)
    artifact = retained(path, "Aé中😀Z")
    data = success(inspect.payload(path, SYMBOLS["R"], artifact, offset=offset, max_bytes=4))
    check.equal(data["text"], text, msg="Four content bytes retain only complete UTF-8 code points at the selected boundary")
    check.equal(data["offset"], offset, msg="A valid retained-byte boundary is never silently rounded")
    check.equal(data["returned_bytes"], len(text.encode()), msg="Chunk size is its actual UTF-8 byte length")
    check.equal(data["total_bytes"], 11, msg="The independent Aé中😀Z control has exactly eleven retained bytes")
    check.equal(data["next_offset"], next_offset, msg="Next offset advances by emitted bytes or is null at retained end")


@pytest.mark.parametrize("offset", [2, 4, 5, 7, 8, 9, -1, 12, 1.0, True, "0"], ids=["inside-e", "inside-zhong-1", "inside-zhong-2", "inside-emoji-1", "inside-emoji-2", "inside-emoji-3", "negative", "beyond-end", "float", "bool", "string"])
def test_payload_rejects_invalid_utf8_offsets(tmp_path, offset):
    path = input_copy(tmp_path)
    artifact = retained(path, "Aé中😀Z")
    error(inspect.payload(path, SYMBOLS["R"], artifact, offset=offset, max_bytes=4), "invalid_request")


@pytest.mark.parametrize("max_bytes", [True, 3, 65537, 4.0], ids=["bool", "below-minimum", "over-maximum", "float"])
def test_invalid_payload_content_budgets_precede_open(tmp_path, max_bytes):
    error(inspect.payload(tmp_path / "missing.sqlite", "run", "artifact", max_bytes=max_bytes), "invalid_request")


@pytest.mark.parametrize("budget,expected", [(None, 4096), (65536, 65536)], ids=["default-4096", "maximum-65536"])
def test_payload_content_budget_is_not_metadata_display_limit(tmp_path, budget, expected):
    path = input_copy(tmp_path)
    artifact = retained(path, "x" * 65537)
    kwargs = {} if budget is None else {"max_bytes": budget}
    data = success(inspect.payload(path, SYMBOLS["R"], artifact, max_output_bytes=MAX_OUTPUT, **kwargs))
    check.equal(data["returned_bytes"], expected, msg="Explicit payload text uses the assigned content bound rather than the 512-byte metadata display cap")
    check.equal(data["next_offset"], expected, msg="Payload continuation starts after exactly the retained content budget")
    check.equal(data["total_bytes"], 65537, msg="A content cap does not reduce the retained aggregate byte count")


def test_original_comparisons_survive_body_only_omission(tmp_path):
    path = input_copy(tmp_path)
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("UPDATE artifacts SET body_text=NULL,stored_sha256=NULL,stored_bytes=0,omitted_reason='capture_metadata' WHERE representation!='request_refs/2'")
        connection.commit()
    data = success(inspect.explain(path, SYMBOLS["R"], SYMBOLS["T1"], SYMBOLS["M0"], max_output_bytes=MAX_OUTPUT))["detail"]
    check.equal([row["forwarded_changed"] for row in data["dispatches"]], [True, False], msg="Complete original argument tokens remain usable after retained argument bodies disappear")
    check.equal([row["text_equal"] for row in data["produced_answers"]], [True, False, True, False, True, False], msg="Complete original result/submitted tokens remain usable after result and message bodies disappear")
    check.equal([row["association_state"] for row in data["produced_answers"]], ["linked"] * 6, msg="Body omission preserves explicit manifest membership and binding identity")


TOKEN_CASES = [("disabled", "disabled"), ("partial", "observation_incomplete"), ("projection", "digest_projection_mismatch"), ("scope", "digest_scope_mismatch"), ("basis", "digest_basis_mismatch"), ("malformed", "malformed_digest"), ("missing", "missing_digest"), ("capacity", "capture_limit"), ("original-partial", "original_incomplete")]


@pytest.mark.parametrize("comparison", ["forwarded", "text"])
@pytest.mark.parametrize("variant,reason", TOKEN_CASES, ids=[row[0] for row in TOKEN_CASES])
def test_unavailable_original_comparison_is_null_with_reason(tmp_path, comparison, variant, reason):
    path = input_copy(tmp_path)
    with closing(sqlite3.connect(path)) as connection:
        if comparison == "forwarded":
            table, identity, selected, key = "events", "event_id", SYMBOLS["D1"], "args_digest"
        else:
            table, identity, selected, key = "links", "link_id", connection.execute("SELECT link_id FROM links WHERE relation='answers' AND source_event_id=?", (SYMBOLS["D1"],)).fetchone()[0], "submitted_text_digest"
        details = json.loads(connection.execute("SELECT details_json FROM " + table + " WHERE " + identity + "=?", (selected,)).fetchone()[0])
        if variant == "partial":
            event = SYMBOLS["H1"] if comparison == "forwarded" else SYMBOLS["E1"]
            connection.execute("UPDATE events SET details_json=json_set(details_json,'$.complete',json('false')) WHERE event_id=?", (event,))
        elif variant == "missing":
            details.pop(key)
        elif variant in {"disabled", "capacity", "original-partial"}:
            details[key].update(value=None, reason={"disabled": "disabled", "capacity": "capture_limit", "original-partial": "original_incomplete"}[variant])
        else:
            field, value = {"projection": ("projection", "unsupported/1"), "scope": ("scope", "another-run"), "basis": ("basis", "retained_copy"), "malformed": ("value", "A" * 64)}[variant]
            details[key][field] = value
        connection.execute("UPDATE " + table + " SET details_json=? WHERE " + identity + "=?", (json.dumps(details), selected))
        connection.commit()
    result = inspect.explain(path, SYMBOLS["R"], SYMBOLS["T1"], SYMBOLS["M0"], max_output_bytes=MAX_OUTPUT)
    detail = success(result)["detail"]
    values = detail["dispatches"] if comparison == "forwarded" else detail["produced_answers"]
    matches = [row for row in values if row["decision_event_id" if comparison == "forwarded" else "source_event_id"] == SYMBOLS["D1"]]
    assert matches, "The explicit association must remain present before inspecting comparison uncertainty"
    for row in matches:
        check.is_none(row["forwarded_changed" if comparison == "forwarded" else "text_equal"], msg="Unavailable or incompatible original comparison evidence is null, never false")
        check.equal(row["comparison_reason"], reason, msg="The unavailable comparison reports its specific token/observation cause")
        if comparison == "text":
            check.equal(row["association_state"], "linked", msg="Digest unavailability alone never erases an explicit known answer binding")
    check.is_true(any(item["code"] == reason for item in result["unknowns"]), msg="The envelope discloses comparison uncertainty without exposing raw HMAC values")


def test_exact_encoded_payload_budget_and_one_byte_over(tmp_path):
    path = input_copy(tmp_path)
    text = 'é"\\\n\t😀' * 200
    artifact = retained(path, text)
    full = inspect.payload(path, SYMBOLS["R"], artifact, max_bytes=65536, max_output_bytes=MAX_OUTPUT)
    success(full)
    exact = len(encoded(full))
    assert 1024 < exact < MAX_OUTPUT, "The independently encoded control must fit inside the allowed output-budget range"
    at_cap = inspect.payload(path, SYMBOLS["R"], artifact, max_bytes=65536, max_output_bytes=exact)
    check.equal(at_cap, full, msg="An envelope exactly at the compact UTF-8 cap is emitted without a false cutoff")
    smaller = inspect.payload(path, SYMBOLS["R"], artifact, max_bytes=65536, max_output_bytes=exact - 1)
    data = success(smaller)
    check.less_equal(len(encoded(smaller)), exact - 1, msg="The whole JSON encoding counts multibyte, quote, backslash and control-string escapes")
    check.is_true(text.startswith(data["text"]), msg="Byte trimming returns an intact retained-text prefix")
    check.equal(data["returned_bytes"], len(data["text"].encode()), msg="Output trimming updates the content byte count exactly")
    check.equal(data["next_offset"], data["returned_bytes"], msg="The continuation starts after the emitted prefix, never a removed character")
    check.equal(data["artifact"]["artifact_id"], artifact, msg="Encoded-budget fitting never shortens an artifact identity")
    check.is_true(any(item["code"] == "byte_limit" for item in smaller["cutoffs"]), msg="A one-byte envelope overflow produces an explicit output cutoff")


def stored_payload_metadata(path, artifact_id):
    """Independent literal public fields from this test's retained input."""
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as connection:
        connection.row_factory = sqlite3.Row
        row = connection.execute("SELECT artifact_id,run_id,media_type,representation,stored_sha256,stored_bytes,observed_bytes,redacted,truncated,omitted_reason,policy_id FROM artifacts WHERE artifact_id=?", (artifact_id,)).fetchone()
        assert row is not None, "The allocated retained artifact must exist before building independent payload expectations"
        return {"artifact_id": row["artifact_id"], "run_id": row["run_id"],
                "media_type": row["media_type"], "representation": row["representation"],
                "stored_sha256": row["stored_sha256"], "stored_bytes": row["stored_bytes"],
                "observed_bytes": row["observed_bytes"], "redacted": bool(row["redacted"]),
                "truncated": bool(row["truncated"]), "omitted_reason": row["omitted_reason"],
                "policy_id": row["policy_id"]}


def literal_payload_envelope(artifact, body, text, *, offset=0, cutoffs=(), omitted=None):
    """Assemble the documented envelope without a reader projection or fitter."""
    returned_bytes = len(text.encode("utf-8"))
    total_bytes = len(body.encode("utf-8"))
    end = offset + returned_bytes
    return {"format": "mfr.inspect/1", "operation": "payload", "ok": True,
            "scope": {"run_id": artifact["run_id"], "artifact_id": artifact["artifact_id"]},
            "data": {"artifact": artifact, "offset": offset, "returned_bytes": returned_bytes,
                     "total_bytes": total_bytes, "text": text, "next_offset": end if end < total_bytes else None},
            "unknowns": [], "cutoffs": list(cutoffs),
            "diagnostics_omitted": {"unknowns": 0, "cutoffs": 0} if omitted is None else omitted,
            "error": None, "untrusted_content": True}


@pytest.mark.parametrize("kind", ["ascii", "control", "mixed"])
def test_payload_large_window_exact_maximal_prefix(tmp_path, monkeypatch, kind):
    from mcp_flightrecorder import _inspect_common, _inspect_navigation

    path = input_copy(tmp_path)
    if kind == "ascii":
        body = "x" * 65536
    elif kind == "control":
        body = "\x00" * 65536
    else:
        unit = 'Aé中😀"\\\n\t'
        body = unit * (65536 // len(unit.encode("utf-8")))
        body += "x" * (65536 - len(body.encode("utf-8")))
    assert len(body.encode("utf-8")) == 65536, "Every large-window input must fill the specified independent UTF-8 content limit"
    artifact_id = retained(path, body)
    artifact = stored_payload_metadata(path, artifact_id)
    original_encode = _inspect_common.encode
    serializations = 0

    def counted_encode(value):
        nonlocal serializations
        serializations += 1
        return original_encode(value)

    # Count only the public operation; restore both existing references before
    # the independent exhaustive oracle does any serialization of its own.
    with monkeypatch.context() as patch:
        patch.setattr(_inspect_common, "encode", counted_encode)
        patch.setattr(_inspect_navigation, "encode", counted_encode)
        result = inspect.payload(path, SYMBOLS["R"], artifact_id, max_bytes=65536, max_output_bytes=1024)
    data = success(result)
    check.less_equal(serializations, 32, msg="Large payload fitting must use bounded full-envelope serialization rather than one serialization per removed character")
    byte_limit = {"code": "byte_limit", "path": "", "ids": {}, "count": None}
    fitting_lengths = [length for length in range(1, min(len(body) - 1, 1024) + 1)
                       if len(encoded(literal_payload_envelope(artifact, body, body[:length], cutoffs=[byte_limit]))) <= 1024]
    assert fitting_lengths, "The independent literal fixture must have a positive proper prefix that fits the minimum envelope budget"
    largest = max(fitting_lengths)
    expected = literal_payload_envelope(artifact, body, body[:largest], cutoffs=[byte_limit])
    check.equal(result, expected, msg="The payload, all stored identity and retention facts, byte counts, continuation and diagnostics equal the independent maximal-prefix envelope")
    check.is_true(0 < len(data["text"]) < len(body), msg="A successful bounded nonempty window retains a positive proper prefix")
    check.less_equal(len(encoded(result)), 1024, msg="The entire returned envelope fits the cap including escapes and multibyte characters")
    check.greater(len(encoded(literal_payload_envelope(artifact, body, body[:largest + 1], cutoffs=[byte_limit]))), 1024,
                  msg="The next whole source character cannot fit, proving exact maximality independently of the fitting implementation")


def test_payload_full_end_window_precedes_prefix_search(tmp_path):
    path = input_copy(tmp_path)
    body = "x" * 100004
    artifact_id = retained(path, body, media_type="m" * 500)
    artifact = stored_payload_metadata(path, artifact_id)
    full = inspect.payload(path, SYMBOLS["R"], artifact_id, offset=100000, max_bytes=4, max_output_bytes=MAX_OUTPUT)
    success(full)
    expected = literal_payload_envelope(artifact, body, "xxxx", offset=100000)
    check.equal(full, expected, msg="The complete end window keeps its literal four bytes, stored total and null continuation")
    cap = len(encoded(full))
    assert 1024 <= cap <= MAX_OUTPUT, "The independently encoded full end window must define a legal output cap"
    shorter = literal_payload_envelope(artifact, body, "xxx", offset=100000)
    check.equal(len(encoded(shorter)), cap + 1, msg="Before any cutoff, the integer continuation 100003 makes three characters one byte larger than the full null-ended window")
    at_cap = inspect.payload(path, SYMBOLS["R"], artifact_id, offset=100000, max_bytes=4, max_output_bytes=cap)
    success(at_cap)
    check.equal(at_cap, expected, msg="The full end window is tested first and succeeds unchanged at its exact cap without entering proper-prefix fitting")


def test_payload_diagnostic_fallback_does_not_regrow_text(tmp_path):
    path = input_copy(tmp_path)
    body = "x" * 100
    artifact_id = retained(path, body, media_type="m" * 600)
    artifact = stored_payload_metadata(path, artifact_id)
    artifact["media_type"] = "m" * 512
    string_limit = {"code": "string_limit", "path": "/artifact/media_type", "ids": {}, "count": 88}
    byte_limit = {"code": "byte_limit", "path": "", "ids": {}, "count": None}
    initial = literal_payload_envelope(artifact, body, "x", cutoffs=[string_limit, byte_limit])
    cap = len(encoded(initial)) - 1
    assert cap == 1451, "The pinned fixture must place the initial one-character envelope exactly one byte above the specified 1451-byte cap"
    result = inspect.payload(path, SYMBOLS["R"], artifact_id, max_bytes=65536, max_output_bytes=cap)
    success(result)
    expected = literal_payload_envelope(artifact, body, "x", cutoffs=[byte_limit], omitted={"unknowns": 0, "cutoffs": 1})
    check.equal(result, expected, msg="The one-character fallback retains the byte cutoff and drops only the media diagnostic without regrowing payload text")
    check.less_equal(len(encoded(result)), cap, msg="Diagnostic fallback fits the complete final envelope within its cap")
    longer = literal_payload_envelope(artifact, body, "xx", cutoffs=[byte_limit], omitted={"unknowns": 0, "cutoffs": 1})
    check.less_equal(len(encoded(longer)), cap, msg="Two characters would fit after diagnostic removal, so retaining one proves the required no-regrowth priority")


def test_next_payload_character_cannot_fit_returns_limit(tmp_path):
    path = input_copy(tmp_path)
    artifact = retained(path, "😀", media_type="m" * 500)
    empty = inspect.payload(path, SYMBOLS["R"], artifact, offset=4)
    success(empty)
    cap = len(encoded(empty))
    assert cap >= 1024, "The metadata-sized empty result must define a valid minimum output budget"
    check.is_true(inspect.payload(path, SYMBOLS["R"], artifact, offset=4, max_output_bytes=cap)["ok"], msg="The required metadata envelope fits exactly at retained end")
    error(inspect.payload(path, SYMBOLS["R"], artifact, max_output_bytes=cap), "limit_exceeded", "required_output")


def test_required_identity_is_never_shortened_to_fit(tmp_path):
    path = input_copy(tmp_path)
    artifact = retained(path, "x")
    long_id = "artifact_" + "é" * 1200
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("UPDATE artifacts SET artifact_id=? WHERE artifact_id=?", (long_id, artifact))
        connection.commit()
    error(inspect.payload(path, SYMBOLS["R"], long_id, max_output_bytes=1024), "limit_exceeded", "required_output")
    large = success(inspect.payload(path, SYMBOLS["R"], long_id, max_output_bytes=MAX_OUTPUT))
    check.equal(large["artifact"]["artifact_id"], long_id, msg="A sufficient cap preserves the full identity instead of applying the display-string rule")


@pytest.mark.parametrize("kind", ["runs", "turns"])
def test_byte_trimmed_pages_resume_after_last_emitted_row(tmp_path, kind):
    path = input_copy(tmp_path)
    with closing(sqlite3.connect(path)) as connection:
        connection.row_factory = sqlite3.Row
        table, field = ("runs", "run_id") if kind == "runs" else ("turns", "turn_id")
        connection.execute("UPDATE " + table + " SET started_at='2000-01-01T00:00:00Z'")
        template = dict(connection.execute("SELECT * FROM " + table + " LIMIT 1").fetchone())
        columns = tuple(template)
        for index in range(12):
            values = {**template, field: ("r_budget_" if kind == "runs" else "t_budget_") + str(index).zfill(3)}
            connection.execute("INSERT INTO " + table + " (" + ",".join(columns) + ") VALUES (" + ",".join("?" for _ in columns) + ")", [values[column] for column in columns])
        expected = [row[0] for row in connection.execute("SELECT " + field + " FROM " + table + " ORDER BY started_at," + field)]
        connection.commit()
    call = (lambda **kw: inspect.runs(path, **kw)) if kind == "runs" else (lambda **kw: inspect.find_turns(path, SYMBOLS["R"], **kw))
    first = call(limit=1, max_output_bytes=MAX_OUTPUT)
    success(first)
    cap = max(1024, len(encoded(first)) + 200)
    emitted, token = [], None
    for _ in range(len(expected) + 1):
        result = call(limit=200, cursor=token, max_output_bytes=cap)
        data = success(result)
        check.less_equal(len(encoded(result)), cap, msg="All emitted pagination metadata and diagnostics fit the selected byte cap")
        identities = [row["run_id"] if kind == "runs" else row["turn"]["turn_id"] for row in data["items"]]
        assert identities, "A successful nonterminal page must retain at least one whole requested item"
        emitted.extend(identities)
        token = data["next_cursor"]
        if token is None:
            break
        check.is_true(any(item["code"] == "byte_limit" for item in result["cutoffs"]), msg="Byte-trimmed optional page rows are disclosed rather than silently omitted")
    check.equal(emitted, expected, msg="Pagination resumes from the final emitted ordering tuple, recovering every byte-trimmed row once")


@pytest.mark.parametrize("size", [512, 513], ids=["exact-512", "over-513"])
def test_display_string_cap_preserves_codepoints_and_ids(tmp_path, size):
    path = input_copy(tmp_path)
    label = "é" * 256 + ("x" if size == 513 else "")
    identity = "identity_" + "中" * 300
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("UPDATE events SET name=? WHERE event_id=?", (label, SYMBOLS["D1"]))
        connection.execute("UPDATE turns SET dataset_item_id=? WHERE turn_id=?", (identity, SYMBOLS["T1"]))
        connection.commit()
    result = inspect.explain(path, SYMBOLS["R"], SYMBOLS["T1"], SYMBOLS["M0"], max_output_bytes=MAX_OUTPUT)
    data = success(result)
    decision = next(row for row in data["detail"]["events"] if row["event_id"] == SYMBOLS["D1"])
    check.equal(decision["name"], "é" * 256, msg="Metadata labels clip only after a whole UTF-8 character at 512 bytes")
    check.equal(data["turn"]["dataset_item_id"], identity, msg="Identity fields remain intact even when longer than the label display cap")
    check.equal(any(item["code"] == "string_limit" for item in result["cutoffs"]), size == 513, msg="Exactly capped labels are retained and one-byte-over labels carry a cutoff")


@pytest.mark.parametrize("kind", ["runs", "turns"])
@pytest.mark.parametrize("population", [200, 201], ids=["exact-200", "over-201"])
def test_discovery_hard_item_cap(tmp_path, kind, population):
    path = input_copy(tmp_path)
    with closing(sqlite3.connect(path)) as connection:
        connection.row_factory = sqlite3.Row
        table, field = ("runs", "run_id") if kind == "runs" else ("turns", "turn_id")
        template = dict(connection.execute("SELECT * FROM " + table + " LIMIT 1").fetchone())
        initial = connection.execute("SELECT count(*) FROM " + table).fetchone()[0]
        columns = tuple(template)
        for index in range(population - initial):
            values = {**template, field: "item_" + str(index).zfill(4)}
            connection.execute("INSERT INTO " + table + " (" + ",".join(columns) + ") VALUES (" + ",".join("?" for _ in columns) + ")", [values[column] for column in columns])
        connection.commit()
    result = inspect.runs(path, limit=200, max_output_bytes=MAX_OUTPUT) if kind == "runs" else inspect.find_turns(path, SYMBOLS["R"], limit=200, max_output_bytes=MAX_OUTPUT)
    data = success(result)
    check.equal(len(data["items"]), 200, msg="Discovery retains the exact permitted 200-item prefix")
    check.equal(data["has_more"], population == 201, msg="One-item lookahead distinguishes exact population from overflow")
    check.equal(any(item["code"] == "item_limit" for item in result["cutoffs"]), population == 201, msg="The hard item cap is reported only when another known row exists")
    default = inspect.runs(path, max_output_bytes=MAX_OUTPUT) if kind == "runs" else inspect.find_turns(path, SYMBOLS["R"], max_output_bytes=MAX_OUTPUT)
    check.equal(len(success(default)["items"]), 50, msg="The default discovery page uses the independently specified 50-item limit")
    check.is_true(default["data"]["has_more"], msg="The default page truthfully reports the available rows beyond its 50-item prefix")


@pytest.mark.parametrize("population", [50, 51], ids=["exact-50", "over-51"])
def test_public_call_overview_cap(tmp_path, population):
    path = tmp_path / "calls.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    run = recorder.capture_scope().run_id
    call_ids = []
    try:
        with recorder.turn("Overview cap") as turn:
            accepted(turn.receipt)
            for _ in range(population):
                request, _response = empty_model_pair(recorder)
                call_ids.append(request.model_call_id)
    finally:
        closed = recorder.close()
    assert closed.writer_stopped and closed.portable, "The call-cap producer must finish before exact aggregate counts are checked"
    result = inspect.explain(path, run, turn.receipt.turn_id, max_output_bytes=MAX_OUTPUT)
    data = success(result)
    check.equal(data["counts"]["model_calls"], population, msg="Overview limits never reduce the completed model-call aggregate")
    check.equal([row["model_call_id"] for row in data["calls"]], call_ids[:50], msg="Overview selects the deterministic earliest 50 call summaries")
    check.equal(any(item["code"] == "item_limit" and item["path"] == "/calls" for item in result["cutoffs"]), population == 51, msg="Call overview overflow is disclosed only beyond its exact hard cap")


@pytest.mark.parametrize("population", [200, 201], ids=["exact-200", "over-201"])
def test_selected_detail_total_record_cap(tmp_path, population):
    path = tmp_path / "detail-reader-input.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    run = recorder.capture_scope().run_id
    try:
        with recorder.turn("Detail cap") as turn:
            accepted(turn.receipt)
            request, response = empty_model_pair(recorder)
    finally:
        closed = recorder.close()
    assert closed.writer_stopped and closed.portable, "The public two-event seed must close before its labelled cap-control copy is expanded"
    # This deliberately expanded reader-input fixture repeats complete response
    # observations. Its expected population is ordinary SQLite rows plus the
    # independently counted request/template/response artifacts, not a reader
    # helper's computed total.
    with closing(sqlite3.connect(path)) as connection:
        connection.row_factory = sqlite3.Row
        artifact_count = connection.execute("SELECT count(*) FROM artifacts WHERE artifact_id NOT IN (SELECT prompt_ref FROM turns WHERE prompt_ref IS NOT NULL)").fetchone()[0]
        seed = dict(connection.execute("SELECT * FROM events WHERE event_id=?", (response.event_id,)).fetchone())
        columns = tuple(seed)
        for index in range(population - artifact_count - 2):
            values = {**seed, "event_id": "e_extra_" + str(index).zfill(4), "sequence": index + 3}
            connection.execute("INSERT INTO events (" + ",".join(columns) + ") VALUES (" + ",".join("?" for _ in columns) + ")", [values[column] for column in columns])
        connection.commit()
    result = inspect.explain(path, run, turn.receipt.turn_id, request.model_call_id, max_output_bytes=MAX_OUTPUT)
    detail = success(result)["detail"]
    arrays = ("events", "operations", "links", "artifacts", "members", "input_answers", "produced_answers", "dispatches")
    check.equal(sum(detail["totals"].values()), population, msg="Selected-call totals include every table-like detail array before output limits")
    check.equal(sum(len(detail[name]) for name in arrays), 200, msg="The detail hard cap counts all emitted records, including artifacts")
    check.equal(len(detail["events"]), population - artifact_count, msg="Deterministic selection retains the events array before later artifact rows")
    check.equal(any(item["code"] == "item_limit" for item in result["cutoffs"]), population == 201, msg="The exact 200-record result is complete while one more available record is disclosed")


def test_explain_byte_trimming_preserves_exact_totals(tmp_path):
    result = inspect.explain(FIXTURE, SYMBOLS["R"], SYMBOLS["T1"], SYMBOLS["M0"], max_output_bytes=MAX_OUTPUT)
    full = success(result)
    cap = 4500
    trimmed = inspect.explain(FIXTURE, SYMBOLS["R"], SYMBOLS["T1"], SYMBOLS["M0"], max_output_bytes=cap)
    data = success(trimmed)
    check.less_equal(len(encoded(trimmed)), cap, msg="A selected explanation fits its whole encoded output budget")
    check.equal(data["counts"], full["counts"], msg="Byte-limited explanation preserves exact turn-wide counts")
    check.equal(data["detail"]["totals"], full["detail"]["totals"], msg="Selected detail totals are exact before optional array records are removed")
    check.equal(data["calls"], full["calls"], msg="The required selected-call summary and its IDs remain intact")
    check.is_true(any(item["code"] == "byte_limit" for item in trimmed["cutoffs"]), msg="Optional detail removal produces an explicit byte cutoff without fabricated continuation")
    check.is_false("next_cursor" in data, msg="Explain does not invent nested paging semantics after trimming")
