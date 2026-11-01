"""Public request identity, first-witness ordering and unresolved binding controls."""
from contextlib import closing
import hashlib
import json
import sqlite3
import threading

import pytest
from pytest_check import check
from mcp_flightrecorder import Recorder

COVERAGE = {"producers": {"model": "test.model", "host": "test.host", "mcp": "test.mcp"},
            "model_complete": True}


def accept(receipt):
    assert receipt.status == "accepted", f"binding fixture requires admitted prerequisite, got {receipt.status}/{receipt.reason}"
    return receipt


def observe(recorder, kind, payload, **facts):
    return accept(recorder.observe(kind, scope=recorder.capture_scope(), payload=payload, facts=facts))


def decision(recorder, provider_ref="reused"):
    request = observe(recorder, "model.request", {"messages": []}, messages=[])
    receipt = observe(recorder, "model.tool_call", {"arguments": {"value": 1}}, name="echo",
                      model_call_id=request.model_call_id, provider_ref=provider_ref)
    observe(recorder, "model.response", {}, model_call_id=request.model_call_id)
    return receipt


def submit(recorder, messages, annotations, **facts):
    payload = {"messages": messages, **facts.pop("fields", {})}
    receipt = observe(recorder, "model.request", payload, messages=annotations, **facts)
    observe(recorder, "model.response", {}, model_call_id=receipt.model_call_id)
    return receipt


def binding(decision_event, pointer="/content", result_event=None):
    return {"decision_event_id": decision_event, "pointer": pointer,
            "result_event_id": result_event, "text_paths": [""]}


def annotation(*bindings, unresolved=None):
    return {"bindings": list(bindings), "unresolved": [] if unresolved is None else unresolved, "complete": True}


def members(db, request_id):
    return db.execute("SELECT * FROM request_members WHERE request_event_id=? ORDER BY message_index", (request_id,)).fetchall()


def test_shifted_duplicate_messages_and_request_fields_share_one_binding(tmp_path):
    path = tmp_path / "history.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    value = {"role": "tool", "content": "answer", "tool_call_id": "reused"}
    prompt = {"role": "user", "content": "continue"}
    try:
        source = decision(recorder)
        fact = annotation(binding(source.event_id))
        first = submit(recorder, [value], [fact], fields={"temperature": 0, "model": "first-model"})
        later = submit(recorder, [prompt, value, value], [annotation(), fact, fact],
                       fields={"temperature": 1, "model": "other-model"})
        compacted = submit(recorder, [{"role": "user", "content": "summary"}], [annotation()])
    finally:
        recorder.close()
    with closing(sqlite3.connect(path)) as db:
        db.row_factory = sqlite3.Row
        first_members, later_members = members(db, first.event_id), members(db, later.event_id)
        assert len(first_members) == 1 and len(later_members) == 3, "the observed one-message and three-message requests must retain all array members"
        check.equal([row["message_artifact_id"] for row in later_members[1:]], [first_members[0]["message_artifact_id"]] * 2,
                    msg="same immutable message/binding facts retain one handle despite request fields and shifted duplicate occurrences")
        rows = db.execute("SELECT * FROM answer_occurrences ORDER BY request_sequence,message_index").fetchall()
        check.equal([(row["request_event_id"], row["target_pointer"], row["binding_witness_event_id"]) for row in rows],
                    [(first.event_id, "/messages/0/content", first.event_id), (later.event_id, "/messages/1/content", first.event_id),
                     (later.event_id, "/messages/2/content", first.event_id)],
                    msg="every occurrence rebases its pointer while retaining the first committed witness")
        links = db.execute("SELECT * FROM links WHERE relation='answers'").fetchall()
        assert len(links) == 1, "repeated membership cannot duplicate the immutable answer binding"
        detail = json.loads(links[0]["details_json"])
        stable = {key: value for key, value in detail.items() if key not in ("binding_format", "message_artifact_id", "relative_pointer")}
        raw_binding = {"decision_event_id": source.event_id, "pointer": "/content", "details": stable}
        normalized_binding = json.loads(json.dumps(raw_binding, sort_keys=True, separators=(",", ":"), allow_nan=False))
        encoded = json.dumps([first_members[0]["message_artifact_id"], normalized_binding], ensure_ascii=False,
                             separators=(",", ":"), allow_nan=False).encode()
        expected_link_id = recorder.capture_scope().run_id + ":b:" + hashlib.sha256(encoded).hexdigest()
        check.equal(links[0]["link_id"], expected_link_id, msg="answer binding keeps the specified request-independent reference encoding")
        check.equal(db.execute("SELECT count(*) FROM answer_occurrences WHERE request_event_id=?", (compacted.event_id,)).fetchone()[0],
                    0, msg="compacted history must not inherit removed answer occurrences")


def test_public_forty_request_history_shares_payload_and_preserves_occurrences(tmp_path):
    path = tmp_path / "forty-requests.sqlite"
    coverage = {"producers": {"model": "probe.model", "host": "probe.host", "mcp": "probe.mcp"},
                "model_complete": True}
    recorder = Recorder(path, capture="content", coverage=coverage)
    history, annotations, requests = [], [], []
    repeated_bytes = 0
    try:
        with recorder.turn("bounded history sharing") as turn:
            accept(turn.receipt)
            for index in range(40):
                payload = {"model": "fixture", "system": "s" * 12000,
                           "tools": [{"name": "lookup", "description": "c" * 20000}],
                           "messages": history}
                # Measure this submitted body before later calls grow its history.
                repeated_bytes += len(json.dumps(payload, ensure_ascii=False, allow_nan=False,
                                                 separators=(",", ":")).encode("utf-8"))
                request = observe(recorder, "model.request", payload, messages=annotations,
                                  field_paths=["/system", "/tools"])
                requests.append(request)
                assert not request.losses, "This sharing fixture requires full retained content."
                choices = [] if index == 39 else [
                    observe(recorder, "model.tool_call", {"arguments": {}}, name="lookup",
                            model_call_id=request.model_call_id) for _ in range(2)
                ]
                observe(recorder, "model.response", {}, model_call_id=request.model_call_id)
                if choices:
                    history.append({"role": "assistant", "content": "two calls"})
                    annotations.append({"bindings": []})
                    for offset, choice in enumerate(choices):
                        history.append({"role": "tool", "tool_call_id": "call-" + str(2 * index + offset),
                                        "content": "r" * 3000})
                        annotations.append({"bindings": [{"decision_event_id": choice.event_id,
                            "pointer": "/content", "result_event_id": None, "text_paths": [""]}]})
                # This tests representation sharing, not producer/writer throughput.
                # Bound each complete round so queue pressure cannot alter retention.
                with recorder._condition:
                    assert recorder._condition.wait_for(
                        lambda: recorder._ordinary_slots == 0 and not recorder._pending,
                        timeout=5), "The sharing fixture writer did not drain."
        accept(turn.exit_receipt)
    finally:
        closed = recorder.close()
    assert closed.writer_stopped and closed.portable, "The complete public history must be readable as a stopped standalone recording."
    assert closed.run_close_state == "clean", "The sharing fixture must retain complete evidence."
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        check.equal(db.execute("SELECT count(*) FROM events WHERE kind='model.request'").fetchone()[0], 40,
                    msg="All forty submitted request observations must survive capture.")
        check.equal(db.execute("SELECT count(*) FROM events").fetchone()[0], 158,
                    msg="Forty request/response pairs and seventy-eight decisions retain every event.")
        check.equal(db.execute("SELECT count(*) FROM links WHERE relation='answers'").fetchone()[0], 78,
                    msg="Repeated history shares each first answer binding without adding another link per occurrence.")
        check.equal(db.execute("SELECT count(*) FROM request_members").fetchone()[0], 2340,
                    msg="Payload sharing must retain every submitted message occurrence.")
        check.equal(db.execute("SELECT count(*) FROM answer_occurrences WHERE association_state='linked'").fetchone()[0], 1560,
                    msg="Repeated answers remain visible at every submitted history position.")
        check.equal([db.execute("SELECT count(*) FROM request_members WHERE request_event_id=?",
                                (request.event_id,)).fetchone()[0] for request in requests],
                    [3 * index for index in range(40)],
                    msg="Each request preserves its own exact history size, including the initially empty request.")
        retained_bytes = db.execute("SELECT sum(stored_bytes) FROM artifacts").fetchone()[0]
        check.less(retained_bytes * 4, repeated_bytes,
                   msg="Stored shared artifacts must occupy less than one quarter of the actual submitted request bytes.")


def test_first_committed_witness_follows_admission_not_preparation(tmp_path, monkeypatch):
    path = tmp_path / "preparation.sqlite"
    prepared, release = threading.Event(), threading.Event()
    original = Recorder._admit

    def pause_earlier_preparation(self, preparation, bundle, *, turn_update=False):
        if threading.current_thread().name == "prepared-first" and bundle.kind == "model.request":
            prepared.set()
            if not release.wait(10):
                raise RuntimeError("test-owned admission gate was not released")
        return original(self, preparation, bundle, turn_update=turn_update)

    monkeypatch.setattr(Recorder, "_admit", pause_earlier_preparation)
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    receipts, failures = [], []
    try:
        source = decision(recorder)
        scope = recorder.capture_scope()
        value = {"messages": [{"content": "answer"}]}
        facts = {"messages": [annotation(binding(source.event_id))]}

        def earlier():
            try:
                receipts.append(recorder.observe("model.request", scope=scope, payload=value, facts=facts))
            except BaseException as error:
                failures.append(error)

        worker = threading.Thread(target=earlier, name="prepared-first")
        worker.start()
        assert prepared.wait(5), "earlier producer must finish preparation before the later request admits"
        later = accept(recorder.observe("model.request", scope=scope, payload=value, facts=facts))
        release.set()
        worker.join(5)
        assert not worker.is_alive(), "test-owned producer must finish after admission gate release"
        assert not failures, f"controlled capture thread must preserve host execution: {failures}"
        assert len(receipts) == 1, "the earlier producer must return exactly one capture receipt"
        earlier_receipt = accept(receipts[0])
        for receipt in (later, earlier_receipt):
            observe(recorder, "model.response", {}, model_call_id=receipt.model_call_id)
    finally:
        release.set()
        if "worker" in locals():
            worker.join(5)
        recorder.close()
    with closing(sqlite3.connect(path)) as db:
        rows = db.execute("SELECT request_event_id,binding_witness_event_id FROM answer_occurrences ORDER BY request_sequence").fetchall()
        check.equal(rows, [(later.event_id, later.event_id), (earlier_receipt.event_id, later.event_id)],
                    msg="first FIFO-admitted/committed request owns the witness regardless of preparation order")
        check.equal(db.execute("SELECT count(*) FROM links WHERE relation='answers'").fetchone()[0], 1,
                    msg="producers carry independent candidates and writer-side dedup commits one binding")


def test_unknown_earlier_message_does_not_gain_later_binding(tmp_path):
    path = tmp_path / "unknown.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    value = {"content": "answer"}
    try:
        source = decision(recorder)
        early = submit(recorder, [value], None)
        known = submit(recorder, [value], [annotation(binding(source.event_id))])
        again_unknown = submit(recorder, [value], None)
    finally:
        recorder.close()
    with closing(sqlite3.connect(path)) as db:
        db.row_factory = sqlite3.Row
        handles = [members(db, receipt.event_id)[0]["message_artifact_id"] for receipt in (early, known, again_unknown)]
        check.equal(len(set(handles)), 3, msg="unavailable binding context gets a fresh occurrence identity every time")
        rows = db.execute("SELECT request_event_id,source_event_id FROM answer_occurrences").fetchall()
        check.equal([tuple(row) for row in rows], [(known.event_id, source.event_id)],
                    msg="later explicit provenance cannot retroactively attribute unknown earlier history")
        for receipt in (early, again_unknown):
            details = json.loads(db.execute("SELECT details_json FROM events WHERE event_id=?", (receipt.event_id,)).fetchone()[0])
            check.equal(details.get("message_identity_unavailable"),
                        [{"message_index": 0, "reason": "binding_context_unavailable"}],
                        msg="each unavailable binding context preserves its exact occurrence and identity reason")
        known_details = json.loads(db.execute("SELECT details_json FROM events WHERE event_id=?", (known.event_id,)).fetchone()[0])
        check.is_not_in("message_identity_unavailable", known_details,
                        msg="the known request cannot inherit an unavailable-binding diagnostic from its unknown neighbors")


def test_mixed_known_and_unresolved_blocks_keep_the_known_evidence(tmp_path):
    path = tmp_path / "mixed.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    value = {"content": [{"text": "one"}, {"text": "two"}]}
    try:
        one, two = decision(recorder), decision(recorder)
        known = binding(one.event_id, "/content/0/text")
        mixed = submit(recorder, [value], [annotation(known, unresolved=[{
            "pointer": "/content/1/text", "reason": "ambiguous_provider_id", "provider_ref": "reused"}])])
        resolved = submit(recorder, [value], [annotation(known, binding(two.event_id, "/content/1/text"))])
        same_decision = submit(recorder, [value], [annotation(known, binding(one.event_id, "/content/1/text"))])
    finally:
        recorder.close()
    with closing(sqlite3.connect(path)) as db:
        rows = db.execute("SELECT request_event_id,source_event_id,target_pointer FROM answer_occurrences ORDER BY request_sequence,target_pointer").fetchall()
        check.equal(rows, [(mixed.event_id, one.event_id, "/messages/0/content/0/text"),
                          (resolved.event_id, one.event_id, "/messages/0/content/0/text"),
                          (resolved.event_id, two.event_id, "/messages/0/content/1/text"),
                          (same_decision.event_id, one.event_id, "/messages/0/content/0/text"),
                          (same_decision.event_id, one.event_id, "/messages/0/content/1/text")],
                    msg="known blocks survive unresolved siblings and each relative block has its own association")
        details = json.loads(db.execute("SELECT details_json FROM events WHERE event_id=?", (mixed.event_id,)).fetchone()[0])
        check.equal(details.get("unresolved_answers"), [{"pointer": "/messages/0/content/1/text",
                    "reason": "ambiguous_provider_id", "provider_ref": "reused"}],
                    msg="mixed-message ambiguity preserves the exact supplied reason, reference and submitted location")
        mixed_handle = db.execute("SELECT message_artifact_id FROM request_members WHERE request_event_id=?", (mixed.event_id,)).fetchone()[0]
        resolved_handle = db.execute("SELECT message_artifact_id FROM request_members WHERE request_event_id=?", (resolved.event_id,)).fetchone()[0]
        check.not_equal(mixed_handle, resolved_handle,
                        msg="resolving the second block changes the message's actual immutable binding identity")


@pytest.mark.parametrize("endpoint", ["missing", "wrong_kind"], ids=["missing-decision", "wrong-kind-decision"])
def test_missing_or_wrong_kind_endpoint_stays_symbolic_and_unknown(tmp_path, endpoint):
    path = tmp_path / "symbolic.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    try:
        source = "symbolic-missing"
        if endpoint == "wrong_kind":
            source = observe(recorder, "host.tool_result", "prepared").event_id
        value = {"content": "answer"}
        first = submit(recorder, [value], [annotation(binding(source))])
        second = submit(recorder, [value], [annotation(binding(source))])
    finally:
        recorder.close()
    with closing(sqlite3.connect(path)) as db:
        db.row_factory = sqlite3.Row
        first_handle, second_handle = (members(db, receipt.event_id)[0]["message_artifact_id"] for receipt in (first, second))
        check.not_equal(first_handle, second_handle, msg="unvalidated explicit decision endpoints cannot create reusable known message identity")
        rows = db.execute("SELECT source_event_id,association_state FROM answer_occurrences ORDER BY request_sequence").fetchall()
        check.equal([tuple(row) for row in rows], [(source, "missing_decision"), (source, "missing_decision")],
                    msg="symbolic endpoints remain visible without inventing valid decision provenance")


def test_same_provider_reference_cannot_merge_distinct_decision_occurrences(tmp_path):
    path = tmp_path / "provider-reference.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    value = {"content": "same", "tool_call_id": "reused"}
    try:
        one, two = decision(recorder), decision(recorder)
        first = submit(recorder, [value], [annotation(binding(one.event_id))])
        second = submit(recorder, [value], [annotation(binding(two.event_id))])
    finally:
        recorder.close()
    with closing(sqlite3.connect(path)) as db:
        db.row_factory = sqlite3.Row
        check.not_equal(members(db, first.event_id)[0]["message_artifact_id"], members(db, second.event_id)[0]["message_artifact_id"],
                        msg="recorder occurrence identity, not reused provider reference or equal bytes, binds answers")
        check.equal([tuple(row) for row in db.execute("SELECT request_event_id,source_event_id FROM answer_occurrences ORDER BY request_sequence")],
                    [(first.event_id, one.event_id), (second.event_id, two.event_id)], msg="each explicit decision remains distinguishable")


def test_retention_variants_do_not_reuse_incompatible_handles(tmp_path):
    path = tmp_path / "retained-handles.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    original = {"content": "private"}
    try:
        source = decision(recorder)
        annotations = [annotation(binding(source.event_id))]
        full = submit(recorder, [original], annotations)
        omitted = submit(recorder, [original], annotations, retention={"policy_id": "remove", "omitted_reason": "policy_removed"})
        redacted = submit(recorder, [original], annotations, retention={"policy_id": "mask", "retained": {"messages": [{"content": "masked"}]}, "redacted": True})
        repeat = submit(recorder, [original], annotations)
    finally:
        recorder.close()
    with closing(sqlite3.connect(path)) as db:
        db.row_factory = sqlite3.Row
        handles = [members(db, receipt.event_id)[0]["message_artifact_id"] for receipt in (full, omitted, redacted, repeat)]
        check.equal(len(set(handles[:3])), 3, msg="retained bytes, policy and loss flags participate in message handle identity")
        check.equal(handles[0], handles[3], msg="an identical compatible retention observation reuses its original known handle")
        rows = {row["artifact_id"]: row for row in db.execute("SELECT * FROM artifacts WHERE artifact_id IN (?,?,?)", handles[:3])}
        check.equal(json.loads(rows[handles[0]]["body_text"]), {"content": "private"},
                    msg="the full-content variant retains the literal original while other policies remain distinct")
        check.is_none(rows[handles[1]]["body_text"], msg="omitted variant does not silently inherit the original retained body")
        check.equal(json.loads(rows[handles[2]]["body_text"]), {"content": "masked"}, msg="redacted variant preserves its own retained bytes")
        check.equal(rows[handles[2]]["redacted"], 1, msg="redaction stays visible independently of original comparison identity")


def test_disabled_comparisons_use_fresh_handles_without_capture_loss(tmp_path):
    path = tmp_path / "disabled-comparisons.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE, comparisons=False)
    try:
        with recorder.turn("disabled comparisons"):
            source = decision(recorder)
            first = submit(recorder, [{"content": "answer"}], [annotation(binding(source.event_id))])
            second = submit(recorder, [{"content": "answer"}], [annotation(binding(source.event_id))])
    finally:
        closed = recorder.close()
    with closing(sqlite3.connect(path)) as db:
        db.row_factory = sqlite3.Row
        check.not_equal(members(db, first.event_id)[0]["message_artifact_id"], members(db, second.event_id)[0]["message_artifact_id"],
                        msg="disabled original comparison identity cannot reuse handles by retained content alone")
        check.equal(db.execute("SELECT count(*) FROM answer_occurrences").fetchone()[0], 2,
                    msg="disabling comparison does not erase explicit provenance occurrences")
        for receipt in (first, second):
            details = json.loads(db.execute("SELECT details_json FROM events WHERE event_id=?", (receipt.event_id,)).fetchone()[0])
            check.equal(details.get("message_identity_unavailable"), [{"message_index": 0, "reason": "disabled"}],
                        msg="both comparison-disabled occurrences retain the exact disabled identity reason")
    check.equal(closed.run_close_state, "clean", msg="comparison unavailability alone is not observation loss")
    check.equal(recorder.health().dropped_events, 0, msg="comparison opt-out does not drop admitted observation envelopes")


def test_duplicate_binding_location_is_rejected_without_request_identity(tmp_path):
    path = tmp_path / "duplicate-location.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    try:
        one, two = decision(recorder), decision(recorder)
        receipt = recorder.observe("model.request", scope=recorder.capture_scope(), payload={"messages": [{"content": "answer"}]},
                                   facts={"messages": [annotation(binding(one.event_id), binding(two.event_id))]})
    finally:
        recorder.close()
    check.equal((receipt.status, receipt.reason), ("rejected", "invalid_input"),
                msg="one relative block location cannot claim two resolved decision bindings")
    check.is_none(receipt.event_id, msg="rejected ambiguous binding request publishes no event prerequisite")
    check.is_none(receipt.model_call_id, msg="rejected ambiguous binding request publishes no model-call prerequisite")


def test_explicit_result_attribution_is_part_of_message_identity(tmp_path, monkeypatch):
    path = tmp_path / "result-attribution.sqlite"
    armed, entered, release = threading.Event(), threading.Event(), threading.Event()
    original = Recorder._persist_bundle

    def hold_first_message_request(self, connection, bundle):
        if armed.is_set() and bundle.kind == "model.request" and not entered.is_set():
            entered.set()
            if not release.wait(10):
                raise RuntimeError("test-owned result-attribution barrier was not released")
        return original(self, connection, bundle)

    monkeypatch.setattr(Recorder, "_persist_bundle", hold_first_message_request)
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    try:
        source = decision(recorder)
        results = []
        for _ in range(2):
            start = accept(recorder.start_operation(scope=recorder.capture_scope(), client_id="c", method="tools/call",
                arguments={}, decision_id=source.decision_id, link_basis="propagated"))
            results.append(accept(recorder.finish_operation(start.ticket, status="ok", result="answer", text_paths=[""])))
        # Drain every prerequisite before arming, so the selected request is the
        # message submission whose caller-owned annotation will be mutated.
        with recorder._condition:
            assert recorder._condition.wait_for(lambda: recorder.health().ordinary.used_slots == 0
                and recorder.health().completion.used_slots == 0, timeout=5), \
                "all prerequisite requests and results must commit before the message-only persistence barrier is armed"
        caller_binding = binding(source.event_id, result_event=results[0].event_id)
        caller_annotation = annotation(caller_binding)
        armed.set()
        first = submit(recorder, [{"content": "answer"}], [caller_annotation])
        assert entered.wait(5), "the accepted first message request must be held before its binding persists"
        caller_binding["result_event_id"] = results[1].event_id
        second = submit(recorder, [{"content": "answer"}], [caller_annotation])
    finally:
        release.set()
        recorder.close()
    with closing(sqlite3.connect(path)) as db:
        db.row_factory = sqlite3.Row
        check.not_equal(members(db, first.event_id)[0]["message_artifact_id"], members(db, second.event_id)[0]["message_artifact_id"],
                        msg="equal message text cannot erase the explicitly named result occurrence")
        rows = db.execute("SELECT details_json FROM answer_occurrences ORDER BY request_sequence").fetchall()
        check.equal([json.loads(row[0])["result_event_id"] for row in rows], [result.event_id for result in results],
                    msg="each answer retains its explicitly supplied result-event attribution")


@pytest.mark.parametrize("variant", ["request_partial", "message_partial", "message_oversized"],
                         ids=["partial-request", "partial-message", "oversized-message"])
def test_unavailable_original_message_identity_never_reuses_handles(tmp_path, variant):
    path = tmp_path / "unavailable-identity.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    try:
        source = decision(recorder)
        value = {"content": "x" * 1_048_577 if variant == "message_oversized" else "answer"}
        annotations = [annotation(binding(source.event_id))]
        if variant == "message_partial":
            annotations[0]["complete"] = False
        options = {"complete": False} if variant == "request_partial" else {}
        first = submit(recorder, [value], annotations, **options)
        second = submit(recorder, [value], annotations, **options)
    finally:
        recorder.close()
    with closing(sqlite3.connect(path)) as db:
        db.row_factory = sqlite3.Row
        check.not_equal(members(db, first.event_id)[0]["message_artifact_id"], members(db, second.event_id)[0]["message_artifact_id"],
                        msg="partial or over-limit original projection must use a fresh message occurrence identity")
        check.equal(db.execute("SELECT count(*) FROM answer_occurrences").fetchone()[0], 2,
                    msg="unavailable reusable identity still preserves known explicit bindings")
        for receipt in (first, second):
            detail = json.loads(db.execute("SELECT details_json FROM events WHERE event_id=?", (receipt.event_id,)).fetchone()[0])
            reason = "capture_limit" if variant == "message_oversized" else "original_incomplete"
            check.equal(detail.get("message_identity_unavailable"), [{"message_index": 0, "reason": reason}],
                        msg="each partial or oversized occurrence preserves its exact original-identity limitation")


def test_same_size_omitted_originals_with_one_binding_keep_distinct_identity(tmp_path):
    path = tmp_path / "same-binding-metadata.sqlite"
    recorder = Recorder(path, capture="metadata", coverage=COVERAGE)
    one, two = {"content": "xxxx"}, {"content": "yyyy"}
    try:
        source = decision(recorder)
        fact = {"bindings": [{"decision_event_id": source.event_id, "pointer": "/content",
                             "result_event_id": None, "text_paths": []}],
                "unresolved": [], "complete": True}
        first = submit(recorder, [one, two], [fact, fact])
        repeat = submit(recorder, [one], [fact])
    finally:
        closed = recorder.close()
    with closing(sqlite3.connect(path)) as db:
        db.row_factory = sqlite3.Row
        first_members, repeated_members = members(db, first.event_id), members(db, repeat.event_id)
        assert len(first_members) == 2 and len(repeated_members) == 1, \
            "both same-size originals and the repeated occurrence must persist before their identity is compared"
        check.equal([(row["message_index"], row["message_path"], row["artifact_missing"]) for row in first_members],
                    [(0, "/messages", 0), (1, "/messages", 0)],
                    msg="metadata capture preserves both ordered present-artifact memberships")
        handles = [row["message_artifact_id"] for row in first_members]
        check.not_equal(handles[0], handles[1],
                        msg="different equal-size originals retain distinct handles with exactly the same binding evidence")
        check.equal(repeated_members[0]["message_artifact_id"], handles[0],
                    msg="an exact omitted original and binding reuse their known handle on the next request")
        stored = db.execute("""SELECT a.body_text,a.stored_sha256,a.stored_bytes,a.observed_bytes,a.omitted_reason
            FROM request_members m JOIN artifacts a
            ON a.run_id=m.run_id AND a.artifact_id=m.message_artifact_id
            WHERE m.request_event_id IN (?,?) ORDER BY m.request_sequence,m.message_index""",
            (first.event_id, repeat.event_id)).fetchall()
        check.equal([tuple(row) for row in stored], [(None, None, 0, 18, "capture_metadata")] * 3,
                    msg="all three occurrences retain exact original size and omission metadata without original bytes")
        links = db.execute("SELECT details_json FROM links WHERE relation='answers' ORDER BY link_id").fetchall()
        assert len(links) == 2, "two original message identities require two first-binding rows and no repeat-specific binding"
        tokens = [json.loads(row[0])["submitted_text_digest"] for row in links]
        assert all(type(token) is dict for token in tokens), "both bindings must expose comparison dictionaries before equality is meaningful"
        check.equal(tokens[0], tokens[1],
                    msg="the empty observed text projection makes binding evidence constant across the two different originals")
        occurrences = db.execute("""SELECT request_event_id,message_index,source_event_id,target_pointer,association_state
            FROM answer_occurrences ORDER BY request_sequence,message_index""").fetchall()
        check.equal([tuple(row) for row in occurrences],
                    [(first.event_id, 0, source.event_id, "/messages/0/content", "linked"),
                     (first.event_id, 1, source.event_id, "/messages/1/content", "linked"),
                     (repeat.event_id, 0, source.event_id, "/messages/0/content", "linked")],
                    msg="metadata-only original identity keeps all explicit answer occurrences and their actual positions")
        for receipt, expected in ((first, handles), (repeat, [handles[0]])):
            manifest_row = db.execute("""SELECT a.body_text FROM events e JOIN artifacts a
                ON a.run_id=e.run_id AND a.artifact_id=e.payload_ref WHERE e.event_id=?""", (receipt.event_id,)).fetchone()
            assert manifest_row is not None and manifest_row[0] is not None, \
                "structural manifests must remain retained so ordered metadata memberships can be checked"
            check.equal(json.loads(manifest_row[0])["message_refs"], expected,
                        msg="each structural manifest retains exactly its observed ordered omitted-message handles")
        dumped = "\n".join(db.iterdump())
        for secret in ("xxxx", "yyyy"):
            check.is_not_in(secret, dumped, msg="metadata capture cannot leak either distinct original into any stored text")
    check.equal(closed.run_close_state, "clean", msg="intentional metadata omission is not a lost observation")
    check.equal((recorder.health().dropped_events, recorder.health().writer_errors), (0, 0),
                msg="the constant-binding identity control must not rely on dropped envelopes or writer failures")


def test_rejected_first_admission_does_not_claim_the_binding(tmp_path, monkeypatch):
    entered, release = threading.Event(), threading.Event()
    original = Recorder._persist_bundle

    def hold_pending_decision(self, connection, bundle):
        if bundle.kind == "model.tool_call" and not entered.is_set():
            entered.set()
            if not release.wait(10):
                raise RuntimeError("test-owned pending-identity barrier was not released")
        return original(self, connection, bundle)

    monkeypatch.setattr(Recorder, "_persist_bundle", hold_pending_decision)
    path = tmp_path / "rejected-first.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE, limits={"ordinary_items": 1})
    try:
        source = observe(recorder, "model.tool_call", {"arguments": {}}, name="echo")
        assert entered.wait(5), "writer must hold the sole ordinary slot while the request prepares its binding"
        payload = {"messages": [{"content": "answer"}]}
        facts = {"messages": [annotation(binding(source.event_id))]}
        rejected = recorder.observe("model.request", scope=recorder.capture_scope(), payload=payload, facts=facts)
        check.equal((rejected.status, rejected.reason), ("rejected", "capacity"),
                    msg="request cannot admit while writer-held decision still owns the only ordinary slot")
        check.is_none(rejected.event_id, msg="capacity rejection must publish no request witness identity")
        release.set()
        # The private condition is only a deterministic wait seam; its predicate is public capacity.
        with recorder._condition:
            assert recorder._condition.wait_for(lambda: recorder.health().ordinary.used_slots == 0, timeout=5), \
                "ordinary capacity must release only after the decision commits"
        committed = accept(recorder.observe("model.request", scope=recorder.capture_scope(), payload=payload, facts=facts))
        with recorder._condition:
            assert recorder._condition.wait_for(lambda: recorder.health().ordinary.used_slots == 0, timeout=5), \
                "request commit must free its slot before submitting its complete model response"
        observe(recorder, "model.response", {}, model_call_id=committed.model_call_id)
    finally:
        release.set()
        recorder.close()
    with closing(sqlite3.connect(path)) as db:
        check.equal(db.execute("SELECT request_event_id,binding_witness_event_id FROM answer_occurrences").fetchall(),
                    [(committed.event_id, committed.event_id)],
                    msg="the next committed request must carry its own binding candidate after the first admission rejects")
        check.equal(db.execute("SELECT count(*) FROM links WHERE relation='answers'").fetchone()[0], 1,
                    msg="a rejected producer cannot leave a shared suppression claim that loses the eventual binding")
    check.equal(recorder.health().dropped_events, 1, msg="one rejected request counts one event attempt regardless of artifact/link candidates")


@pytest.mark.parametrize("invalid_annotation,outer_complete", [
    (None, True), ({"bindings": [], "complete": 0}, False),
], ids=["null-array-item", "invalid-nested-flag-under-partial-request"])
def test_invalid_annotation_shape_and_boolean_are_rejected(tmp_path, invalid_annotation, outer_complete):
    path = tmp_path / "invalid-annotations.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    value = {"content": "observed"}
    try:
        # Both documented forms of unavailable binding context remain valid controls.
        submit(recorder, [value], None)
        submit(recorder, [value], [{"bindings": None}])
        rejected = recorder.observe("model.request", scope=recorder.capture_scope(), payload={"messages": [value]},
                                    facts={"messages": [invalid_annotation], "complete": outer_complete})
    finally:
        recorder.close()
    check.equal((rejected.status, rejected.reason), ("rejected", "invalid_input"),
                msg="explicit annotation items must be objects with a boolean complete flag even when the request is already partial")
    check.is_none(rejected.model_call_id, msg="invalid annotation input cannot publish a model-call identity")
    with closing(sqlite3.connect(path)) as db:
        check.equal(db.execute("SELECT count(*) FROM events WHERE kind='model.request'").fetchone()[0], 2,
                    msg="whole-context null and bindings=null remain accepted while the malformed third request leaves no event")
