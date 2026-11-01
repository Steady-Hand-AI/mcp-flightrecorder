"""P2.5–P2.7: literal ancestry sets, context upgrades, cycles and exact bounds."""
from contextlib import closing
import hashlib
import json
import shutil
import sqlite3

import pytest
import pytest_check as check

from mcp_flightrecorder import Recorder
from mcp_flightrecorder import inspect as reader

from .recordings import integration_recording


COVERAGE = {"producers": {"model": "slice.model", "host": "slice.host", "mcp": "slice.mcp"}, "model_complete": True}
TABLES = ("runs", "turns", "events", "operations", "links", "artifacts", "members", "answers")
EXPECTED_EVENTS = {
    "R0": {"R0"},
    "W1": {"R0", "D1", "D2", "Z0", "H1", "S1", "H2", "S2", "E2", "E1", "P", "W1"},
    "W2": {"R0", "D1", "D2", "Z0", "H1", "S1", "H2", "S2", "E2", "E1", "W1", "W2"},
    "W3": {"R0", "D1", "D2", "Z0", "H1", "S1", "H2", "S2", "E2", "E1", "W1", "W3"},
}
EXPECTED_LINKS = {
    ("dispatch_source", "D1", "H1", "H1", None, None, None, None),
    ("dispatch_source", "D2", "H2", "H2", None, None, None, None),
    ("answers", "D1", "W1", "W1", None, None, "payload", "/messages/3/content"),
    ("answers", "D2", "W1", "W1", None, None, "payload", "/messages/4/content"),
    ("transformed_from", "E1", "P", "P", "payload", "/content/0/text", None, None),
    ("transformed_from", "E2", "P", "P", "payload", "/content/0/text", None, None),
    ("inserted_into", "P", "W1", "W1", "payload", "", "payload", "/messages/5/content"),
}


@pytest.fixture
def recording(tmp_path):
    path = tmp_path / "slice-input-copy.sqlite"
    source, symbols = integration_recording()
    shutil.copyfile(source, path)
    return path, symbols


def successful(value):
    assert value["ok"], f"Graph prerequisites must complete before semantic assertions: {value.get('error')}"
    return value["data"]


def observed(recorder, kind, payload, **facts):
    receipt = recorder.observe(kind, scope=recorder.capture_scope(), payload=payload, facts=facts)
    assert receipt.status == "accepted", f"Public graph control requires admitted {kind}: {receipt.status}/{receipt.reason}"
    return receipt


def closed(recorder):
    result = recorder.close()
    assert result.writer_stopped and result.portable, "Public graph input must be stopped and portable before lone-file reads."


def count_records(data):
    return sum(len(data[name]) for name in TABLES)


def identities_from_literal_graph(path, symbols, root_name):
    """Read allocated artifact identities only after the literal graph is fixed.

    This does not traverse candidate output or invoke its graph helpers. Event
    membership comes solely from EXPECTED_EVENTS; manifest references expose
    the allocated IDs for the independently fixed 2/6/8/10 member populations.
    """
    event_ids = [symbols[name] for name in EXPECTED_EVENTS[root_name]]
    artifacts, members = set(), []
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as connection:
        for event_id in event_ids:
            payload, catalog, turn = connection.execute("SELECT payload_ref,catalog_ref,turn_id FROM events WHERE event_id=?", (event_id,)).fetchone()
            artifacts.update(value for value in (payload, catalog) if value)
            prompt = connection.execute("SELECT prompt_ref FROM turns WHERE turn_id=?", (turn,)).fetchone()[0]
            if prompt:
                artifacts.add(prompt)
        for name in ("R0", "W1", "W2", "W3"):
            if name not in EXPECTED_EVENTS[root_name]:
                continue
            text = connection.execute("SELECT a.body_text FROM events e JOIN artifacts a ON a.artifact_id=e.payload_ref WHERE e.event_id=?", (symbols[name],)).fetchone()[0]
            manifest = json.loads(text)
            expected_count = {"R0": 2, "W1": 6, "W2": 8, "W3": 10}[name]
            assert len(manifest["message_refs"]) == expected_count, "The accepted fixture must preserve its independent literal request-member count."
            artifacts.add(manifest["template_ref"])
            artifacts.update(manifest["message_refs"])
            artifacts.update(field["artifact_id"] for field in manifest["field_refs"])
            members.extend((symbols[name], index, identity) for index, identity in enumerate(manifest["message_refs"]))
    return artifacts, members


@pytest.mark.parametrize("root_name", ["R0", "W1", "W2", "W3"])
def test_canonical_four_exact_graphs_members_and_current_answers(recording, root_name):
    path, symbols = recording
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    data = successful(reader.slice(path, symbols["R"], symbols[root_name], limit=1000, max_depth=12, max_output_bytes=1048576))
    reverse = {value: name for name, value in symbols.items() if type(value) is str}
    check.equal({reverse[row["event_id"]] for row in data["events"]}, EXPECTED_EVENTS[root_name], msg="Submitted-input ancestry must match the literal canonical graph, excluding discovery and future outputs.")
    check.equal({reverse[row["operation_id"]] for row in data["operations"]}, set() if root_name == "R0" else {"O1", "O2"}, msg="Only explicitly reached tool operations belong in ancestry; same-server discovery does not.")
    relations = {tuple(reverse.get(row[name], row[name]) for name in ("relation", "source_event_id", "target_event_id", "witness_event_id", "source_slot", "source_pointer", "target_slot", "target_pointer")) for row in data["links"]}
    expected_links = set() if root_name == "R0" else EXPECTED_LINKS if root_name == "W1" else {row for row in EXPECTED_LINKS if row[0] in {"dispatch_source", "answers"}}
    check.equal(relations, expected_links, msg="Only W1 has the actual prepared-context insertion; later witness context does not re-expand historical insertions.")
    artifacts, members = identities_from_literal_graph(path, symbols, root_name)
    check.equal({row["artifact_id"] for row in data["artifacts"]}, artifacts, msg="Each included request/catalog/prompt and every declared manifest part must expose its metadata without bodies.")
    check.equal([(row["request_event_id"], row["message_index"], row["message_artifact_id"]) for row in data["members"]], members, msg="Membership preserves chronological request order and every position, including contextual witnesses.")
    expected_answers = [] if root_name == "R0" else [(symbols["D1"], "/messages/3/content" if root_name == "W1" else "/messages/4/content", True), (symbols["D2"], "/messages/4/content" if root_name == "W1" else "/messages/5/content", False)]
    check.equal([(row["source_event_id"], row["target_pointer"], row["text_equal"]) for row in data["answers"]], expected_answers, msg="Only full root request occurrences are returned, with exact current pointers and independent original-text comparisons.")
    check.is_true(all(row["request_event_id"] == symbols[root_name] and row["binding_witness_event_id"] == symbols["W1"] for row in data["answers"]), msg="The root's current occurrence keeps its original W1 witness without adding witness answers to output.")
    check.equal((data["complete"], data["frontier"]), (True, []), msg="The complete canonical graph has neither unresolved required evidence nor traversal cutoffs.")
    check.is_true(all("body_text" not in row for row in data["artifacts"]) and all("details_json" not in row for row in data["events"] + data["links"]), msg="Default slice output contains allowlisted metadata rather than retained bodies or raw detail JSON.")
    check.equal(hashlib.sha256(path.read_bytes()).hexdigest(), before, msg="An ancestry read cannot alter the accepted graph, its rows or SQLite bytes.")


def make_context_graph(path, *, upgrade):
    """Public graph whose explicit extra edge upgrades an earlier witness."""
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    symbols = {"R": recorder.capture_scope().run_id}
    try:
        with recorder.turn("context-only sibling control"):
            initial = observed(recorder, "model.request", {"messages": []}, messages=[])
            symbols["R0"] = initial.event_id
            decisions = [observed(recorder, "model.tool_call", {"arguments": {}}, name="same-name", model_call_id=initial.model_call_id) for _ in range(2)]
            symbols.update(DA=decisions[0].event_id, DB=decisions[1].event_id)
            symbols["Z"] = observed(recorder, "model.response", {}, model_call_id=initial.model_call_id).event_id
            results = []
            for index, text in enumerate(("answer A", "answer B")):
                start = recorder.start_operation(scope=recorder.capture_scope(), client_id="public", server_key="same-server", method="tools/call", name="same-name", arguments={}, decision_id=decisions[index].decision_id, link_basis="propagated")
                assert start.status == "accepted", "Context control requires explicit operation attribution, not name matching."
                end = recorder.finish_operation(start.ticket, status="ok", result={"text": text}, text_paths=["/text"])
                assert end.status == "accepted", "Context control requires terminal result identities."
                symbols.update({"OA" if index == 0 else "OB": start.operation_id,
                                "SA" if index == 0 else "SB": start.event_id,
                                "EA" if index == 0 else "EB": end.event_id})
                results.append(end)
            prepared = {"summary": "prepared B"}
            q = observed(recorder, "host.tool_result", prepared, sources=[{"event_id": results[1].event_id}])
            symbols["Q"] = q.event_id
            messages = [{"content": "answer A"}, {"content": "answer B"}, {"content": prepared}]
            bindings = [{"bindings": [{"decision_event_id": decisions[index].event_id, "result_event_id": results[index].event_id,
                                        "pointer": "/content", "text_paths": [""]}]} for index in range(2)]
            witness = observed(recorder, "model.request", {"messages": messages}, messages=[*bindings, {"bindings": []}],
                               insertions=[{"source_event_id": q.event_id, "target_pointer": "/messages/2/content", "content": prepared}])
            symbols["W"] = witness.event_id
            symbols["FW"] = observed(recorder, "model.response", {}, model_call_id=witness.model_call_id).event_id
            prepared_w = {"summary": "prepared W"}
            p = observed(recorder, "host.tool_result", prepared_w, sources=[{"event_id": witness.event_id, "pointer": "/messages/0/content"}])
            symbols["P"] = p.event_id
            root = observed(recorder, "model.request", {"messages": [messages[0], {"content": prepared_w}]},
                            messages=[bindings[0], {"bindings": []}],
                            insertions=[{"source_event_id": p.event_id, "target_pointer": "/messages/1/content", "content": prepared_w}] if upgrade else [])
            symbols["ROOT"] = root.event_id
            symbols["FR"] = observed(recorder, "model.response", {}, model_call_id=root.model_call_id).event_id
    finally:
        closed(recorder)
    return symbols


@pytest.mark.parametrize("upgrade", [False, True], ids=["context-only", "later-full-upgrade"])
def test_contextual_witness_excludes_siblings_until_full_edge(tmp_path, upgrade):
    path = tmp_path / "context.sqlite"
    symbols = make_context_graph(path, upgrade=upgrade)
    data = successful(reader.slice(path, symbols["R"], symbols["ROOT"], max_output_bytes=1048576))
    baseline = {"R0", "DA", "Z", "SA", "EA", "W", "ROOT"}
    expected = baseline | ({"DB", "SB", "EB", "Q", "P"} if upgrade else set())
    check.equal({row["event_id"] for row in data["events"]}, {symbols[name] for name in expected}, msg="The same witness metadata expands its sibling answer only after an actual permitted incoming edge upgrades its context visit.")
    check.equal({row["operation_id"] for row in data["operations"]}, {symbols["OA"], symbols["OB"]} if upgrade else {symbols["OA"]}, msg="Shared server/name and contextual membership cannot create sibling operation ancestry.")
    check.is_false({symbols["FW"], symbols["FR"]} & {row["event_id"] for row in data["events"]}, msg="Neither a contextual request nor the root pulls its future response merely through model_call_id.")
    check.equal(len(data["answers"]), 3 if upgrade else 1, msg="Full upgrade exposes two witness request answers; context-only mode contributes its members but no historical answers.")
    check.is_true(data["complete"], msg="Both acyclic public controls have complete explicit ancestry under full limits.")


def test_repeated_answers_consume_slice_record_budget(tmp_path):
    path = tmp_path / "answer-budget.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    run_id = recorder.capture_scope().run_id
    try:
        with recorder.turn("answer bound") as turn:
            assert turn.receipt.status == "accepted", "The answer-budget control requires an admitted turn and its nonempty prompt."
            initial = observed(recorder, "model.request", {"messages": []}, messages=[])
            choice = observed(recorder, "model.tool_call", {"arguments": {}}, name="echo",
                              model_call_id=initial.model_call_id)
            observed(recorder, "model.response", {}, model_call_id=initial.model_call_id)
            root = observed(recorder, "model.request", {"messages": [{"content": "answer"}] * 20},
                            messages=[{"bindings": [{"decision_event_id": choice.event_id,
                                                       "pointer": "/content"}]}] * 20)
            observed(recorder, "model.response", {}, model_call_id=root.model_call_id)
        assert turn.exit_receipt.status == "accepted", "Turn closure must preserve all admitted answer occurrences."
    finally:
        closed(recorder)
    data = successful(reader.slice(path, run_id, root.event_id, limit=38, max_output_bytes=1048576))
    check.equal({name: len(data[name]) for name in TABLES},
                {"runs": 1, "turns": 1, "events": 1, "operations": 0, "links": 1,
                 "artifacts": 4, "members": 20, "answers": 10},
                msg="Repeated answer rows consume the same total record budget as their run, turn, event, artifacts, memberships and binding.")
    check.equal(count_records(data), 38, msg="The concrete answer-bound traversal must fill exactly its thirty-eight-record allowance.")
    check.equal([answer["target_pointer"] for answer in data["answers"]],
                ["/messages/" + str(index) + "/content" for index in range(10)],
                msg="The answer prefix retains each of the first ten ordered submitted positions.")
    check.is_false(data["complete"], msg="Unemitted answer occurrences cannot be reported as complete ancestry.")
    check.is_true(any(item["kind"] == "answer" and item["id"] == root.event_id and
                      item["reason"] == "record_limit" for item in data["frontier"]),
                  msg="The root request's omitted answers must appear as an explicit record-limit frontier.")


def test_shared_dispatch_keeps_only_consumed_operation(tmp_path):
    path = tmp_path / "shared-dispatch.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    run_id = recorder.capture_scope().run_id
    operations, terminals = [], []
    prepared_value = {"summary": "used operation result"}
    try:
        with recorder.turn("one consumed operation in a shared dispatch") as turn:
            assert turn.receipt.status == "accepted", "The shared-dispatch control requires an admitted public turn."
            initial = observed(recorder, "model.request", {"messages": []}, messages=[])
            choice = observed(recorder, "model.tool_call", {"arguments": {}}, name="lookup",
                              model_call_id=initial.model_call_id)
            response = observed(recorder, "model.response", {}, model_call_id=initial.model_call_id)
            with recorder.dispatch(action="execute", name="lookup", arguments={},
                                   decision_event_ids=[choice.event_id]) as dispatch:
                assert dispatch.receipt.status == "accepted", "Both operations must share one real admitted dispatch."
                for text in ("used", "unused"):
                    start = recorder.start_operation(scope=recorder.capture_scope(), client_id="public",
                        method="tools/call", name="lookup", arguments={},
                        link_basis="host_hook", decision_id=choice.decision_id)
                    assert start.status == "accepted", "Each operation must retain explicit attribution to the shared dispatch and decision."
                    end = recorder.finish_operation(start.ticket, status="ok", resolution="remote",
                                                    result={"text": text}, text_paths=["/text"])
                    assert end.status == "accepted", "Each operation must have a real terminal observation."
                    operations.append(start)
                    terminals.append(end)
            prepared = observed(recorder, "host.tool_result", prepared_value,
                                sources=[{"event_id": terminals[0].event_id}])
            root = observed(recorder, "model.request", {"messages": [{"content": prepared_value}]},
                            messages=[{"bindings": []}], insertions=[{
                                "source_event_id": prepared.event_id, "target_pointer": "/messages/0/content",
                                "content": prepared_value}])
            future = observed(recorder, "model.response", {}, model_call_id=root.model_call_id)
        assert turn.exit_receipt.status == "accepted", "The shared-dispatch source must close its turn without losing observations."
    finally:
        closed(recorder)
    data = successful(reader.slice(path, run_id, root.event_id, max_output_bytes=1048576))
    event_ids = {row["event_id"] for row in data["events"]}
    check.equal(event_ids, {initial.event_id, choice.event_id, response.event_id, dispatch.receipt.event_id,
                            operations[0].event_id, terminals[0].event_id, prepared.event_id, root.event_id},
                msg="The consumed result must retain exactly its producing model context, shared dispatch, own lifecycle, preparation and root request.")
    check.equal({row["operation_id"] for row in data["operations"]}, {operations[0].operation_id},
                msg="A shared dispatch and decision cannot introduce an unconsumed sibling operation.")
    check.is_false({operations[1].event_id, terminals[1].event_id, future.event_id} & event_ids,
                   msg="Neither the sibling lifecycle nor the root's future response is submitted-input ancestry.")
    check.is_true(data["complete"], msg="The consumed operation has complete explicit ancestry without its unconsumed sibling.")


def canonical_w1_entities(path, symbols, data):
    """Check the existing literal W1 graph independently of projected labels."""
    reverse = {value: name for name, value in symbols.items() if type(value) is str}
    check.equal({row["event_id"] for row in data["events"]}, {symbols[name] for name in EXPECTED_EVENTS["W1"]},
                msg="Metadata projection keeps the exact canonical W1 event ancestry.")
    check.equal({row["operation_id"] for row in data["operations"]}, {symbols["O1"], symbols["O2"]},
                msg="Metadata projection retains both canonical reached operations.")
    fields = ("relation", "source_event_id", "target_event_id", "witness_event_id", "source_slot", "source_pointer", "target_slot", "target_pointer")
    check.equal({tuple(reverse.get(row[name], row[name]) for name in fields) for row in data["links"]}, EXPECTED_LINKS,
                msg="Metadata projection preserves every literal canonical answer, dispatch and prepared-context edge.")
    artifacts, members = identities_from_literal_graph(path, symbols, "W1")
    check.equal({row["artifact_id"] for row in data["artifacts"]}, artifacts,
                msg="All canonical event, catalog, prompt and manifest artifact identities remain reached.")
    check.equal([(row["request_event_id"], row["message_index"], row["message_artifact_id"]) for row in data["members"]], members,
                msg="Repeated metadata visits retain complete chronologically ordered canonical request membership.")
    check.equal([(row["source_event_id"], row["target_pointer"], row["text_equal"], row["request_event_id"], row["binding_witness_event_id"]) for row in data["answers"]],
                [(symbols["D1"], "/messages/3/content", True, symbols["W1"], symbols["W1"]),
                 (symbols["D2"], "/messages/4/content", False, symbols["W1"], symbols["W1"])],
                msg="The two literal W1 answers keep their sources, current pointers, original comparisons and first witnesses.")


@pytest.mark.parametrize("kind", ["run", "turn"])
def test_slice_projects_retained_metadata_once(recording, kind):
    path, symbols = recording
    run_started = "s" * 1024
    run_ended = "e" * 1024
    turn_ended = "t" * 1024
    with closing(sqlite3.connect(path)) as connection:
        if kind == "run":
            connection.execute("UPDATE runs SET started_at=?,ended_at=? WHERE run_id=?", (run_started, run_ended, symbols["R"]))
        else:
            connection.execute("UPDATE turns SET ended_at=? WHERE turn_id=?", (turn_ended, symbols["T1"]))
        connection.commit()
    value = reader.slice(path, symbols["R"], symbols["W1"], max_output_bytes=1048576)
    data = successful(value)
    if kind == "run":
        check.equal([(row["run_id"], row["started_at"], row["ended_at"]) for row in data["runs"]],
                    [(symbols["R"], run_started[:512], run_ended[:512])],
                    msg="One retained run row clips each independently named display value once.")
    else:
        check.equal([(row["turn_id"], row["ended_at"]) for row in data["turns"]], [(symbols["T1"], turn_ended[:512])],
                    msg="One retained T1 row keeps its once-clipped end label through every metadata visit.")
    cutoff = {"code": "string_limit", "path": "", "ids": {}, "count": 512}
    check.equal(value["cutoffs"], [cutoff] * (2 if kind == "run" else 1),
                msg="Every overlong retained metadata field contributes exactly one display cutoff despite repeated visits.")
    check.equal(value["unknowns"], [], msg="Complete canonical metadata introduces no uncertainty during duplicate suppression.")
    check.equal(value["diagnostics_omitted"], {"unknowns": 0, "cutoffs": 0},
                msg="Duplicate metadata projection does not consume or overflow diagnostic capacity.")
    canonical_w1_entities(path, symbols, data)
    check.equal((data["complete"], data["frontier"]), (True, []),
                msg="The canonical W1 traversal remains complete after retaining metadata projections once.")


def test_slice_event_upgrade_projects_once_and_expands(tmp_path):
    path = tmp_path / "event-upgrade.sqlite"
    symbols = make_context_graph(path, upgrade=True)
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("UPDATE events SET name=? WHERE event_id=?", ("n" * 1024, symbols["W"]))
        connection.commit()
    value = reader.slice(path, symbols["R"], symbols["ROOT"], max_output_bytes=1048576)
    data = successful(value)
    names = {"R0", "DA", "DB", "Z", "SA", "SB", "EA", "EB", "W", "Q", "P", "ROOT"}
    check.equal({row["event_id"] for row in data["events"]}, {symbols[name] for name in names},
                msg="An already projected contextual witness still expands every literal ancestor after its full upgrade.")
    check.equal(len(data["events"]), 12, msg="The twelve reached events remain unique through the context-to-full revisit.")
    check.equal([row["name"] for row in data["events"] if row["event_id"] == symbols["W"]], ["n" * 512],
                msg="The upgraded W event is retained once with its exact whole display prefix.")
    check.equal({row["operation_id"] for row in data["operations"]}, {symbols["OA"], symbols["OB"]},
                msg="The full upgrade still reaches both explicitly attributable operations.")
    check.equal(len(data["answers"]), 3, msg="The root answer and both fully expanded W answers survive event projection deduplication.")
    check.is_false({symbols["FW"], symbols["FR"]} & {row["event_id"] for row in data["events"]},
                   msg="The upgrade does not pull future response events into submitted-input ancestry.")
    check.equal(value["cutoffs"], [{"code": "string_limit", "path": "", "ids": {}, "count": 512}],
                msg="W's long label creates exactly one cutoff across its contextual and full visits.")
    check.equal(value["unknowns"], [], msg="The complete upgraded public graph retains conclusive evidence.")
    check.equal(value["diagnostics_omitted"], {"unknowns": 0, "cutoffs": 0},
                msg="The once-projected upgrade leaves both diagnostic omission counters at zero.")
    check.equal((data["complete"], data["frontier"]), (True, []),
                msg="Every explicit full-upgrade edge remains traversed within the generous bounds.")


def test_slice_retained_turn_rechecks_missing_prompt_at_each_depth(recording):
    path, symbols = recording
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("UPDATE turns SET prompt_ref='missing-reader-prompt' WHERE turn_id=?", (symbols["T1"],))
        connection.commit()
    value = reader.slice(path, symbols["R"], symbols["W1"], max_output_bytes=1048576)
    data = successful(value)
    expected_frontier = [{"kind": "artifact", "id": "missing-reader-prompt", "depth": depth, "reason": "missing_artifact"}
                         for depth in [0, 1, 2, 3]]
    check.equal(data["frontier"], expected_frontier,
                msg="Each metadata depth independently reports the still-missing retained turn prompt in traversal order.")
    check.equal(value["unknowns"], [{"code": "missing_artifact", "path": "", "ids": {"artifact_id": "missing-reader-prompt"}, "count": 1}] * 4,
                msg="All four depth-specific frontier facts preserve their matching unknown notices.")
    check.equal(value["cutoffs"], [], msg="Missing evidence is uncertainty rather than a fabricated output cutoff.")
    check.equal(value["diagnostics_omitted"], {"unknowns": 0, "cutoffs": 0},
                msg="The four missing-prompt facts fit without discarding diagnostics.")
    check.is_false(data["complete"], msg="Retaining a turn projection cannot turn a missing prompt into complete provenance.")
    check.equal([(row["turn_id"], row["prompt_ref"]) for row in data["turns"]], [(symbols["T1"], "missing-reader-prompt")],
                msg="The single retained turn keeps the complete missing prompt identity for every visit.")
    check.is_not_in("missing-reader-prompt", {row["artifact_id"] for row in data["artifacts"]},
                    msg="A missing prompt never becomes a manufactured artifact row or a successful negative cache entry.")
    check.equal({row["event_id"] for row in data["events"]}, {symbols[name] for name in EXPECTED_EVENTS["W1"]},
                msg="Missing prompt checks preserve all literal canonical W1 event identities.")
    check.equal({row["operation_id"] for row in data["operations"]}, {symbols["O1"], symbols["O2"]},
                msg="Missing prompt checks leave both explicit canonical operation ancestors present.")


def test_slice_repeated_link_preserves_classification_at_each_depth(tmp_path):
    original = tmp_path / "public-repeated-link.sqlite"
    symbols = make_context_graph(original, upgrade=True)
    original_bytes = original.read_bytes()
    path = tmp_path / "malformed-reader-link-copy.sqlite"
    shutil.copyfile(original, path)
    # This copy models an explicitly invalid external recording. The local
    # constraint bypass constructs that input only; the schema and original
    # public recording stay intact and no production validation is weakened.
    with closing(sqlite3.connect(path)) as connection:
        selected = connection.execute("SELECT link_id FROM links WHERE relation='answers' AND source_event_id=?", (symbols["DA"],)).fetchall()
        assert len(selected) == 1, "The public control must allocate exactly one DA answer link before constructing its malformed external copy"
        link_id = selected[0][0]
        connection.execute("PRAGMA ignore_check_constraints=ON")
        connection.execute("UPDATE links SET source_slot=? WHERE link_id=?", ("s" * 1024, link_id))
        connection.commit()
    value = reader.slice(path, symbols["R"], symbols["ROOT"], max_output_bytes=1048576)
    data = successful(value)
    check.equal([row["source_slot"] for row in data["links"] if row["link_id"] == link_id], ["s" * 512],
                msg="The repeated malformed link keeps one projected row with exactly one clipped slot.")
    check.equal(value["cutoffs"], [{"code": "string_limit", "path": "", "ids": {}, "count": 512}],
                msg="Repeated link classification does not repeat the display projection cutoff.")
    check.equal([(row["kind"], row["id"], row["depth"], row["reason"]) for row in data["frontier"]],
                [("link", link_id, 0, "invalid_answer_shape"), ("link", link_id, 0, "invalid_binding"),
                 ("link", link_id, 2, "invalid_answer_shape"), ("link", link_id, 2, "invalid_binding")],
                msg="Both link classifications remain explicit at each independent root and upgraded-witness depth.")
    expected_unknowns = []
    for request in (symbols["ROOT"], symbols["W"]):
        expected_unknowns.extend([
            {"code": "invalid_binding", "path": "", "ids": {"request_event_id": request, "link_id": link_id}, "count": 1},
            {"code": "invalid_answer_shape", "path": "", "ids": {"request_event_id": request, "link_id": link_id}, "count": 1},
            {"code": "invalid_answer_shape", "path": "", "ids": {"link_id": link_id}, "count": 1},
            {"code": "invalid_binding", "path": "", "ids": {"link_id": link_id}, "count": 1},
        ])
    check.equal(value["unknowns"], expected_unknowns,
                msg="Full answer iteration and depth-specific link classification preserve all eight ordered notices without collapsing repeated codes.")
    check.equal(value["diagnostics_omitted"], {"unknowns": 0, "cutoffs": 0},
                msg="All repeated-link facts remain visible within diagnostic capacity.")
    check.is_false(data["complete"], msg="A known malformed binding cannot become complete merely because its link projection was already retained.")
    names = {"R0", "DA", "DB", "Z", "SA", "SB", "EA", "EB", "W", "Q", "P", "ROOT"}
    check.equal({row["event_id"] for row in data["events"]}, {symbols[name] for name in names},
                msg="Repeated invalid classification still queues all twelve literal explicit ancestors.")
    check.equal({row["operation_id"] for row in data["operations"]}, {symbols["OA"], symbols["OB"]},
                msg="Both explicit operations remain reachable through the preserved repeated link visits.")
    check.equal(len(data["answers"]), 3, msg="Both DA occurrences and the unaffected DB occurrence remain present.")
    check.equal([row["association_state"] for row in data["answers"] if row["source_event_id"] == symbols["DA"]], ["invalid_binding", "invalid_binding"],
                msg="Both current DA occurrences retain their failed association state after repeated-link projection is skipped.")
    check.equal([row["association_state"] for row in data["answers"] if row["source_event_id"] == symbols["DB"]], ["linked"],
                msg="The neighboring complete DB binding remains linked.")
    check.equal(original.read_bytes(), original_bytes, msg="Malformed external-fixture construction and inspection leave the original public recording byte-for-byte unchanged.")


def test_malformed_cycle_terminates_with_literal_graph_and_diagnostic(tmp_path):
    original = tmp_path / "public-context.sqlite"
    symbols = make_context_graph(original, upgrade=True)
    path = tmp_path / "malformed-cycle-copy.sqlite"
    shutil.copyfile(original, path)
    # Explicitly malformed input: Q now cites its later descendant P. No
    # production link ID or digest is recomputed to make the edit appear valid.
    with sqlite3.connect(path) as connection:
        connection.execute("INSERT INTO links(link_id,run_id,witness_event_id,source_event_id,target_event_id,relation,source_slot,source_pointer,details_json) VALUES(?,?,?,?,?,'transformed_from','payload','','{}')",
                           ("reader-test-cycle", symbols["R"], symbols["Q"], symbols["P"], symbols["Q"]))
    value = reader.slice(path, symbols["R"], symbols["ROOT"], max_output_bytes=1048576)
    data = successful(value)
    names = {"R0", "DA", "DB", "Z", "SA", "SB", "EA", "EB", "W", "Q", "P", "ROOT"}
    check.equal({row["event_id"] for row in data["events"]}, {symbols[name] for name in names}, msg="A cycle terminates through visited context/full states without duplicate nodes or fabricated ancestry.")
    check.equal(len(data["events"]), len(names), msg="The already fully expanded cycle target must not be emitted twice.")
    check.is_false(data["complete"], msg="A known future-source contradiction cannot be reported as complete provenance.")
    check.is_in("contradictory_witness_order", {entry["reason"] for entry in data["frontier"]}, msg="The cycle remains a visible temporal contradiction instead of silently disappearing through visited handling.")


def test_depth_zero_reports_explicit_frontier_and_root_only(recording):
    path, symbols = recording
    first = reader.slice(path, symbols["R"], symbols["W1"], max_depth=0, max_output_bytes=1048576)
    second = reader.slice(path, symbols["R"], symbols["W1"], max_depth=0, max_output_bytes=1048576)
    data = successful(first)
    check.equal({row["event_id"] for row in data["events"]}, {symbols["W1"]}, msg="Depth zero includes the intact root and attached metadata, with no followed event association.")
    check.equal((len(data["members"]), len(data["answers"]), data["operations"]), (6, 2, []), msg="Root membership and occurrence rows count as attached metadata while operation associations consume depth.")
    check.is_false(data["complete"], msg="Unfollowed root decisions/prepared insertion prevent false graph completeness.")
    check.is_true(all(row["depth"] == 1 and row["reason"] == "depth_limit" for row in data["frontier"]), msg="Each unavailable next explicit edge is described at exactly depth one.")
    check.equal(first, second, msg="Bounded BFS selection, diagnostics and frontier must be deterministic on one unchanged recording.")


def test_operation_associations_each_consume_graph_depth(recording):
    path, symbols = recording
    data = successful(reader.slice(path, symbols["R"], symbols["W3"], max_depth=2, max_output_bytes=1048576))
    check.equal({row["event_id"] for row in data["events"]}, {symbols[name] for name in ("W3", "W1", "D1", "D2", "R0", "Z0")}, msg="Root-to-decision is one edge; producing model context is the second edge, while lifecycle events require the third.")
    check.equal({row["operation_id"] for row in data["operations"]}, {symbols["O1"], symbols["O2"]}, msg="Explicit decision-to-operation associations fit at depth two as table-like records.")
    check.is_in({"kind": "event", "id": symbols["S1"], "depth": 3, "reason": "depth_limit"}, data["frontier"], msg="An operation's start event is a distinct followed association and must not bypass the depth limit.")


@pytest.mark.parametrize("chain_length,complete", [(12, True), (13, False)], ids=["exact-depth-twelve", "one-edge-beyond-depth"])
def test_exact_depth_limit_and_one_more_explicit_edge(tmp_path, chain_length, complete):
    path = tmp_path / "depth-chain.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    run_id = recorder.capture_scope().run_id
    prepared = []
    try:
        with recorder.turn("explicit prepared chain"):
            for index in range(chain_length):
                sources = [] if not prepared else [{"event_id": prepared[-1].event_id}]
                prepared.append(observed(recorder, "host.tool_result", {"step": index}, sources=sources))
            root = observed(recorder, "model.request", {"messages": [{"content": {"step": chain_length - 1}}]}, messages=[{"bindings": []}],
                            insertions=[{"source_event_id": prepared[-1].event_id, "target_pointer": "/messages/0/content", "content": {"step": chain_length - 1}}])
            observed(recorder, "model.response", {}, model_call_id=root.model_call_id)
    finally:
        closed(recorder)
    data = successful(reader.slice(path, run_id, root.event_id, max_depth=12, max_output_bytes=1048576))
    expected = {root.event_id, *(row.event_id for row in prepared[-12:])}
    check.equal({row["event_id"] for row in data["events"]}, expected, msg="Every explicit followed source consumes exactly one graph depth, through the twelfth edge.")
    check.equal(data["complete"], complete, msg="Exactly exhausted depth is complete only when no further required edge exists.")
    if not complete:
        check.is_in({"kind": "event", "id": prepared[0].event_id, "depth": 13, "reason": "depth_limit"}, data["frontier"], msg="The one additional source appears intact at depth thirteen rather than being inferred or silently discarded.")


@pytest.mark.parametrize("member_count,complete", [(993, True), (994, False)], ids=["exact-thousand-records", "one-record-beyond"])
def test_exact_total_record_limit_includes_memberships(tmp_path, member_count, complete):
    path = tmp_path / "members.sqlite"
    recorder = Recorder(path, capture="content", coverage=COVERAGE)
    run_id = recorder.capture_scope().run_id
    try:
        with recorder.turn("distinct prompt artifact"):
            message = {"content": "one repeated explicit message"}
            request = observed(recorder, "model.request", {"messages": [message] * member_count}, messages=[{"bindings": []}] * member_count)
            observed(recorder, "model.response", {}, model_call_id=request.model_call_id)
    finally:
        closed(recorder)
    value = reader.slice(path, run_id, request.event_id, limit=1000, max_output_bytes=1048576)
    data = successful(value)
    check.equal(count_records(data), 1000, msg="The limit counts run, turn, root, four artifact rows and every emitted repeated membership.")
    check.equal((len(data["runs"]), len(data["turns"]), len(data["events"]), len(data["artifacts"]), len(data["members"])), (1, 1, 1, 4, 993), msg="Shared artifact identity must not erase repeated membership positions from the total record budget.")
    check.equal(data["complete"], complete, msg="The exact boundary is complete, while one more available membership requires an explicit cutoff.")
    if not complete:
        check.is_in("record_limit", {row["reason"] for row in data["frontier"]}, msg="The additional retained membership must produce an explicit bounded frontier.")


def test_small_record_cap_and_byte_cap_preserve_root_identity(recording):
    path, symbols = recording
    data = successful(reader.slice(path, symbols["R"], symbols["W1"], limit=1, max_output_bytes=1048576))
    check.equal((count_records(data), [row["event_id"] for row in data["events"]], data["complete"]), (1, [symbols["W1"]], False), msg="A one-record graph contains the root first and marks unavailable attached context as incomplete.")
    full = reader.slice(path, symbols["R"], symbols["W3"], max_output_bytes=1048576)
    budget = len(json.dumps(full, ensure_ascii=False, separators=(",", ":")).encode()) - 500
    limited = reader.slice(path, symbols["R"], symbols["W3"], max_output_bytes=budget)
    clipped = successful(limited)
    check.is_false(clipped["complete"], msg="Byte-limited optional rows cannot leave a falsely complete ancestry claim.")
    check.is_in(symbols["W3"], {row["event_id"] for row in clipped["events"]}, msg="Whole-row trimming must preserve the required root event.")
    check.less_equal(len(json.dumps(limited, ensure_ascii=False, separators=(",", ":")).encode()), budget, msg="All graph arrays, diagnostics, frontier and scope share the compact UTF-8 byte budget.")
    check.less_equal(len(clipped["frontier"]), 20, msg="Frontier diagnostics remain separately bounded under output trimming.")


def test_pending_fanout_is_bounded_before_graph_materialization(recording):
    path, symbols = recording
    # Labelled malformed reader input: many explicit operation identities cite
    # the same admitted decision/start pair. They intentionally do not pretend
    # to be independently captured valid operations; this exercises graph caps.
    with sqlite3.connect(path) as connection:
        connection.row_factory = sqlite3.Row
        original = dict(connection.execute("SELECT * FROM operations WHERE operation_id=?", (symbols["O1"],)).fetchone())
        columns = tuple(original)
        sql = "INSERT INTO operations(" + ",".join(columns) + ") VALUES(" + ",".join("?" for _ in columns) + ")"
        for index in range(1001):
            row = {**original, "operation_id": "reader-fanout-" + str(index).zfill(4)}
            connection.execute(sql, [row[name] for name in columns])
    value = reader.slice(path, symbols["R"], symbols["W3"], max_output_bytes=1048576)
    data = successful(value)
    check.less_equal(count_records(data), 1000, msg="A large explicit fanout must remain bounded across all emitted record kinds.")
    check.is_in("pending_limit", {entry["code"] for entry in value["cutoffs"]}, msg="The queued association frontier must hit its own bound before materializing the whole graph.")
    check.less_equal(len(data["frontier"]), 20, msg="A large pending frontier cannot expand output diagnostics beyond their fixed cap.")
    check.greater(value["diagnostics_omitted"]["cutoffs"], 0, msg="Suppressed fanout/frontier diagnostics require explicit omitted accounting.")
    check.is_false(data["complete"], msg="Bounded fanout cannot claim complete ancestry.")


@pytest.mark.parametrize("case", ["missing-endpoint", "missing-manifest", "matching-body-does-not-repair"])
def test_missing_required_evidence_is_not_reconstructed(recording, case):
    path, symbols = recording
    with sqlite3.connect(path) as connection:
        if case == "missing-manifest":
            connection.execute("DELETE FROM artifacts WHERE artifact_id=(SELECT payload_ref FROM events WHERE event_id=?)", (symbols["W3"],))
        else:
            connection.execute("UPDATE links SET source_event_id='symbolic-missing-source' WHERE relation='answers' AND source_event_id=?", (symbols["D1"],))
            if case == "matching-body-does-not-repair":
                # D1 still has identical retained selection bytes and name, but
                # neither those bytes nor its existing operation repairs an ID.
                connection.execute("UPDATE events SET name='same-name' WHERE event_id IN (?,?)", (symbols["D1"], symbols["D2"]))
    data = successful(reader.slice(path, symbols["R"], symbols["W3"], max_output_bytes=1048576))
    check.is_false(data["complete"], msg="A required missing endpoint or manifest remains unavailable even when similar retained evidence exists elsewhere.")
    if case != "missing-manifest":
        check.is_false(symbols["D1"] in {row["event_id"] for row in data["events"]}, msg="Symbolic missing decision identity cannot be repaired by matching names, arguments or old operation attribution.")
        check.is_in("missing_decision", {row["association_state"] for row in data["answers"]}, msg="The missing endpoint stays visible in the current answer occurrence.")
    else:
        check.equal(data["members"], [], msg="A missing root manifest exposes no membership population and must not be mistaken for known empty history.")


@pytest.mark.parametrize("limit,max_depth", [(0, 12), (1001, 12), (True, 12), (1000, -1), (1000, 13), (1000, False)])
def test_slice_rejects_out_of_range_and_boolean_bounds(recording, limit, max_depth):
    path, symbols = recording
    value = reader.slice(path, symbols["R"], symbols["R0"], limit=limit, max_depth=max_depth)
    check.equal((value["ok"], value["error"]["code"]), (False, "invalid_request"), msg="Graph limits require actual integers within the exact documented record/depth bounds.")


def test_slice_root_validation_distinguishes_absent_non_authoritative_and_wrong_kind(recording):
    path, symbols = recording
    missing = reader.slice(path, symbols["R"], "absent")
    wrong = reader.slice(path, symbols["R"], symbols["D1"])
    with sqlite3.connect(path) as connection:
        connection.execute("UPDATE events SET provenance='imported' WHERE event_id=?", (symbols["R0"],))
    unsupported = reader.slice(path, symbols["R"], symbols["R0"])
    check.equal([value["error"]["code"] for value in (missing, wrong, unsupported)], ["unknown_event", "invalid_request", "unknown_event"], msg="A known authoritative wrong-kind root differs from absent or non-authoritative event selection.")


def test_long_root_identity_and_no_content_metadata_policy(recording):
    path, symbols = recording
    identity = "request-" + "é中" * 400
    with sqlite3.connect(path) as connection:
        connection.execute("UPDATE events SET event_id=? WHERE event_id=?", (identity, symbols["R0"]))
    value = reader.slice(path, symbols["R"], identity, no_content=True, max_output_bytes=1048576)
    data = successful(value)
    check.equal((value["scope"]["request_event_id"], data["root_request_event_id"], data["events"][0]["event_id"]), (identity, identity, identity), msg="Identity-bearing strings remain intact beyond the 512-byte display cap.")
    check.is_true(all(row["conditions"] is None for row in data["turns"]), msg="No-content suppresses caller condition bodies while preserving graph computation from internal manifests.")
    check.equal(len(data["members"]), 2, msg="No-content policy does not disable structural membership inspection.")
    refused = reader.slice(path, symbols["R"], identity, max_output_bytes=1024)
    check.equal((refused["ok"], refused["error"]["code"]), (False, "limit_exceeded"), msg="An intact required identity that cannot fit is refused rather than truncated into a different event.")


def test_slice_exact_encoded_cap_and_one_byte_over(recording):
    path, symbols = recording
    text = 'é中"\\\n\t' * 15
    with sqlite3.connect(path) as connection:
        connection.execute("UPDATE events SET name=? WHERE event_id=?", (text, symbols["D1"]))
    full = reader.slice(path, symbols["R"], symbols["W3"], max_output_bytes=1048576)
    encoded = json.dumps(full, ensure_ascii=False, separators=(",", ":")).encode()
    exact = reader.slice(path, symbols["R"], symbols["W3"], max_output_bytes=len(encoded))
    smaller = reader.slice(path, symbols["R"], symbols["W3"], max_output_bytes=len(encoded) - 1)
    check.equal(exact, full, msg="An exactly fitting encoded graph must retain every row despite multibyte and escaped control characters.")
    data = successful(smaller)
    check.is_false(data["complete"], msg="One byte over the cap removes only whole optional rows and marks ancestry incomplete.")
    check.is_in(symbols["W3"], {row["event_id"] for row in data["events"]}, msg="The required root identity remains intact after whole-record output trimming.")
    check.is_in("byte_limit", {row["code"] for row in smaller["cutoffs"]}, msg="The one-byte-over graph must identify the encoded-envelope cutoff.")
    check.less_equal(len(json.dumps(smaller, ensure_ascii=False, separators=(",", ":")).encode()), len(encoded) - 1, msg="UTF-8 bytes and JSON escaping determine the final graph budget, not Python character counts.")
