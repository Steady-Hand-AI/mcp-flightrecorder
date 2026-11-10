"""SDK-independent scaling contracts: ownership, occurrence truth and bounded reads."""
from collections import Counter
from contextlib import closing, contextmanager
import hashlib
from importlib.resources import files
import inspect as signatures
import json
import logging
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import threading
from types import SimpleNamespace

import pytest
from pytest_check import check

from mcp_flightrecorder import Recorder, inspect as reader
from mcp_flightrecorder import _inspect_common as common
from mcp_flightrecorder import _inspect_checks as checks
from mcp_flightrecorder import _inspect_navigation as navigation
from mcp_flightrecorder import _inspect_relations as relations
from mcp_flightrecorder import _recorder, _storage, cli


COVERAGE = {"producers": {"model": "scale.model", "host": "scale.host", "mcp": "scale.mcp"},
            "model_complete": True}


def _accepted(value):
    assert value.status == "accepted", value
    return value


def _observe(recorder, kind, payload, **facts):
    return _accepted(recorder.observe(kind, scope=recorder.capture_scope(), payload=payload, facts=facts))


def _drain(recorder):
    with recorder._condition:
        assert recorder._condition.wait_for(lambda: recorder._ordinary_slots == 0 and not recorder._pending, timeout=5), "writer did not drain"


@contextmanager
def _recording(path, **limits):
    recorder = Recorder(path, capture="content", coverage=COVERAGE, limits=limits)
    try:
        yield recorder
    finally:
        result = recorder.close()
        assert result.writer_stopped, result


def _seed(recorder):
    """Commit genuine reference sources; no fabricated pending/cache metadata."""
    request = _observe(recorder, "model.request", {"messages": []}, messages=[])
    _drain(recorder)
    decision = _observe(recorder, "model.tool_call", {"arguments": {}}, name="echo", model_call_id=request.model_call_id)
    _drain(recorder)
    _observe(recorder, "model.response", {}, model_call_id=request.model_call_id)
    _drain(recorder)
    with recorder.dispatch(action="execute", name="echo", arguments={}, decision_event_ids=[decision.event_id]) as dispatch:
        _accepted(dispatch.receipt)
        _drain(recorder)
        start = _accepted(recorder.start_operation(scope=recorder.capture_scope(), client_id="client", method="tools/call",
                                                    arguments={}, decision_id=decision.decision_id, link_basis="host_hook"))
        _drain(recorder)
        end = _accepted(recorder.finish_operation(start.ticket, status="ok", result="answer", text_paths=[""], resolution="local"))
        _drain(recorder)
    return request, decision, dispatch.receipt, end


class _TrackedReader:
    """Test-owned wrapper of a real connection, recording creator/close threads."""
    def __init__(self, native, action=None, close_failure=False):
        self.native = native
        self.creator = threading.get_ident()
        self.closer = None
        self.closed = False
        self.action = action
        self.close_failure = close_failure
        self.close_calls = 0

    @property
    def row_factory(self):
        return self.native.row_factory

    @row_factory.setter
    def row_factory(self, value):
        self.native.row_factory = value

    def execute(self, *args, **kwargs):
        if self.action is not None:
            self.action(self, args)
        return self.native.execute(*args, **kwargs)

    def close(self):
        self.close_calls += 1
        self.closer = threading.get_ident()
        if self.close_failure and self.close_calls == 1:
            raise sqlite3.OperationalError("one-time owned close failure")
        self.native.close()
        self.closed = True


@contextmanager
def _track_readers(monkeypatch, action=None, close_failure=False):
    opened = []
    original = _storage.connect
    def connect(*args, **kwargs):
        native = original(*args, **kwargs)
        if kwargs.get("readonly"):
            native = _TrackedReader(native, action, close_failure)
            opened.append(native)
        return native
    with monkeypatch.context() as patch:
        patch.setattr(_storage, "connect", connect)
        yield opened


def _read(path, sql, parameters=()):
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as connection:
        connection.row_factory = sqlite3.Row
        return connection.execute(sql, parameters).fetchall()


@contextmanager
def _context(path, run_id):
    ctx = common.Context("check", {"run_id": run_id})
    common.open_read(ctx, path.resolve())
    try:
        yield ctx
    finally:
        ctx.connection.rollback()
        ctx.connection.close()


def _write_artifact(connection, identity, value):
    body = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    raw = body.encode()
    connection.execute("UPDATE artifacts SET body_text=?,stored_sha256=?,stored_bytes=? WHERE artifact_id=?",
                       (body, hashlib.sha256(raw).hexdigest(), len(raw), identity))


def _history(path, rounds=20):
    """Actual, fully annotated host observations, with repeated linked history."""
    requests, decisions, results = [], [], []
    messages, annotations = [{"role": "user", "content": "start"}], [{"bindings": []}]
    with _recording(path) as recorder:
        with recorder.turn("history") as turn:
            for index in range(rounds):
                request = _observe(recorder, "model.request", {"messages": messages}, messages=annotations)
                decision = _observe(recorder, "model.tool_call", {"arguments": {"i": index}},
                                    name="echo", model_call_id=request.model_call_id)
                _observe(recorder, "model.response", {}, model_call_id=request.model_call_id)
                with recorder.dispatch(action="execute", name="echo", arguments={"i": index},
                                       decision_event_ids=[decision.event_id]):
                    start = _accepted(recorder.start_operation(scope=recorder.capture_scope(), client_id="client",
                        method="tools/call", name="echo", arguments={"i": index}, decision_id=decision.decision_id,
                        link_basis="host_hook"))
                    text = "answer " + str(index)
                    end = _accepted(recorder.finish_operation(start.ticket, status="ok", result=text,
                                                               text_paths=[""], resolution="local"))
                _drain(recorder)
                messages += [{"role": "assistant", "content": "call " + str(index)}, {"role": "tool", "content": text}]
                annotations += [{"bindings": []}, {"bindings": [{"decision_event_id": decision.event_id,
                    "pointer": "/content", "result_event_id": end.event_id, "text_paths": [""]}]}]
                requests.append(request.event_id)
                decisions.append(decision.event_id)
                results.append(end.event_id)
        run_id = recorder.capture_scope().run_id
    assert _read(path, "PRAGMA integrity_check")[0][0] == "ok"
    assert _read(path, "SELECT close_state,dropped_events,writer_errors FROM runs")[0][:] == ("clean", 0, 0)
    return {"run": run_id, "turn": turn.receipt.turn_id, "requests": requests, "decisions": decisions, "results": results}


def test_unfinished_models_preserves_nulls_multiplicity_and_run_scope():
    with closing(sqlite3.connect(":memory:", isolation_level=None)) as connection:
        connection.executescript(files("mcp_flightrecorder").joinpath("schema.sql").read_text())
        for run_id in ("r", "other"):
            connection.execute("INSERT INTO runs(run_id,started_at,environment_json,policy_json,coverage_json) VALUES(?, 'now','{}','{}','{}')", (run_id,))
            for turn in ("a", "b"):
                connection.execute("INSERT INTO turns(run_id,turn_id,started_at,conditions_json,condition_key,coverage_json) VALUES(?,?,'now','{}','key','{}')", (run_id, turn))
        records = [("r", "a", "model.request", "matched"), ("r", "a", "model.response", "matched"),
                   ("r", "a", "model.request", "missing"), ("r", "a", "model.request", "missing"),
                   ("r", "b", "model.request", None), ("r", None, "model.request", None),
                   ("r", "b", "model.response", None), ("r", "b", "model.request", "foreign"),
                   ("other", "b", "model.response", "foreign")]
        for sequence, (run, turn, kind, model) in enumerate(records):
            connection.execute("INSERT INTO events(event_id,run_id,turn_id,sequence,kind,boundary,provenance,source_id,observed_at,model_call_id,details_json) VALUES(?,?,?,?,?,'model','observed','test','now',?,'{}')",
                               (str(sequence), run, turn, sequence, kind, model))
        writer = _storage.Writer(SimpleNamespace(_run_id="r"))
        writer.connection = connection
        assert Counter(row[0] for row in writer._unfinished_models()) == Counter({"a": 2, "b": 2, None: 1})
        connection.execute("DELETE FROM events WHERE kind='model.response'")
        assert Counter(row[0] for row in writer._unfinished_models()) == Counter({"a": 3, "b": 2, None: 1})
        connection.execute("DELETE FROM events")
        assert writer._unfinished_models() == []


@pytest.mark.parametrize("unscoped", [False, True], ids=["matched-clean", "null-turn-incomplete"])
def test_unscoped_unfinished_request_reaches_actual_closure(tmp_path, unscoped):
    path = tmp_path / "closure.sqlite"
    with _recording(path) as recorder:
        for name in ("first", "second"):
            with recorder.turn(name):
                request = _observe(recorder, "model.request", {"messages": []}, messages=[])
                _observe(recorder, "model.response", {}, model_call_id=request.model_call_id)
        if unscoped:
            _observe(recorder, "model.request", {"messages": []}, messages=[])
    assert _read(path, "SELECT close_state FROM runs")[0][0] == ("incomplete" if unscoped else "clean")
    assert [row[0] for row in _read(path, "SELECT capture_state FROM turns")] == ["partial" if unscoped else "complete"] * 2


@pytest.mark.parametrize("route", ["request", "dispatch", "result", "attribution"])
def test_committed_reference_paths_share_one_reader_and_close_before_admit(tmp_path, monkeypatch, route):
    with _recording(tmp_path / "scope.sqlite") as recorder:
        _, decision, dispatch, end = _seed(recorder)
        assert recorder._limits["committed_cache_items"] == 512
        with recorder._condition:
            recorder._committed.clear()
        with _track_readers(monkeypatch) as opened:
            original_admit = recorder._admit
            def admit(*args, **kwargs):
                assert all(item.closed and item.creator == item.closer == threading.get_ident() for item in opened)
                return original_admit(*args, **kwargs)
            with monkeypatch.context() as patch:
                patch.setattr(recorder, "_admit", admit)
                if route == "request":
                    message = {"content": "answer"}
                    binding = {"bindings": [{"decision_event_id": decision.event_id, "pointer": "/content",
                                             "result_event_id": end.event_id, "text_paths": [""]}]}
                    receipt = _observe(recorder, "model.request", {"messages": [message] * 4}, messages=[binding] * 4)
                elif route == "dispatch":
                    with recorder.dispatch(action="execute", name="echo", arguments={}, decision_event_ids=[decision.event_id] * 3) as scope:
                        receipt = _accepted(scope.receipt)
                elif route == "result":
                    receipt = _observe(recorder, "host.tool_result", "prepared", sources=[{"event_id": end.event_id}, {"event_id": decision.event_id}])
                else:
                    receipt = _accepted(recorder.start_operation(scope=recorder.capture_scope(), client_id="client", method="tools/call",
                        arguments={}, dispatch_event_id=dispatch.event_id, link_basis="host_hook"))
                    _accepted(recorder.finish_operation(receipt.ticket, status="ok", result="answer"))
            assert len(opened) == 1
            assert all(item.closed and item.creator == item.closer for item in opened)
            assert not receipt.losses


def test_pending_lookup_uses_no_reader_and_does_not_promote_before_commit(tmp_path, monkeypatch):
    with _recording(tmp_path / "pending.sqlite", batch_items=1) as recorder:
        entered, release = threading.Event(), threading.Event()
        original = recorder._commit_batch
        def held(connection):
            entered.set()
            assert release.wait(5)
            return original(connection)
        with monkeypatch.context() as patch:
            patch.setattr(recorder, "_commit_batch", held)
            try:
                source = _observe(recorder, "cache.served", {}, owner="host")
                assert entered.wait(5)
                with _track_readers(monkeypatch) as opened:
                    result = _observe(recorder, "host.tool_result", "answer", sources=[{"event_id": source.event_id}])
                    assert not result.losses and opened == []
                with recorder._condition:
                    assert ("event", source.event_id) in recorder._pending
                    assert ("event", source.event_id) not in recorder._committed
            finally:
                release.set()
        _drain(recorder)
        with _track_readers(monkeypatch) as opened:
            _observe(recorder, "host.tool_result", "answer", sources=[{"event_id": source.event_id}])
            assert opened == []
        assert len(recorder._committed) <= 512


def test_same_scope_miss_sees_later_commit_after_memory_eviction(tmp_path, monkeypatch):
    path = tmp_path / "fresh.sqlite"
    with _recording(path, committed_cache_items=1, batch_items=1) as recorder:
        prepared, publish, missed, resume = (threading.Event() for _ in range(4))
        state, lookups = {}, []
        original_admit, original_lookup = recorder._admit, recorder._lookup_connection
        def gate_admit(preparation, bundle, **kwargs):
            if threading.current_thread().name == "prerequisite":
                state["source_id"] = bundle.receipt.event_id
                prepared.set()
                assert publish.wait(5)
            return original_admit(preparation, bundle, **kwargs)
        def lookup(connection, category, identity):
            value = original_lookup(connection, category, identity)
            if threading.current_thread().name == "observer" and identity == state.get("source_id"):
                lookups.append(value)
                if len(lookups) == 1:
                    assert value is None
                    missed.set()
                    assert resume.wait(5)
            return value
        def prerequisite():
            state["source"] = recorder.observe("cache.served", scope=recorder.capture_scope(), payload={}, facts={"owner": "host"})
        def observer():
            state["observed"] = recorder.observe("host.tool_result", scope=recorder.capture_scope(), payload="prepared",
                facts={"sources": [{"event_id": state["source_id"]}] * 2})
        with monkeypatch.context() as patch, _track_readers(monkeypatch) as opened:
            patch.setattr(recorder, "_admit", gate_admit)
            patch.setattr(recorder, "_lookup_connection", lookup)
            producer = threading.Thread(target=prerequisite, name="prerequisite")
            consumer = threading.Thread(target=observer, name="observer")
            producer.start()
            try:
                assert prepared.wait(5)
                consumer.start()
                assert missed.wait(5)
                publish.set()
                producer.join(5)
                assert not producer.is_alive()
                _accepted(state["source"])
                _drain(recorder)
                assert ("event", state["source_id"]) in recorder._committed
                _observe(recorder, "cache.served", {}, owner="host")
                _drain(recorder)
                assert ("event", state["source_id"]) not in recorder._committed
                resume.set()
                consumer.join(5)
                assert not consumer.is_alive()
            finally:
                publish.set(); resume.set()
                producer.join(5)
                if consumer.ident is not None:
                    consumer.join(5)
            receipt = _accepted(state["observed"])
            assert len(lookups) == 2 and lookups[0] is None and lookups[1]["kind"] == "cache.served"
            assert len(opened) == 1 and opened[0].closed and opened[0].creator == opened[0].closer == consumer.ident
            assert "unresolved" in receipt.losses, "later knowledge cannot heal the first materialized unknown"
    details = json.loads(_read(path, "SELECT details_json FROM events WHERE event_id=?", (receipt.event_id,))[0][0])
    assert "unresolved" in details["capture_losses"]


@pytest.mark.parametrize("failure", ["validation", "read", "capacity", "close"])
def test_identity_scope_failure_releases_reader_without_late_publication(tmp_path, monkeypatch, failure):
    path = tmp_path / "cleanup.sqlite"
    with _recording(path, ordinary_items=1, batch_items=1) as recorder:
        _, decision, _, _ = _seed(recorder)
        with recorder._condition:
            recorder._committed.clear()
        entered, release = threading.Event(), threading.Event()
        original_persist, original_admit = recorder._persist_bundle, recorder._admit
        admitted = []
        def held(connection, bundle):
            if bundle.kind == "cache.served":
                entered.set()
                assert release.wait(5)
            return original_persist(connection, bundle)
        def read_action(connection, arguments):
            if failure == "read":
                raise sqlite3.OperationalError("forced read failure")
        with monkeypatch.context() as patch:
            if failure == "capacity":
                patch.setattr(recorder, "_persist_bundle", held)
                _observe(recorder, "cache.served", {}, owner="host")
                assert entered.wait(5)
            before = len(_read(path, "SELECT event_id FROM events"))
            try:
                with _track_readers(monkeypatch, read_action, failure == "close") as opened:
                    def admit(*args, **kwargs):
                        assert all(item.closed and item.creator == item.closer for item in opened)
                        admitted.append(True)
                        return original_admit(*args, **kwargs)
                    patch.setattr(recorder, "_admit", admit)
                    sources = [{"event_id": decision.event_id}]
                    if failure == "validation":
                        sources += [{"event_id": decision.event_id, "pointer": "not-a-pointer"}]
                    receipt = recorder.observe("host.tool_result", scope=recorder.capture_scope(), payload="answer", facts={"sources": sources})
                    assert receipt.status == "rejected"
                    assert receipt.reason == {"validation": "invalid_input", "capacity": "capacity", "close": "writer_failed", "read": "writer_failed"}[failure]
                    assert len(opened) == 1 and opened[0].closed
                    assert opened[0].creator == opened[0].closer == threading.get_ident()
                    with pytest.raises(sqlite3.ProgrammingError):
                        opened[0].native.execute("SELECT 1")
                    assert bool(admitted) == (failure == "capacity")
                    if failure == "close":
                        assert opened[0].close_calls == 2
            finally:
                release.set()
        _drain(recorder)
        assert len(_read(path, "SELECT event_id FROM events")) == before + int(failure == "capacity")


def test_nested_capture_owns_distinct_local_reader(tmp_path, monkeypatch):
    with _recording(tmp_path / "nested.sqlite") as recorder:
        _, decision, _, end = _seed(recorder)
        with recorder._condition:
            recorder._committed.clear()
        nested, started = [], False
        def nest(connection, arguments):
            nonlocal started
            if not started:
                started = True
                nested.append(_observe(recorder, "host.tool_result", "inner", sources=[{"event_id": end.event_id}]))
                assert not connection.closed, "the outer scope remains owned during a nested call"
        with _track_readers(monkeypatch, nest) as opened:
            outer = _observe(recorder, "host.tool_result", "outer", sources=[{"event_id": decision.event_id}])
            assert len(nested) == 1 and not outer.losses and not nested[0].losses
            assert len(opened) == 2 and opened[0].native is not opened[1].native
            assert all(item.closed and item.creator == item.closer == threading.get_ident() for item in opened)


def test_capture_log_handler_runs_after_scope_cleanup(tmp_path, monkeypatch):
    with _recording(tmp_path / "logging.sqlite") as recorder:
        _, decision, _, _ = _seed(recorder)
        recorder._committed.clear()
        nested = []
        with _track_readers(monkeypatch) as opened:
            class Handler(logging.Handler):
                def emit(self, record):
                    if record.getMessage().startswith("capture_loss") and not nested:
                        assert opened and all(item.closed for item in opened)
                        recorder._committed.clear()
                        nested.append(_observe(recorder, "host.tool_result", "logged", sources=[{"event_id": decision.event_id}]))
            handler = Handler()
            _recorder._logger.addHandler(handler)
            try:
                receipt = recorder.observe("host.tool_result", scope=recorder.capture_scope(), payload="outer",
                    facts={"sources": [{"event_id": decision.event_id}, {"event_id": decision.event_id, "pointer": "invalid"}]})
                assert receipt.reason == "invalid_input" and len(nested) == 1
                assert len(opened) == 2 and all(item.closed for item in opened)
            finally:
                _recorder._logger.removeHandler(handler)


@pytest.mark.parametrize("incomplete", [False, True], ids=["complete", "last-request-incomplete"])
def test_repeated_answers_keep_occurrence_completeness_and_full_envelopes(tmp_path, monkeypatch, incomplete):
    path = tmp_path / "occurrences.sqlite"
    ids = _history(path)
    if incomplete:
        with closing(sqlite3.connect(path)) as connection:
            raw = connection.execute("SELECT details_json FROM events WHERE event_id=?", (ids["requests"][-1],)).fetchone()[0]
            connection.execute("UPDATE events SET details_json=? WHERE event_id=?", (json.dumps({**json.loads(raw), "complete": False}), ids["requests"][-1]))
            connection.commit()
    before = path.read_bytes()
    checks, turns = reader.check(path, ids["run"]), reader.find_turns(path, ids["run"])
    assert checks["ok"] and turns["ok"]
    stats = checks["data"]["stats"]
    assert (stats["answer_occurrences"], stats["text_equal"], stats["text_unknown"]) == (190, 171 if incomplete else 190, 19 if incomplete else 0)
    with monkeypatch.context() as patch:
        patch.setattr(common, "_CACHE_MAX_ENTRIES", 0)
        assert reader.check(path, ids["run"]) == checks
        assert reader.find_turns(path, ids["run"]) == turns
    assert path.read_bytes() == before
    with _context(path, ids["run"]) as ctx:
        answers = list(relations.answer_rows(ctx, ids["run"], request_ids=[ids["requests"][-1]]))
        assert len(answers) == 19
        assert [row["result_event_id"] for row in answers] == ids["results"][:-1]
        assert [row["target_pointer"] for row in answers] == [f"/messages/{2 * index + 2}/content" for index in range(19)]
    details = reader.explain(path, ids["run"], ids["turn"], max_output_bytes=1_048_576)
    graph = reader.slice(path, ids["run"], ids["requests"][-1], max_output_bytes=1_048_576)
    assert details["ok"] and graph["ok"]
    assert all(row["result_event_id"] in ids["results"] and row["target_pointer"].endswith("/content") for row in graph["data"]["answers"])


def test_incomplete_current_request_precedes_invalid_comparison_token(tmp_path):
    path = tmp_path / "precedence.sqlite"
    ids = _history(path, 4)
    with closing(sqlite3.connect(path)) as connection:
        raw = connection.execute("SELECT details_json FROM events WHERE event_id=?", (ids["requests"][-1],)).fetchone()[0]
        connection.execute("UPDATE events SET details_json=? WHERE event_id=?", (json.dumps({**json.loads(raw), "complete": False}), ids["requests"][-1]))
        for identity, raw in connection.execute("SELECT link_id,details_json FROM links WHERE relation='answers'").fetchall():
            connection.execute("UPDATE links SET details_json=? WHERE link_id=?", (json.dumps({**json.loads(raw), "submitted_text_digest": {"scope": "wrong"}}), identity))
        connection.commit()
    with _context(path, ids["run"]) as ctx:
        rows = list(relations.answer_rows(ctx, ids["run"]))
        earlier = [row for row in rows if row["request_event_id"] != ids["requests"][-1]]
        later = [row for row in rows if row["request_event_id"] == ids["requests"][-1]]
        assert earlier and all(row["comparison_reason"] == "digest_scope_mismatch" for row in earlier)
        assert len(later) == 3 and all(row["comparison_reason"] == "observation_incomplete" for row in later)


def test_answer_diagnostics_repeat_both_branches_and_count_overflow(tmp_path, monkeypatch):
    path = tmp_path / "diagnostics.sqlite"
    ids = _history(path, 8)
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("UPDATE links SET source_event_id='missing-decision' WHERE relation='answers'")
        connection.commit()
    with _context(path, ids["run"]) as ctx:
        first = list(relations.answer_rows(ctx, ids["run"]))
        assert len(first) == 28 and all(row["association_state"] == "missing_decision" for row in first)
        assert len(ctx.unknowns) == 20 and ctx.diagnostics_omitted["unknowns"] == 36
        assert all(entry["code"] == "missing_decision" for entry in ctx.unknowns)
        assert all(ctx.unknowns[i] == ctx.unknowns[i + 1] for i in range(0, 20, 2)), "association and comparison notices both remain"
        second = list(relations.answer_rows(ctx, ids["run"], decision_ids=["missing-decision"]))
        assert first == second and ctx.diagnostics_omitted["unknowns"] == 92
    for operation in (reader.check, reader.find_turns):
        full = operation(path, ids["run"], max_output_bytes=1_048_576)
        small = operation(path, ids["run"], max_output_bytes=4096)
        with monkeypatch.context() as patch:
            patch.setattr(common, "_CACHE_MAX_ENTRIES", 0)
            assert operation(path, ids["run"], max_output_bytes=1_048_576) == full
            assert operation(path, ids["run"], max_output_bytes=4096) == small
        assert len(json.dumps(small, ensure_ascii=False, separators=(",", ":")).encode()) <= 4096


def test_turn_count_does_not_project_discarded_member_records(tmp_path, monkeypatch):
    path = tmp_path / "count-work.sqlite"
    ids = _history(path, 6)
    projections = []
    original = relations.member_record
    def project(ctx, row):
        projections.append((row["request_event_id"], row["message_index"]))
        return original(ctx, row)
    with monkeypatch.context() as patch:
        patch.setattr(relations, "member_record", project)
        result = reader.find_turns(path, ids["run"])
    assert result["ok"], result
    assert result["data"]["items"][0]["counts"]["request_members"] == 36
    assert projections == [], "count-only discovery must not build discarded public records"


@pytest.mark.parametrize("selection", ["all", "subset", "empty", "foreign"])
def test_member_count_preserves_projection_diagnostics_and_selection(tmp_path, monkeypatch, selection):
    path = tmp_path / "count-diagnostics.sqlite"
    ids = _history(path, 6)
    with closing(sqlite3.connect(path)) as connection:
        # SQLite's INTEGER affinity does not prohibit a malformed text sequence.
        for index, request_id in enumerate(ids["requests"]):
            connection.execute("UPDATE events SET sequence=? WHERE event_id=?", ("s" * (600 + index * 10), request_id))
        parts = connection.execute("SELECT artifact_id FROM artifacts WHERE representation<>'request_refs/2' ORDER BY artifact_id").fetchall()
        for index, (artifact_id,) in enumerate(parts):
            connection.execute("UPDATE artifacts SET body_text=NULL,stored_sha256=NULL,stored_bytes=0,omitted_reason=? WHERE artifact_id=?",
                               ("é" * (300 + index), artifact_id))
        # Retain the guarded malformed-manifest behavior and long identity fields.
        manifests = connection.execute("SELECT artifact_id,body_text FROM artifacts WHERE representation='request_refs/2' ORDER BY artifact_id").fetchall()
        connection.execute("UPDATE artifacts SET body_text='not-json' WHERE artifact_id=?", (manifests[0][0],))
        for artifact_id, body in manifests[1:]:
            manifest = json.loads(body)
            manifest["message_path"] = "/" + "p" * 800
            _write_artifact(connection, artifact_id, manifest)
        connection.commit()
    request_ids = {"all": None, "subset": ids["requests"][::2], "empty": [], "foreign": ["missing-request"]}[selection]
    before = path.read_bytes()
    with _context(path, ids["run"]) as reference:
        projected = list(relations.member_rows(reference, ids["run"], request_ids))
        expected = (reference.unknowns, reference.cutoffs, reference.diagnostics_omitted)
    with _context(path, ids["run"]) as counted:
        deadline_checks = []
        original = counted.check_deadline
        def deadline():
            deadline_checks.append(True)
            original()
        monkeypatch.setattr(counted, "check_deadline", deadline)
        assert relations._member_count(counted, ids["run"], request_ids) == len(projected)
        assert (counted.unknowns, counted.cutoffs, counted.diagnostics_omitted) == expected
        assert len(deadline_checks) >= len(projected)
    if selection == "all":
        assert expected[1] and expected[2]["cutoffs"] > 0
        assert all(row["message_path"].startswith("/" + "p" * 800) for row in projected)
    assert path.read_bytes() == before


def test_check_reuses_link_validation_before_operation_cache_pressure(tmp_path, monkeypatch):
    path = tmp_path / "check-order.sqlite"
    ids = _history(path, 20)
    expected = reader.check(path, ids["run"], max_output_bytes=1_048_576)
    assert expected["ok"] and expected["data"]["status"] == "pass"
    operation_started = []
    repeated_after_operations = []
    seen = Counter()
    original_operation, original_binding = checks._operation_state, relations._message_binding_state
    def operation(ctx, row):
        operation_started.append(row["operation_id"])
        return original_operation(ctx, row)
    def binding(ctx, run_id, link, witness, identity, **kwargs):
        if operation_started and seen[link["link_id"]]:
            repeated_after_operations.append(link["link_id"])
        seen[link["link_id"]] += 1
        return original_binding(ctx, run_id, link, witness, identity, **kwargs)
    with monkeypatch.context() as patch:
        # Small genuine history with enough pressure to expose redundant revalidation.
        patch.setattr(common, "_CACHE_MAX_ENTRIES", 128)
        patch.setattr(checks, "_operation_state", operation)
        patch.setattr(relations, "_message_binding_state", binding)
        actual = reader.check(path, ids["run"], max_output_bytes=1_048_576)
    assert actual == expected
    assert len(operation_started) == 20 and len(seen) == 19
    assert repeated_after_operations == [], "operation-only rows must not displace reusable link validation before its final scan"


def test_turn_reuses_only_its_current_request_validation(tmp_path, monkeypatch):
    path = tmp_path / "current-request-work.sqlite"
    ids = _history(path, 8)
    expected = reader.find_turns(path, ids["run"])
    calls = Counter()
    original = relations.request_info
    def request_info(ctx, run_id, request_id):
        calls[(run_id, request_id)] += 1
        return original(ctx, run_id, request_id)
    with monkeypatch.context() as patch:
        patch.setattr(navigation, "request_info", request_info)
        patch.setattr(relations, "request_info", request_info)
        actual = reader.find_turns(path, ids["run"])
    assert actual == expected and actual["ok"]
    assert calls == Counter((ids["run"], request_id) for request_id in ids["requests"]), "the already owned current witness must not reconstruct its manifest again"


@pytest.mark.parametrize("hint_scope", ["same", "other-request", "other-run"])
def test_witness_info_reuse_requires_exact_current_scope(tmp_path, monkeypatch, hint_scope):
    path = tmp_path / "current-request-scope.sqlite"
    ids = _history(path, 3)
    with _context(path, ids["run"]) as ctx:
        link = ctx.connection.execute("SELECT * FROM links WHERE relation='answers' AND witness_event_id=?", (ids["requests"][-1],)).fetchone()
        witness = relations.event_row(ctx, ids["run"], link["witness_event_id"], observed=True)
        identity = relations._binding_identity(json.loads(link["details_json"]))
        info = relations.request_info(ctx, ids["run"], witness["event_id"])
        expected = relations._message_binding_state(ctx, ids["run"], link, witness, identity)
        assert expected == ("pass", None)
        if hint_scope != "same":
            info = {"event": {"run_id": "other-run" if hint_scope == "other-run" else ids["run"],
                              "event_id": ids["requests"][0] if hint_scope == "other-request" else witness["event_id"]},
                    "status": "fail", "reason": "must_not_use_foreign_info"}
        calls = []
        original = relations.request_info
        def request_info(context, run_id, request_id):
            calls.append((run_id, request_id))
            return original(context, run_id, request_id)
        monkeypatch.setattr(relations, "request_info", request_info)
        assert relations._message_binding_state(ctx, ids["run"], link, witness, identity, witness_info=info) == expected
        assert calls == ([] if hint_scope == "same" else [(ids["run"], witness["event_id"])])
        assert all(value is not info for value in vars(ctx).values())
        assert all(value is not info for value, _cost in ctx._cache.entries.values())


def test_turn_summary_does_not_build_discarded_answer_records(tmp_path, monkeypatch):
    path = tmp_path / "answer-summary-work.sqlite"
    ids = _history(path, 8)
    projected = []
    original = navigation.answer_rows
    def answer_rows(*args, **kwargs):
        for row in original(*args, **kwargs):
            projected.append((row["request_event_id"], row["link_id"]))
            yield row
    monkeypatch.setattr(navigation, "answer_rows", answer_rows)
    result = reader.find_turns(path, ids["run"])
    assert result["ok"], result
    counts = result["data"]["items"][0]["counts"]
    assert (counts["input_answers"], counts["produced_answers"]) == (28, 28)
    assert projected == [], "aggregate passes must consume states without discarded public dictionaries"


@pytest.mark.parametrize("case", ["linked", "mismatch", "unresolved"])
def test_answer_summary_preserves_each_pass_and_diagnostics(tmp_path, case):
    path = tmp_path / "answer-summary-parity.sqlite"
    ids = _history(path, 8)
    if case != "linked":
        with closing(sqlite3.connect(path)) as connection:
            if case == "unresolved":
                connection.execute("UPDATE links SET source_event_id='missing-decision' WHERE relation='answers'")
            else:
                identity, raw = connection.execute("SELECT link_id,details_json FROM links WHERE relation='answers' ORDER BY link_id LIMIT 1").fetchone()
                details = json.loads(raw)
                details["submitted_text_digest"]["value"] = "0" * 64
                connection.execute("UPDATE links SET details_json=? WHERE link_id=?", (json.dumps(details), identity))
            connection.commit()
    before = path.read_bytes()
    selections = ({}, {"request_ids": ids["requests"]},
                  {"decision_ids": ["missing-decision"] if case == "unresolved" else ids["decisions"]})
    with _context(path, ids["run"]) as reference, _context(path, ids["run"]) as summary:
        for selection in selections:
            answers = list(relations.answer_rows(reference, ids["run"], **selection))
            expected = (len(answers), any(row["text_equal"] is False for row in answers))
            assert expected == (28, case == "mismatch")
            assert relations._answer_summary(summary, ids["run"], **selection) == expected
            assert (summary.unknowns, summary.cutoffs, summary.diagnostics_omitted) == (reference.unknowns, reference.cutoffs, reference.diagnostics_omitted)
        if case == "unresolved":
            assert len(summary.unknowns) == 20 and summary.diagnostics_omitted["unknowns"] == 148
    assert path.read_bytes() == before


@pytest.mark.parametrize("earlier", ["complete", "incomplete", "malformed", "missing-reference", "omitted"])
def test_prefix_minimum_never_replaces_earlier_membership_authority(tmp_path, monkeypatch, earlier):
    path = tmp_path / "authority.sqlite"
    ids = _history(path, 3)
    with closing(sqlite3.connect(path)) as connection:
        connection.row_factory = sqlite3.Row
        link = connection.execute("SELECT * FROM links WHERE relation='answers' ORDER BY witness_event_id").fetchall()
        link = next(row for row in link if row["witness_event_id"] == ids["requests"][1])
        handle = json.loads(link["details_json"])["message_artifact_id"]
        event = connection.execute("SELECT * FROM events WHERE event_id=?", (ids["requests"][0],)).fetchone()
        manifest = json.loads(connection.execute("SELECT body_text FROM artifacts WHERE artifact_id=?", (event["payload_ref"],)).fetchone()[0])
        manifest["message_refs"].append(handle)
        if earlier == "malformed":
            manifest["field_refs"] = "not-an-array"
        if earlier == "missing-reference":
            manifest["message_refs"].append("missing-part")
        _write_artifact(connection, event["payload_ref"], manifest)
        if earlier == "incomplete":
            connection.execute("UPDATE events SET details_json=? WHERE event_id=?", (json.dumps({**json.loads(event["details_json"]), "complete": False}), event["event_id"]))
        if earlier == "omitted":
            connection.execute("UPDATE artifacts SET body_text=NULL,stored_sha256=NULL,stored_bytes=0,omitted_reason='test' WHERE artifact_id=?", (event["payload_ref"],))
        connection.commit()
    authority_reads = []
    original = relations.request_info
    def request_info(ctx, run_id, request_id):
        authority_reads.append(request_id)
        return original(ctx, run_id, request_id)
    with monkeypatch.context() as patch:
        patch.setattr(relations, "request_info", request_info)
        with _context(path, ids["run"]) as ctx:
            status = relations.relation_state(ctx, ids["run"], link)
            assert status == (("fail", "contradictory_first_witness_order") if earlier == "complete" else ("pass", None))
    if earlier != "omitted":
        assert ids["requests"][0] in authority_reads
    expected = reader.check(path, ids["run"], max_output_bytes=1_048_576)
    with monkeypatch.context() as patch:
        patch.setattr(common, "_CACHE_MAX_ENTRIES", 0)
        assert reader.check(path, ids["run"], max_output_bytes=1_048_576) == expected


def test_prefix_handles_nonmonotonic_and_equal_sequence_witnesses(tmp_path):
    path = tmp_path / "prefix.sqlite"
    ids = _history(path, 4)
    with _context(path, ids["run"]) as ctx:
        members = list(relations.member_rows(ctx, ids["run"], request_ids=[ids["requests"][1]]))
        handle = members[-1]["message_artifact_id"]
        first = relations.event_row(ctx, ids["run"], ids["requests"][1], observed=True)
        later = relations.event_row(ctx, ids["run"], ids["requests"][-1], observed=True)
        assert relations._earlier_binding_member(ctx, ids["run"], later, handle)
        assert not relations._earlier_binding_member(ctx, ids["run"], first, handle)
        assert not relations._earlier_binding_member(ctx, ids["run"], {"sequence": first["sequence"]}, handle)
        assert ctx._member_through[ids["run"]] == later["sequence"]
        assert not ctx._cache.minima_disabled


@pytest.mark.parametrize("bound", ["eviction", "refusal"])
def test_prefix_disables_permanently_when_a_minimum_is_forgotten(tmp_path, bound):
    path = tmp_path / "bounded-prefix.sqlite"
    ids = _history(path, 4)
    with _context(path, ids["run"]) as ctx:
        handle = list(relations.member_rows(ctx, ids["run"], [ids["requests"][1]]))[-1]["message_artifact_id"]
        first = relations.event_row(ctx, ids["run"], ids["requests"][1], observed=True)
        later = relations.event_row(ctx, ids["run"], ids["requests"][-1], observed=True)
        if bound == "refusal":
            ctx._cache.max_entry_bytes = 1
        else:
            assert not relations._earlier_binding_member(ctx, ids["run"], first, handle)
            ctx._cache.max_entries = 1
            ctx._cache.store(("event", ids["run"], "force", False), None)
        assert relations._earlier_binding_member(ctx, ids["run"], later, handle)
        assert ctx._cache.minima_disabled
        ctx._cache.max_entry_bytes = 1_048_576
        ctx._cache.max_entries = 4096
        assert relations._earlier_binding_member(ctx, ids["run"], later, handle)
        assert ctx._cache.minima_disabled, "raising the bound cannot restore forgotten coverage"
        assert ctx._cache.bytes <= ctx._cache.max_bytes


def test_two_relative_bindings_and_repeated_handle_keep_order_and_pointers(tmp_path, monkeypatch):
    path = tmp_path / "relative.sqlite"
    with _recording(path) as recorder:
        with recorder.turn("relative pointers") as turn:
            _, decision, _, end = _seed(recorder)
            message = {"content": "answer", "nested": {"text": "answer"}}
            bindings = [{"decision_event_id": decision.event_id, "result_event_id": end.event_id,
                         "pointer": pointer, "text_paths": [""]} for pointer in ("/content", "/nested/text")]
            first = _observe(recorder, "model.request", {"messages": [message, message]}, messages=[{"bindings": bindings}] * 2)
            _observe(recorder, "model.response", {}, model_call_id=first.model_call_id)
            second = _observe(recorder, "model.request", {"messages": [message]}, messages=[{"bindings": bindings}])
            _observe(recorder, "model.response", {}, model_call_id=second.model_call_id)
            run_id = recorder.capture_scope().run_id
    with _context(path, run_id) as ctx:
        answers = list(relations.answer_rows(ctx, run_id))
        assert [row["target_pointer"] for row in answers] == ["/messages/0/content", "/messages/0/nested/text", "/messages/1/content", "/messages/1/nested/text", "/messages/0/content", "/messages/0/nested/text"]
        assert {row["binding_witness_event_id"] for row in answers} == {first.event_id}
        assert len({row["link_id"] for row in answers}) == 2
        assert all(row["text_equal"] is True and row["result_event_id"] == end.event_id for row in answers)
    expected = reader.explain(path, run_id, turn.receipt.turn_id, max_output_bytes=1_048_576)
    graph = reader.slice(path, run_id, second.event_id, max_output_bytes=1_048_576)
    with monkeypatch.context() as patch:
        patch.setattr(common, "_CACHE_MAX_ENTRIES", 1)
        assert reader.explain(path, run_id, turn.receipt.turn_id, max_output_bytes=1_048_576) == expected
        assert reader.slice(path, run_id, second.event_id, max_output_bytes=1_048_576) == graph


def test_reader_cache_caps_missing_rows_and_manifest_oversize_bypass(tmp_path, monkeypatch):
    path = tmp_path / "cache.sqlite"
    ids = _history(path, 3)
    with _context(path, ids["run"]) as ctx:
        queries = []
        ctx.connection.set_trace_callback(queries.append)
        assert relations.event_row(ctx, ids["run"], "missing") is None
        assert relations.event_row(ctx, ids["run"], "missing") is None
        assert sum("event_id='missing'" in query for query in queries) == 1
        event = relations.event_row(ctx, ids["run"], ids["requests"][0])
        manifest = relations.artifact_row(ctx, ids["run"], event["payload_ref"])
        assert manifest["representation"] == "request_refs/2"
        assert ("artifact", ids["run"], event["payload_ref"]) not in ctx._cache.entries
        part_id = json.loads(manifest["body_text"])["message_refs"][0]
        ctx._cache.max_entry_bytes = 1
        assert relations.artifact_row(ctx, ids["run"], part_id) is not None
        assert ("artifact", ids["run"], part_id) not in ctx._cache.entries
    with monkeypatch.context() as patch:
        patch.setattr(common, "_CACHE_MAX_BYTES", 1024)
        patch.setattr(common, "_CACHE_MAX_ENTRIES", 2)
        patch.setattr(common, "_CACHE_ENTRY_BYTES", 600)
        with _context(path, ids["run"]) as ctx:
            for index in range(30):
                relations.event_row(ctx, ids["run"], "absent-" + str(index))
                assert ctx._cache.bytes <= 1024 and len(ctx._cache.entries) <= 2
            assert ctx._cache.high_water_bytes <= 1024 and ctx._cache.high_water_entries <= 2
            assert all(cost <= 600 for _, cost in ctx._cache.entries.values())
            assert len(ctx._cache.entries) > 0, "bounds test must actually retain entries"


def test_raw_observed_run_and_file_cache_keys_do_not_alias(tmp_path):
    path = tmp_path / "isolation.sqlite"
    ids = _history(path, 2)
    identity = ids["requests"][0]
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("UPDATE events SET provenance='declared' WHERE event_id=?", (identity,))
        connection.commit()
    with _context(path, ids["run"]) as ctx:
        assert relations.event_row(ctx, "other-run", identity) is None
        assert relations.event_row(ctx, ids["run"], identity, observed=True) is None
        raw = relations.event_row(ctx, ids["run"], identity)
        assert raw["provenance"] == "declared"
        assert len([key for key in ctx._cache.entries if key[0] == "event"]) == 3
    other = tmp_path / "other.sqlite"
    shutil.copyfile(path, other)
    with closing(sqlite3.connect(other)) as connection:
        connection.execute("UPDATE events SET name='other-file' WHERE event_id=?", (identity,))
        connection.commit()
    with _context(path, ids["run"]) as first, _context(other, ids["run"]) as second:
        assert relations.event_row(first, ids["run"], identity)["name"] is None
        assert relations.event_row(second, ids["run"], identity)["name"] == "other-file"


@pytest.mark.parametrize("missing", [False, True], ids=["present", "cached-none"])
def test_warm_cache_deadline_returns_no_partial_data_and_next_call_is_fresh(tmp_path, monkeypatch, missing):
    path = tmp_path / "deadline.sqlite"
    ids = _history(path, 2)
    clock = [100.0]
    contexts = []
    with monkeypatch.context() as patch:
        patch.setattr(common, "time", SimpleNamespace(monotonic=lambda: clock[0]))
        def work(ctx):
            contexts.append(ctx)
            identity = "missing" if missing else ids["requests"][0]
            assert (relations.event_row(ctx, ids["run"], identity) is None) == missing
            assert ctx._cache.entries
            clock[0] = 111.0
            relations.event_row(ctx, ids["run"], identity)
            raise AssertionError("deadline must precede a warm cache hit")
        result = common.run_read("check", path, {"run_id": ids["run"]}, work, max_output_bytes=65536, no_content=False)
        assert not result["ok"] and result["data"] is None and result["error"]["code"] == "query_timeout"
        assert contexts[0]._cache.entries == {} and contexts[0]._cache.bytes == 0
        with pytest.raises(sqlite3.ProgrammingError):
            contexts[0].connection.execute("SELECT 1")
        clock[0] = 100.0
        assert reader.check(path, ids["run"])["ok"]


def test_live_read_retains_snapshot_and_later_public_call_sees_commit(tmp_path):
    path = tmp_path / "live.sqlite"
    with _recording(path) as recorder:
        _observe(recorder, "cache.served", {}, owner="host")
        _drain(recorder)
        read_started, resume = threading.Event(), threading.Event()
        output = {}
        def work(ctx):
            count = ctx.connection.execute("SELECT count(*) FROM events").fetchone()[0]
            read_started.set()
            assert resume.wait(5)
            return {"before": count, "after": ctx.connection.execute("SELECT count(*) FROM events").fetchone()[0]}
        def inspect_live():
            output["result"] = common.run_read("check", path, {"run_id": recorder.capture_scope().run_id}, work,
                                               max_output_bytes=65536, no_content=False)
        thread = threading.Thread(target=inspect_live)
        thread.start()
        try:
            assert read_started.wait(5)
            _observe(recorder, "cache.served", {}, owner="host")
            _drain(recorder)
            resume.set()
            thread.join(5)
            assert not thread.is_alive()
        finally:
            resume.set()
            thread.join(5)
        assert output["result"]["ok"] and output["result"]["data"] == {"before": 1, "after": 1}
        fresh = reader.query(path, "SELECT count(*) FROM events")
        assert fresh["ok"] and fresh["data"]["rows"] == [[2]]


def _call_route(route, path, ids, **options):
    if route == "check":
        return reader.check(path, ids["run"], **options)
    if route == "turns":
        return reader.find_turns(path, ids["run"], **options)
    if route == "explain":
        return reader.explain(path, ids["run"], ids["turn"], **options)
    if route == "payload":
        return reader.payload(path, ids["run"], ids["artifact"], **options)
    return reader.slice(path, ids["run"], ids["requests"][-1], **options)


def _route_args(route, path, ids):
    base = [route, str(path)]
    if route == "explain":
        base += ["--turn", ids["turn"]]
    elif route == "payload":
        base.append(ids["artifact"])
    elif route == "slice":
        base.append(ids["requests"][-1])
    return base


@pytest.mark.parametrize("route", ["check", "turns", "explain", "payload", "slice"])
def test_scoped_timeout_invalid_before_open_for_api_and_installed_dispatch(tmp_path, monkeypatch, capsys, route):
    missing = tmp_path / "missing.sqlite"
    ids = {"run": "r", "turn": "t", "artifact": "a", "requests": ["e"]}
    def forbidden(*args, **kwargs):
        raise AssertionError("invalid timeout must not open a recording or infer its run")
    monkeypatch.setattr(common, "open_read", forbidden)
    for value in (True, False, 0, -1, 31, float("nan"), float("inf"), -float("inf")):
        result = _call_route(route, missing, ids, timeout=value)
        assert not result["ok"] and result["error"]["reason"] == "invalid_timeout"
        monkeypatch.setattr(sys, "argv", ["fr", *_route_args(route, missing, ids), "--timeout=" + str(value)])
        assert cli.main() == 1
        output = capsys.readouterr()
        assert not output.err
        refusal = json.loads(output.out)
        assert not refusal["ok"] and refusal["error"]["reason"] in ("invalid_timeout", "invalid_cli")
    assert not missing.exists()


@pytest.mark.parametrize("route", ["check", "turns", "explain", "payload", "slice"])
def test_scoped_timeout_reaches_both_reads_and_real_cli_defaults(tmp_path, monkeypatch, capsys, route):
    path = tmp_path / "timeouts.sqlite"
    ids = _history(path, 3)
    ids["artifact"] = _read(path, "SELECT payload_ref FROM events WHERE event_id=?", (ids["results"][0],))[0][0]
    original = common.Context.__init__
    budgets = []
    def init(self, operation, scope, max_output_bytes=65536, no_content=False, timeout=10.0):
        budgets.append((scope.get("run_id"), timeout))
        original(self, operation, scope, max_output_bytes, no_content, timeout)
    with monkeypatch.context() as patch:
        patch.setattr(common.Context, "__init__", init)
        for explicit in (False, True):
            budgets.clear()
            args = _route_args(route, path, ids) + ["--timeout", "30"]
            if explicit:
                args += ["--run", ids["run"]]
            patch.setattr(sys, "argv", ["fr", *args])
            code = cli.main()
            output = capsys.readouterr()
            actual = json.loads(output.out)
            assert not output.err and actual["ok"] and code == cli.exit_status(actual)
            assert budgets == ([(ids["run"], 30.0)] if explicit else [(None, 30.0), (ids["run"], 30.0)])
    for explicit, timeout in ((False, None), (True, 30), (False, 1)):
        command = [str(Path(sys.executable).with_name("fr")), *_route_args(route, path, ids)]
        if explicit:
            command += ["--run", ids["run"]]
        if timeout is not None:
            command += ["--timeout", str(timeout)]
        result = subprocess.run(command, cwd=tmp_path, text=True, capture_output=True, timeout=15)
        actual = json.loads(result.stdout)
        assert not result.stderr and actual["ok"]
        assert result.returncode == cli.exit_status(actual)
        assert actual == _call_route(route, path, ids, **({} if timeout is None else {"timeout": timeout}))


def test_timeout_discovery_matches_public_operation_defaults():
    data = reader.schema()["data"]
    for name in ("runs", "check", "find_turns", "explain", "payload", "slice"):
        assert data["operations"][name]["optional_defaults"]["timeout"] == 10.0
        parameter = signatures.signature(getattr(reader, name)).parameters["timeout"]
        assert parameter.default == 10.0 and parameter.kind == signatures.Parameter.KEYWORD_ONLY
    assert data["limits"]["scoped_reader_timeout"] == {"default": 10.0, "min_exclusive": 0, "max": 30}
    assert data["limits"]["query_snapshot_timeout"] == {"default": 10.0, "min_exclusive": 0, "max": 30}
    assert data["limits"]["db_deadline_seconds"] == 10
    assert data["operations"]["query"]["optional_defaults"]["timeout"] == 10.0
    assert data["operations"]["snapshot"]["optional_defaults"] == {"timeout": 10.0}


def test_default_one_mib_entry_limit_bypasses_large_retained_artifact(tmp_path):
    path = tmp_path / "oversize.sqlite"
    ids = _history(path, 2)
    with closing(sqlite3.connect(path)) as connection:
        identity = connection.execute("SELECT artifact_id FROM artifacts WHERE representation='json' LIMIT 1").fetchone()[0]
        _write_artifact(connection, identity, {"content": "x" * 1_048_576})
        connection.commit()
    with _context(path, ids["run"]) as ctx:
        assert (ctx._cache.max_bytes, ctx._cache.max_entries, ctx._cache.max_entry_bytes) == (8_388_608, 4096, 1_048_576)
        first = relations.artifact_row(ctx, ids["run"], identity)
        assert len(first["body_text"]) > 1_048_576
        assert tuple(relations.artifact_row(ctx, ids["run"], identity)) == tuple(first)
        assert ("artifact", ids["run"], identity) not in ctx._cache.entries
        assert ctx._cache.bytes == 0


@pytest.mark.parametrize("failure", ["deadline", "storage"])
def test_interrupted_prefix_never_publishes_partial_coverage(tmp_path, monkeypatch, failure):
    path = tmp_path / "prefix-failure.sqlite"
    ids = _history(path, 5)
    clock, observed = [100.0], {}
    with monkeypatch.context() as patch:
        patch.setattr(common, "time", SimpleNamespace(monotonic=lambda: clock[0]))
        def work(ctx):
            observed["context"] = ctx
            witness = relations.event_row(ctx, ids["run"], ids["requests"][-1], observed=True)
            store = ctx._cache.store
            def interrupt(key, value):
                store(key, value)
                if key[0] == "member_min" and not observed.get("interrupted"):
                    observed["interrupted"] = True
                    if failure == "deadline":
                        clock[0] = 111.0
                    else:
                        ctx.connection.interrupt()
            ctx._cache.store = interrupt
            try:
                relations._earlier_binding_member(ctx, ids["run"], witness, "not-present")
            except (common.InspectionError, sqlite3.Error):
                observed["watermarks"] = dict(ctx._member_through)
                raise
            raise AssertionError("an interrupted prefix cannot prove absence")
        result = common.run_read("check", path, {"run_id": ids["run"]}, work, max_output_bytes=65536, no_content=False)
    assert observed["interrupted"] and observed["watermarks"] == {}
    assert not result["ok"] and result["data"] is None
    assert result["error"]["code"] == ("query_timeout" if failure == "deadline" else "recording_unreadable")
    ctx = observed["context"]
    assert not ctx._cache.entries and not ctx._member_through
    with pytest.raises(sqlite3.ProgrammingError):
        ctx.connection.execute("SELECT 1")
    assert reader.check(path, ids["run"])["ok"]




# Pre-repair relation is an explicit semantic oracle, not derived from candidate SQL.
_R9_ORIGINAL_MEMBERS_SQL = "\nSELECT e.run_id,e.event_id AS request_event_id,e.turn_id,e.model_call_id,\n       e.sequence AS request_sequence,e.payload_ref AS manifest_ref,\n       CAST(m.key AS INTEGER) AS message_index,\n       json_extract(CASE WHEN json_valid(a.body_text) THEN a.body_text ELSE '{}' END,'$.message_path') AS message_path,\n       json_extract(CASE WHEN json_valid(a.body_text) THEN a.body_text ELSE '{}' END,'$.message_path') || '/' || m.key AS message_pointer,\n       m.value AS message_artifact_id,p.artifact_id IS NULL AS artifact_missing,\n       p.omitted_reason,p.redacted,p.truncated\nFROM observed_events e\nJOIN artifacts a ON a.run_id=e.run_id AND a.artifact_id=e.payload_ref\nJOIN json_each(CASE WHEN json_valid(a.body_text) THEN a.body_text ELSE '{}' END,'$.message_refs') m\nLEFT JOIN artifacts p ON p.run_id=e.run_id AND p.artifact_id=m.value\nWHERE e.kind='model.request' AND a.representation='request_refs/2'\n  AND json_extract(CASE WHEN json_valid(a.body_text) THEN a.body_text ELSE '{}' END,'$.format')='request_refs/2'\n  AND m.type='text'\n"


def _r9_occurrence_snapshot(path, run_id, **selection):
    """Capture full typed rows, ordering, multiplicity and emitted diagnostics."""
    with _context(path, run_id) as ctx:
        rows = []
        for row, state, result_id, equal, reason in relations._answer_states(
                ctx, run_id, **selection):
            typed = tuple((type(value).__name__, value) for value in tuple(row))
            rows.append((tuple(row.keys()), typed, state, result_id, equal, reason))
        return rows, (ctx.unknowns, ctx.cutoffs, ctx.diagnostics_omitted)


def _r9_compare_original_members(path, run_id, monkeypatch, **selection):
    with monkeypatch.context() as patch:
        patch.setattr(relations, "MEMBERS_SQL", _R9_ORIGINAL_MEMBERS_SQL)
        expected = _r9_occurrence_snapshot(path, run_id, **selection)
    actual = _r9_occurrence_snapshot(path, run_id, **selection)
    assert actual == expected
    return actual


@pytest.mark.parametrize("body", [None, "{", "null", "[]", "1", '"x"',
    '{"format":"other","message_refs":["x"]}'],
    ids=["omitted", "malformed", "json-null", "array", "number", "string", "wrong-format"])
def test_member_path_guard_preserves_invalid_and_nonmanifest_bodies(tmp_path, monkeypatch, body):
    path = tmp_path / "guard-body.sqlite"
    ids = _history(path, 4)
    with closing(sqlite3.connect(path)) as connection:
        if body is None:
            connection.execute("UPDATE artifacts SET body_text=NULL,stored_sha256=NULL,stored_bytes=0,omitted_reason='review_omitted' WHERE representation='request_refs/2'")
        else:
            connection.execute("UPDATE artifacts SET body_text=? WHERE representation='request_refs/2'", (body,))
        connection.commit()
    before = path.read_bytes()
    actual, _diagnostics = _r9_compare_original_members(path, ids["run"], monkeypatch)
    assert actual == [], "Invalid/nonmanifest bodies cannot manufacture members"
    # These are the real consumers; an isolated expression is insufficient.
    for operation in (reader.check, reader.find_turns):
        with monkeypatch.context() as patch:
            patch.setattr(relations, "MEMBERS_SQL", _R9_ORIGINAL_MEMBERS_SQL)
            patch.setattr(checks, "MEMBERS_SQL", _R9_ORIGINAL_MEMBERS_SQL)
            expected = operation(path, ids["run"])
        assert operation(path, ids["run"]) == expected
    assert path.read_bytes() == before


@pytest.mark.parametrize("message_path", [
    None, True, 7, 0.30000000000000004, {"nested": 1}, ["messages"], "/messages",
], ids=["null", "boolean", "integer", "precise-real", "object", "array", "text"])
def test_member_path_guard_preserves_scalar_types(tmp_path, monkeypatch, message_path):
    path = tmp_path / "guard-path-type.sqlite"
    ids = _history(path, 4)
    with closing(sqlite3.connect(path)) as connection:
        manifests = connection.execute(
            "SELECT artifact_id,body_text FROM artifacts WHERE representation='request_refs/2'"
        ).fetchall()
        for artifact_id, body in manifests:
            value = json.loads(body)
            value["message_path"] = message_path
            _write_artifact(connection, artifact_id, value)
        connection.commit()
    before = path.read_bytes()
    rows, _diagnostics = _r9_compare_original_members(path, ids["run"], monkeypatch)
    assert len(rows) == 6, "Malformed path values must retain occurrence multiplicity"
    assert path.read_bytes() == before


@pytest.mark.parametrize("selection", [
    "all", "request-subset", "decision-subset", "empty-requests", "empty-decisions", "limit",
])
def test_member_path_guard_preserves_selection_order_and_multiplicity(
        tmp_path, monkeypatch, selection):
    path = tmp_path / "guard-selection.sqlite"
    ids = _history(path, 5)
    kwargs = {
        "all": {},
        "request-subset": {"request_ids": ids["requests"][1::2]},
        "decision-subset": {"decision_ids": ids["decisions"][::2]},
        "empty-requests": {"request_ids": []},
        "empty-decisions": {"decision_ids": []},
        "limit": {"limit": 3},
    }[selection]
    rows, diagnostics = _r9_compare_original_members(path, ids["run"], monkeypatch, **kwargs)
    expected_counts = {"all": 10, "request-subset": 4, "decision-subset": 6,
                       "empty-requests": 0, "empty-decisions": 0, "limit": 3}
    assert len(rows) == expected_counts[selection]
    if selection.startswith("empty-"):
        assert diagnostics[0] == []


def test_member_path_guard_preserves_duplicate_artifact_rows(tmp_path, monkeypatch):
    path = tmp_path / "guard-duplicate-manifest.sqlite"
    ids = _history(path, 4)
    with closing(sqlite3.connect(path)) as connection:
        view_sql = dict(connection.execute(
            "SELECT name,sql FROM sqlite_master WHERE name IN ('request_members','answer_occurrences')"
        ))
        connection.execute("DROP VIEW answer_occurrences")
        connection.execute("DROP VIEW request_members")
        connection.execute("ALTER TABLE artifacts RENAME TO original_artifacts")
        connection.execute("CREATE TABLE artifacts AS SELECT * FROM original_artifacts")
        connection.execute("""INSERT INTO artifacts SELECT * FROM original_artifacts
                              WHERE representation='request_refs/2'""")
        connection.execute(view_sql["request_members"])
        connection.execute(view_sql["answer_occurrences"])
        connection.commit()
    before = path.read_bytes()
    rows, _diagnostics = _r9_compare_original_members(path, ids["run"], monkeypatch)
    assert len(rows) == 12, "Two artifact rows remain two rows, not one or four"
    assert path.read_bytes() == before


def test_answer_decision_selectors_preserve_complete_ordered_occurrences(tmp_path):
    """Select complete typed occurrences without consuming an iterator twice."""
    path = tmp_path / "decision-selectors.sqlite"
    ids = _history(path, 4)
    first_decision = ids["decisions"][0]
    selectors = ([first_decision], (value for value in [first_decision]),
                 [first_decision, first_decision])
    snapshots = [_r9_occurrence_snapshot(path, ids["run"], decision_ids=selector)
                 for selector in selectors]
    expected = [(ids["run"], request, first_decision, ids["results"][0],
                 "/messages/2/content") for request in ids["requests"][1:]]
    for snapshot in snapshots:
        facts = []
        for names, typed, _state, result_id, _equal, _reason in snapshot[0]:
            row = dict(zip(names, (value for _kind, value in typed)))
            facts.append((row["run_id"], row["request_event_id"],
                          row["source_event_id"], result_id, row["target_pointer"]))
        assert facts == expected, "The first decision occurs exactly once in each later request, in order"
        assert snapshot == snapshots[0], "List, generator and repeated IDs preserve every typed row and diagnostic"

    all_snapshot = _r9_occurrence_snapshot(path, ids["run"], decision_ids=None)
    all_facts = []
    for names, typed, _state, result_id, _equal, _reason in all_snapshot[0]:
        row = dict(zip(names, (value for _kind, value in typed)))
        all_facts.append((row["request_event_id"], row["source_event_id"],
                          result_id, row["target_pointer"]))
    all_expected = [(ids["requests"][request_index], ids["decisions"][decision_index],
                     ids["results"][decision_index], f"/messages/{2 * decision_index + 2}/content")
                    for request_index in range(1, 4) for decision_index in range(request_index)]
    assert all_facts == all_expected, "None retains all six independently known history occurrences"
    snapshots.append(all_snapshot)

    for selector in ([], (value for value in []), ["nonexistent-decision"]):
        empty = _r9_occurrence_snapshot(path, ids["run"], decision_ids=selector)
        assert empty[0] == [], "Empty and nonexistent decisions cannot manufacture occurrences"
        snapshots.append(empty)
    limited = _r9_occurrence_snapshot(path, ids["run"], request_ids=ids["requests"][-2:],
                                      decision_ids=[first_decision], limit=1)
    assert limited[0] == [snapshots[0][0][1]], "Combined request selection and LIMIT retain requests[-2] with all row fields"
    snapshots.append(limited)

    for rows, diagnostics in snapshots:
        assert diagnostics == ([], [], {"unknowns": 0, "cutoffs": 0}), "Healthy selected history has no diagnostics or omitted notices"
        assert all(type(value) is int for value in diagnostics[2].values()), "Diagnostic counters retain integer scalar types"
        for _names, _typed, state, result_id, equal, reason in rows:
            assert type(state) is str and state == "linked", "Every selected occurrence retains its linked state and type"
            assert type(result_id) is str and result_id in ids["results"], "Every result retains a recorded string identity"
            assert equal is True and reason is None, "Healthy comparisons remain boolean true with no reason"


def test_answer_decision_selectors_preserve_sqlite_variable_capacity(tmp_path):
    """Reuse run and decision slots within the native eight-variable budget."""
    path = tmp_path / "decision-variable-limit.sqlite"
    ids = _history(path, 4)
    with _context(path, ids["run"]) as ctx:
        if (not hasattr(ctx.connection, "setlimit") or not hasattr(ctx.connection, "getlimit")
                or not hasattr(sqlite3, "SQLITE_LIMIT_VARIABLE_NUMBER")):
            pytest.skip("Native SQLite variable-limit test API is unavailable")
        category = sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER
        previous_limit = ctx.connection.setlimit(category, 8)
        try:
            assert ctx.connection.getlimit(category) == 8, "The actual Context connection must enforce eight variable slots"
            snapshots = []
            selectors = (("list", ids["decisions"]),
                         ("generator", (value for value in ids["decisions"])), ("empty", []))
            for name, selector in selectors:
                rows, facts = [], []
                for row, state, result_id, equal, reason in relations._answer_states(
                        ctx, ids["run"], request_ids=ids["requests"][-2:],
                        decision_ids=selector, limit=1):
                    typed = tuple((type(value).__name__, value) for value in tuple(row))
                    rows.append((tuple(row.keys()), typed, state, result_id, equal, reason))
                    facts.append((row["run_id"], row["request_event_id"], row["source_event_id"],
                                  result_id, row["target_pointer"]))
                    assert type(state) is str and state == "linked", "The bounded query preserves linked occurrence state"
                    assert type(result_id) is str and equal is True and reason is None, "The bounded query preserves typed result/comparison truth"
                expected = [] if name == "empty" else [(ids["run"], ids["requests"][-2],
                    ids["decisions"][0], ids["results"][0], "/messages/2/content")]
                assert facts == expected, "Run + two requests + four decisions + LIMIT fit eight slots without changing selection"
                diagnostics = (list(ctx.unknowns), list(ctx.cutoffs), dict(ctx.diagnostics_omitted))
                assert diagnostics == ([], [], {"unknowns": 0, "cutoffs": 0}), "Successful capacity checks emit no hidden diagnostics"
                assert all(type(value) is int for value in diagnostics[2].values()), "Omitted-notice counters remain integers"
                snapshots.append((rows, diagnostics))
            assert snapshots[0] == snapshots[1], "List and one-shot generator preserve complete typed ordered rows under the same native limit"
            assert snapshots[2][0] == [], "An empty selector stays empty under the native limit"
        finally:
            ctx.connection.setlimit(category, previous_limit)
