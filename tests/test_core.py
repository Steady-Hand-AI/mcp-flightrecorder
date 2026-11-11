"""Installed foundation checks: construction, turns, immutable receipts and scope."""
from dataclasses import FrozenInstanceError, replace
from importlib import metadata, resources
import hashlib
import json
from pathlib import Path
import sqlite3

import pytest
from packaging.requirements import Requirement
from pytest_check import check

from mcp_flightrecorder import Recorder, RecorderError


COVERAGE = {'producers': {'model': 'test.model', 'host': 'test.host', 'mcp': 'test.mcp'},
            'model_complete': True}
ROOT = Path(__file__).resolve().parents[1]


def test_installed_foundation_turn_prompt_conditions_and_schema(tmp_path):
    path = tmp_path / 'recording.sqlite'
    prompt = {'question': 'What was submitted?', 'values': [True, 1, None]}
    conditions = {'z': 2, 'case': 'foundation'}
    rec = Recorder(path, capture='content', coverage=COVERAGE, environment={'synthetic': True})
    initial = rec.health()
    with rec.turn(prompt, conversation_id='conversation', conditions=conditions) as turn:
        assert turn.receipt.status == 'accepted', 'A valid turn must be admitted before its prompt can be inspected'
        check.is_none(turn.receipt.event_id, msg='Turn entry is a row transition, not a timeline event')
        check.equal(rec.capture_scope().turn_id, turn.receipt.turn_id, msg='Task-local scope must identify its admitted turn')
    check.is_none(turn.exit_receipt.event_id, msg='Turn exit also must not invent a timeline event')
    check.is_none(rec.capture_scope().turn_id, msg='Exiting a turn restores the previously unscoped caller')
    closed = rec.close()
    assert closed.writer_stopped and closed.portable, 'Foundation recording must join its writer and reopen as a portable file'
    with sqlite3.connect(path.as_uri() + '?mode=ro', uri=True) as reader:
        row = reader.execute('SELECT t.*, a.body_text FROM turns t JOIN artifacts a ON t.prompt_ref=a.artifact_id').fetchone()
        assert row is not None, 'The public turn API must persist its original prompt and turn row'
        columns = [item[0] for item in reader.execute('SELECT * FROM turns').description]
        turn_row = dict(zip(columns, row[:-1]))
        run = reader.execute('SELECT environment_json,policy_json,coverage_json,close_state,dropped_events,writer_errors FROM runs').fetchone()
        assert run is not None, 'The installed writer must create exactly its recording run'
        check.equal(reader.execute("SELECT count(*) FROM sqlite_schema WHERE type='table'").fetchone()[0], 7,
                    msg='Installed capture must preserve the canonical seven-table contract')
        check.equal(reader.execute('PRAGMA user_version').fetchone()[0], 6,
                    msg='The installed resource must retain schema contract version 6')
        check.equal(reader.execute('SELECT count(*) FROM findings').fetchone()[0], 0,
                    msg='A recorder captures evidence without writing evaluation findings')
        check.equal(reader.execute('SELECT count(*) FROM events').fetchone()[0], 0,
                    msg='A no-tool turn must not acquire synthetic timeline observations')
        check.equal(json.loads(row[-1]), prompt, msg='The stored prompt must preserve exposed JSON types and values')
        check.equal(turn_row['condition_key'], hashlib.sha256(json.dumps(conditions, sort_keys=True, ensure_ascii=False,
                    separators=(',', ':'), allow_nan=False).encode()).hexdigest(),
                    msg='Condition identity must follow the independently specified sorted UTF-8 encoding')
        check.equal(turn_row['capture_state'], 'complete', msg='An ended loss-free no-work turn becomes complete only on close')
        check.equal(turn_row['host_outcome'], 'returned', msg='Normal lexical exit must record returned host outcome')
        check.equal(json.loads(run[0]), {'synthetic': True}, msg='Environment must remain the explicitly supplied metadata')
        check.equal(json.loads(run[2]), COVERAGE, msg='Run coverage must preserve fixed producer groups and declared model completeness')
        check.equal(run[3:], ('clean', 0, 0), msg='Loss-free foundation closure must have no dropped events or writer errors')
    check.equal(initial.state, 'accepting', msg='Constructor returns only after the accepting writer initialized successfully')
    health = rec.health()
    check.equal((health.state, health.ordinary.used_slots, health.ordinary.used_bytes,
                 health.completion.used_slots, health.completion.used_bytes), ('stopped', 0, 0, 0, 0),
                msg='Orderly drained close releases both capacity pools while preserving stopped admission')


@pytest.mark.parametrize('suffix', ['', '-wal', '-shm', '-journal'], ids=['main', 'wal', 'shm', 'journal'])
def test_existing_file_or_sidecar_is_never_overwritten(tmp_path, suffix):
    path = tmp_path / 'existing.sqlite'
    existing = Path(str(path) + suffix)
    existing.write_bytes(b'owner evidence')
    # Existing destinations are an explicit public refusal, not recorder initialization failure.
    with pytest.raises(FileExistsError):
        Recorder(path, capture='content', coverage=COVERAGE)
    check.equal(existing.read_bytes(), b'owner evidence', msg='Refusing an existing path must preserve its exact original bytes')
    if suffix:
        check.is_false(path.exists(), msg='A preexisting sidecar must be refused before a new main database is created')


@pytest.mark.parametrize('overrides', [
    {'capture': 'guess'}, {'comparisons': 1}, {'limits': {'ordinary_items': 0}},
    {'limits': {'ordinary_items': True}}, {'limits': {'ordinary_items': 1.5}},
    {'limits': {'unknown': 1}}, {'coverage': {'producers': {'model': 'm', 'host': 'h'}}},
    {'coverage': {'producers': {'model': None, 'host': 'h', 'mcp': 'c'}, 'model_complete': True}},
    {'coverage': {'producers': {'model': '', 'host': 'h', 'mcp': 'c'}}},
], ids=['capture', 'comparison-type', 'zero-limit', 'boolean-limit', 'float-limit', 'unknown-limit',
         'missing-producer', 'contradictory-coverage', 'empty-producer'])
def test_invalid_configuration_fails_before_file_creation(tmp_path, overrides):
    path = tmp_path / 'invalid.sqlite'
    settings = {'capture': 'content', 'coverage': COVERAGE, **overrides}
    # Constructor configuration errors are intentionally hard ValueError prerequisites.
    with pytest.raises(ValueError):
        Recorder(path, **settings)
    check.is_false(path.exists(), msg='Invalid constructor input must not create an apparent recording')


def test_missing_parent_and_storage_initialization_error_are_distinct(tmp_path, monkeypatch):
    # A missing parent is invalid configuration; the recorder must not create directories implicitly.
    with pytest.raises(ValueError):
        Recorder(tmp_path / 'absent' / 'recording.sqlite', capture='content', coverage=COVERAGE)
    from mcp_flightrecorder import _storage
    def unavailable(*args, **kwargs):
        raise sqlite3.OperationalError('controlled initialization failure')
    monkeypatch.setattr(_storage, 'connect', unavailable)
    # A valid new path with unavailable SQLite storage must export RecorderError to the caller.
    with pytest.raises(RecorderError):
        Recorder(tmp_path / 'unavailable.sqlite', capture='content', coverage=COVERAGE)


def test_receipts_and_scopes_are_immutable_and_cross_recorder_scope_rejects(tmp_path):
    rec = Recorder(tmp_path / 'one.sqlite', capture='content', coverage=COVERAGE)
    other = Recorder(tmp_path / 'two.sqlite', capture='content', coverage=COVERAGE)
    try:
        with rec.turn('first') as turn:
            # A frozen receipt must not allow a caller to rewrite admission history.
            with pytest.raises(FrozenInstanceError):
                turn.receipt.status = 'rejected'
            # A frozen scope must not allow accidental attribution mutation.
            with pytest.raises(FrozenInstanceError):
                turn.scope.turn_id = 'invented'
            wrong = other.observe('cache.served', scope=turn.scope, facts={'owner': 'host'})
            forged = replace(turn.scope, turn_id='invented')
            changed = rec.observe('cache.served', scope=forged, facts={'owner': 'host'})
            check.equal((wrong.status, wrong.reason), ('rejected', 'invalid_scope'),
                        msg='A scope issued by another recorder must not cross the run boundary')
            check.equal((changed.status, changed.reason), ('rejected', 'invalid_scope'),
                        msg='A copied scope with altered identity must not become a caller-constructed attribution')
            # Health mappings must be immutable snapshots, not references to live counters.
            with pytest.raises(TypeError):
                rec.health().reason_counts['closed'] = 50
    finally:
        rec.close()
        other.close()


def test_schema_resource_matches_canonical_and_runtime_has_no_dependencies():
    installed = resources.files('mcp_flightrecorder').joinpath('schema.sql').read_bytes()
    check.equal(installed, (ROOT / 'sql/schema.sql').read_bytes(),
                msg='The wheel resource must be a byte-identical build copy of the single canonical schema')
    check.equal(metadata.version('mcp-flightrecorder'), '0.5.22', msg='The installed package matches the source release version')
    requirements = [Requirement(value) for value in metadata.requires('mcp-flightrecorder') or []]
    base = [str(value) for value in requirements if value.marker is None or value.marker.evaluate({'extra': ''})]
    check.equal(base, [],
                msg='Recorder runtime must remain stdlib-only; SDK dependencies activate only with their optional extra')


@pytest.mark.parametrize('timeout', [0, -1, True, float('nan'), float('inf'), 10**400],
                         ids=['zero', 'negative', 'boolean', 'nan', 'infinity', 'unrepresentable'])
def test_timeout_configuration_errors_preserve_accepting_state(tmp_path, timeout):
    rec = Recorder(tmp_path / 'timeout.sqlite', capture='metadata', coverage=COVERAGE)
    try:
        # Invalid close/finalize waits must be configuration errors, not an accidental shutdown.
        with pytest.raises(ValueError):
            rec.close(timeout)
        with pytest.raises(ValueError):
            rec.finalize(timeout)
        check.equal(rec.health().state, 'accepting', msg='Invalid wait configuration must leave the recorder available for a valid close')
    finally:
        rec.close()


def test_large_finite_timeout_remains_a_valid_bounded_platform_wait(tmp_path):
    rec = Recorder(tmp_path / 'large-timeout.sqlite', capture='metadata', coverage=COVERAGE)
    closed = rec.close(1e300)
    repeated = rec.finalize(1e300)
    check.is_true(closed.writer_stopped, msg='A finite positive timeout must not overflow the platform thread-join limit')
    check.is_true(closed.portable, msg='A large caller budget must still finish real SQLite finalization and fresh read')
    check.equal(repeated, closed, msg='An already portable recording must support a repeated finite-budget finalize')


@pytest.mark.parametrize('field', ['dispatch-action', 'link-basis', 'terminal-status', 'terminal-resolution'])
def test_nonstring_enum_inputs_reject_without_running_host_comparisons(tmp_path, field):
    class HostObject:
        def __eq__(self, other):
            raise AssertionError('Capture validation must not execute an unsupported host value')

    rec = Recorder(tmp_path / 'invalid-enum.sqlite', capture='metadata', coverage=COVERAGE)
    try:
        scope = rec.capture_scope()
        value = HostObject()
        if field == 'dispatch-action':
            with rec.dispatch(action=value, name='tool', arguments={}) as dispatch:
                rejected = dispatch.receipt
        elif field == 'link-basis':
            rejected = rec.start_operation(scope=scope, client_id='client', method='tools/call',
                                           arguments={}, link_basis=value)
        else:
            started = rec.start_operation(scope=scope, client_id='client', method='tools/call', arguments={})
            assert started.status == 'accepted', 'The malformed terminal control requires an admitted operation'
            kwargs = {'status': 'ok', 'resolution': 'remote'}
            kwargs['status' if field == 'terminal-status' else 'resolution'] = value
            rejected = rec.finish_operation(started.ticket, **kwargs)
            finished = rec.finish_operation(started.ticket, status='ok', resolution='remote')
            check.equal(finished.status, 'accepted', msg='Rejecting invalid terminal metadata must preserve its usable completion ticket')
        check.equal((rejected.status, rejected.reason), ('rejected', 'invalid_input'),
                    msg='Unsupported enum values must return an invalid-input receipt instead of invoking host comparison code')
        check.equal(rec.health().state, 'accepting', msg='Expected producer validation must not disable further recording')
        check.equal(rec.health().writer_errors, 0, msg='Invalid producer metadata is not a storage failure')
    finally:
        rec.close()
