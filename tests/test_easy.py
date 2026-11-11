"""SDK-free public host integration, bounded replay and explicit selection."""
import asyncio
from contextlib import closing
from copy import deepcopy
import json
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

from mcp_flightrecorder.easy import AgentRecorder, IntegrationError
from mcp_flightrecorder import MISSING
from mcp_flightrecorder._types import CaptureResult


def rows(path, table='events'):
    with closing(sqlite3.connect(path)) as db:
        db.row_factory = sqlite3.Row
        return [dict(row) for row in db.execute('SELECT * FROM ' + table)]


def choice(conv, payload=None):
    with conv.model_call(payload or {'messages': []}) as call:
        native = {'content': 'host parses this'}
        assert call.done(native) is native
    return call.choice('echo', {'text': 'alpha'}, raw='host parses this')


def selected(conv, *, value='alpha', key=MISSING, prepared=False, message=None):
    """A real local invocation; no synthetic MCP operation is introduced."""
    selected_choice = choice(conv)
    with selected_choice.run(local=True) as attempt:
        assert attempt.call(lambda: value) == value
    message = {'role': 'tool', 'content': value} if message is None else message
    assert attempt.answer(message, key=key, prepared=prepared) is message
    return message, attempt


def replay(conv, messages, keys=None):
    with conv.model_call({'messages': messages}, message_keys=keys) as call:
        call.done({'content': 'ok'})


def test_inert_core_import_and_disabled_host(tmp_path):
    destination = tmp_path / 'inert.sqlite'
    rec = AgentRecorder(destination)
    assert not destination.exists()
    code = "import sys; from mcp_flightrecorder.easy import AgentRecorder; assert not any(x in sys.modules for x in ['openai','mcp','httpx2'])"
    assert subprocess.run([sys.executable, '-I', '-c', code], cwd=tmp_path).returncode == 0
    with AgentRecorder() as disabled:
        conv = disabled.conversation('chat')
        with conv.turn('one'):
            message, attempt = selected(conv)
            replay(conv, [message])
    assert not destination.exists()
    assert conv.binding_count == conv.prepared_bytes == 0
    assert disabled.finalize() is None


def test_manual_local_native_identity_and_cross_turn_replay(tmp_path):
    path = tmp_path / 'local.sqlite'
    native = {'value': ['alpha']}
    with AgentRecorder(path, capture='content', all_model_calls_recorded=True) as rec:
        conv = rec.conversation('chat')
        with conv.turn('one'):
            selected_choice = choice(conv)
            with selected_choice.run(local=True) as attempt:
                assert attempt.call(lambda: native) is native
            message = attempt.answer({'role': 'tool', 'content': 'alpha'})
        with conv.turn('two'):
            replay(conv, [message])
        assert conv.binding_count == 1
    events, links = rows(path), rows(path, 'links')
    assert rec.finalize().portable
    assert not [e for e in events if e['kind'].startswith('mcp.')]
    assert len([e for e in events if e['kind'] == 'host.tool_result']) == 1
    answers = [link for link in links if link['relation'] == 'answers']
    assert len(answers) == 1
    assert json.loads(answers[0]['details_json'])['result_event_id'] is None


@pytest.mark.parametrize('capture', ['content', 'metadata'])
def test_key_rebuilt_stripped_history_and_prepared_snapshot(tmp_path, capture):
    path = tmp_path / 'replay.sqlite'
    with AgentRecorder(path, capture=capture) as rec:
        conv = rec.conversation('chat')
        with conv.turn('one'):
            message, _ = selected(conv, key='answer-1', prepared=True)
        with conv.turn('two'):
            rebuilt = deepcopy(message)
            rebuilt.pop('tool_call_id', None)
            replay(conv, [rebuilt], ['answer-1'])
            rebuilt['content'] = 'changed after binding'
            replay(conv, [rebuilt], ['answer-1'])
        assert conv.prepared_bytes > 0
        conv.close()
        assert conv.binding_count == conv.prepared_bytes == 0
    events, links = rows(path), rows(path, 'links')
    assert len([link for link in links if link['relation'] == 'answers']) == 2
    assert len([link for link in links if link['relation'] == 'inserted_into']) == 1
    requests = [json.loads(e['details_json']) for e in events if e['kind'] == 'model.request']
    assert any('insertion_destination_mismatch' in json.dumps(details) for details in requests)


def test_explicit_missing_key_never_falls_back_to_identity(tmp_path):
    path = tmp_path / 'missing.sqlite'
    with AgentRecorder(path, capture='content') as rec:
        conv = rec.conversation('chat')
        with conv.turn('one'):
            message, _ = selected(conv)
            replay(conv, [message], ['absent-key'])
    assert not [link for link in rows(path, 'links') if link['relation'] == 'answers']
    assert 'binding_key_missing' in json.dumps(rows(path))


def test_key_conflict_stays_unknown_and_never_overwrites(tmp_path):
    path = tmp_path / 'conflict.sqlite'
    with AgentRecorder(path, capture='content') as rec:
        conv = rec.conversation('chat')
        with conv.turn('one'):
            first, _ = selected(conv, key='answer')
            second, _ = selected(conv, key='answer')
            replay(conv, [first, second], ['answer', 'answer'])
        assert conv.binding_count == 1
        assert any(note['reason'] == 'binding_conflict' for note in rec.diagnostics)
    assert not [link for link in rows(path, 'links') if link['relation'] == 'answers']
    assert 'binding_conflict' in json.dumps(rows(path))


def test_overflow_overloaded_role_predicate_stays_unknown(tmp_path):
    path = tmp_path / 'overflow.sqlite'
    with AgentRecorder(path, capture='content', binding_limit=1) as rec:
        conv = rec.conversation('chat', is_tool_message=lambda msg: msg.get('source') == 'tool')
        with conv.turn('one'):
            first, _ = selected(conv, message={'role': 'system', 'content': 'one', 'source': 'tool'})
            second, _ = selected(conv, message={'role': 'system', 'content': 'two', 'source': 'tool'})
            replay(conv, [first, second])
        assert conv.binding_count == 1
        assert any(note['reason'] == 'binding_limit' for note in rec.diagnostics)
    assert len([link for link in rows(path, 'links') if link['relation'] == 'answers']) == 1
    assert 'binding_key_missing' in json.dumps(rows(path))


def test_generation_restart_and_new_conversation_never_borrow(tmp_path):
    rec = AgentRecorder(tmp_path / 'first.sqlite', capture='content').start()
    old = rec.conversation('same')
    with old.turn('one'):
        message, _ = selected(old, key='same')
    rec.close()
    assert old.binding_count == 0
    rec.start(tmp_path / 'second.sqlite')
    new = rec.conversation('same')
    with new.turn('two'):
        replay(new, [message], ['same'])
    rec.close()
    assert not [link for link in rows(tmp_path / 'second.sqlite', 'links') if link['relation'] == 'answers']
    assert 'binding_key_missing' in json.dumps(rows(tmp_path / 'second.sqlite'))


@pytest.mark.parametrize('existing', [False, True])
def test_directory_generations_are_fresh_and_configuration_stays_inert(tmp_path, existing):
    directory = tmp_path / 'recordings with spaces'
    if existing:
        directory.mkdir(mode=0o750)
    destination = directory if existing else str(directory) + '/'
    rec = AgentRecorder(destination, capture='content')
    assert directory.exists() is existing
    if existing:
        assert list(directory.iterdir()) == []
    paths = []
    for index in range(2):
        rec.start()
        path = Path(rec.path)
        assert path.is_absolute() and path.parent == directory
        assert path.suffix == '.sqlite' and path.is_file()
        conv = rec.conversation('generation')
        with conv.turn(str(index)):
            message, _ = selected(conv)
            replay(conv, [message])
        rec.close()
        assert conv.binding_count == 0
        paths.append(path)
    assert paths[0] != paths[1]
    assert len(list(directory.iterdir())) == 2
    assert rows(paths[0], 'runs')[0]['run_id'] != rows(paths[1], 'runs')[0]['run_id']
    assert directory.stat().st_mode & 0o777 == (0o750 if existing else 0o700)
    assert all(path.stat().st_mode & 0o777 == 0o600 for path in paths)
    replacement = tmp_path / 'explicit.sqlite'
    rec.start(replacement).close()
    assert Path(rec.path) == replacement
    with pytest.raises(FileExistsError):
        rec.start()
    rec.start(None).close()
    assert rec.path is None and len(list(directory.iterdir())) == 2


def test_directory_disabled_and_missing_ancestors_do_not_create_paths(tmp_path):
    destination = tmp_path / 'missing' / 'leaf'
    rec = AgentRecorder(str(destination) + '/')
    rec.start(None).close()
    assert not destination.parent.exists()
    with pytest.raises(FileNotFoundError):
        rec.start(str(destination) + '/')
    assert not destination.parent.exists()


@pytest.mark.parametrize('as_path', [False, True])
def test_explicit_relative_destination_preserves_caller_value_and_type(tmp_path, monkeypatch, as_path):
    monkeypatch.chdir(tmp_path)
    destination = Path('relative.sqlite') if as_path else 'relative.sqlite'
    rec = AgentRecorder(destination)
    assert rec.path is destination and not Path(destination).exists()
    rec.start()
    assert rec.path is destination and Path(destination).is_file()
    rec.close()
    with pytest.raises(FileExistsError):
        rec.start()
    assert rec.path is destination
    replacement = 'replacement.sqlite'
    rec.start(replacement).close()
    assert rec.path is replacement and Path(replacement).is_file()


@pytest.mark.parametrize('destination', [17, b'bytes.sqlite'])
def test_invalid_destination_type_retains_value_error(tmp_path, monkeypatch, destination):
    monkeypatch.chdir(tmp_path)
    rec = AgentRecorder(destination)
    with pytest.raises(ValueError, match='path must name a new local recording'):
        rec.start()
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize('suffix', ['', '-wal', '-shm', '-journal'])
def test_facade_explicit_destination_conflicts_preserve_files(tmp_path, suffix):
    destination = tmp_path / 'reserved.sqlite'
    conflict = Path(str(destination) + suffix)
    conflict.write_bytes(b'preexisting bytes')
    with pytest.raises(FileExistsError):
        AgentRecorder(destination).start()
    assert conflict.read_bytes() == b'preexisting bytes'
    assert list(tmp_path.iterdir()) == [conflict]


def test_two_concurrent_conversations_keep_equal_keys_separate(tmp_path):
    path = tmp_path / 'concurrent.sqlite'
    async def host(rec):
        ready = asyncio.Event()
        entered = 0
        async def conversation(name):
            nonlocal entered
            conv = rec.conversation(name)
            with conv.turn(name):
                message, _ = selected(conv, value=name, key='same')
                entered += 1
                if entered == 2:
                    ready.set()
                await ready.wait()
                replay(conv, [deepcopy(message)], ['same'])
        await asyncio.gather(conversation('left'), conversation('right'))
    with AgentRecorder(path, capture='content') as rec:
        asyncio.run(host(rec))
    events = {event['event_id']: event for event in rows(path)}
    links = [link for link in rows(path, 'links') if link['relation'] == 'answers']
    assert len(links) == 2
    assert all(events[link['source_event_id']]['turn_id'] == events[link['target_event_id']]['turn_id'] for link in links)
    assert len({events[link['source_event_id']]['turn_id'] for link in links}) == 2


def test_overlapping_turn_cannot_borrow_outer_scope(tmp_path):
    path = tmp_path / 'overlap.sqlite'
    with AgentRecorder(path, capture='content') as rec:
        conv = rec.conversation('chat')
        with conv.turn('outer'):
            with conv.turn('inner'):
                replay(conv, [])
            replay(conv, [])
        assert any(note['reason'] == 'overlapping_turns' for note in rec.diagnostics)
    assert len([event for event in rows(path) if event['kind'] == 'model.request']) == 1


@pytest.mark.parametrize('strict', [False, True])
def test_ambiguous_choice_counts_all_local_attempts(tmp_path, strict):
    path = tmp_path / 'ambiguous.sqlite'
    with AgentRecorder(path, capture='content', strict=strict) as rec:
        conv = rec.conversation('chat')
        with conv.turn('one'):
            selected_choice = choice(conv)
            for _ in range(2):
                with selected_choice.run(local=True) as attempt:
                    attempt.call(lambda: 'equal')
            message = {'role': 'tool', 'content': 'equal'}
            if strict:
                with pytest.raises(IntegrationError, match='attempt_ambiguous'):
                    selected_choice.answer(message)
            else:
                assert selected_choice.answer(message) is message
                replay(conv, [message])
    assert not [link for link in rows(path, 'links') if link['relation'] == 'answers']


def test_explicit_selected_inner_attempt_and_multi_call_handle(tmp_path):
    path = tmp_path / 'attempts.sqlite'
    with AgentRecorder(path, capture='content') as rec:
        conv = rec.conversation('chat')
        with conv.turn('one'):
            selected_choice = choice(conv)
            with selected_choice.run(local=True) as first:
                first.call(lambda: 'equal')
            with selected_choice.run(local=True) as selected_attempt:
                selected_attempt.call(lambda: 'equal')
            answer = selected_attempt.answer({'role': 'tool', 'content': 'equal'})
            with selected_choice.run(local=True) as multi:
                multi.call(lambda: 'equal')
                multi.call(lambda: 'equal')
            ambiguous = multi.answer({'role': 'tool', 'content': 'equal'})
            replay(conv, [answer, ambiguous])
    assert len([link for link in rows(path, 'links') if link['relation'] == 'answers']) == 1
    assert 'attempt_ambiguous' in json.dumps(rows(path))


@pytest.mark.parametrize('reason', ['ordinary_budget', 'writer_failed', 'closing'])
def test_selected_capture_rejection_never_substitutes_earlier(tmp_path, monkeypatch, reason):
    path = tmp_path / 'rejected.sqlite'
    with AgentRecorder(path, capture='content', strict=True) as rec:
        conv = rec.conversation('chat')
        with conv.turn('one'):
            selected_choice = choice(conv)
            with selected_choice.run(local=True) as first:
                first.call(lambda: 'equal')
            original = rec._recorder.observe
            def reject(kind, **kwargs):
                return CaptureResult('rejected', reason) if kind == 'host.tool_result' else original(kind, **kwargs)
            monkeypatch.setattr(rec._recorder, 'observe', reject)
            with selected_choice.run(local=True) as last:
                assert last.call(lambda: 'equal') == 'equal'
            message = last.answer({'role': 'tool', 'content': 'equal'})
            replay(conv, [message])
    assert not [link for link in rows(path, 'links') if link['relation'] == 'answers']
    assert 'capture_unavailable' in json.dumps(rows(path))


@pytest.mark.parametrize('strict', [False, True])
def test_unsupported_manual_payload_has_actionable_diagnostic(tmp_path, strict):
    with AgentRecorder(tmp_path / 'bad.sqlite', strict=strict) as rec:
        conv = rec.conversation('chat')
        with conv.turn('one'):
            if strict:
                with pytest.raises(IntegrationError, match='messages_unavailable'):
                    with conv.model_call({'messages': object()}):
                        pytest.fail('strict preflight should precede native work')
            else:
                with conv.model_call({'messages': object()}):
                    pass
        assert any(note['reason'] == 'messages_unavailable' and note['hint'] for note in rec.diagnostics)


@pytest.mark.parametrize('strict', [False, True])
def test_host_exception_and_cancellation_remain_authoritative(tmp_path, monkeypatch, strict):
    with AgentRecorder(tmp_path / 'error.sqlite', strict=strict) as rec:
        conv = rec.conversation('chat')
        with conv.turn('one'):
            original = rec._recorder.observe
            def reject_terminal(kind, **kwargs):
                return CaptureResult('rejected', 'invalid_input') if kind == 'model.response' else original(kind, **kwargs)
            monkeypatch.setattr(rec._recorder, 'observe', reject_terminal)
            for error in (RuntimeError('host failure'), asyncio.CancelledError('cancelled')):
                with pytest.raises(type(error)) as caught:
                    with conv.model_call({'messages': []}):
                        raise error
                assert caught.value is error
            selected_choice = conv.host_call('local', {})
            with selected_choice.run(local=True) as attempt:
                error = RuntimeError('native local failure')
                def fail():
                    raise error
                with pytest.raises(RuntimeError) as caught:
                    attempt.call(fail)
                assert caught.value is error


@pytest.mark.parametrize('content', [{'unsupported': True}, [{'type': 'image', 'url': 'x'}]])
def test_unsupported_prepared_content_is_reported(tmp_path, content):
    with AgentRecorder(tmp_path / 'prepared.sqlite') as rec:
        conv = rec.conversation('chat')
        with conv.turn('one'):
            selected(conv, prepared=True, message={'role': 'tool', 'content': content})
        assert any(note['reason'] == 'prepared_content_unavailable' for note in rec.diagnostics)
        assert conv.prepared_bytes == 0


def test_prepared_byte_and_key_bounds(tmp_path):
    with AgentRecorder(tmp_path / 'limits.sqlite', key_byte_limit=4, prepared_byte_limit=4) as rec:
        conv = rec.conversation('chat')
        with conv.turn('one'):
            selected(conv, key='ok', prepared=True)
            selected(conv, key='too-long')
        assert conv.binding_count == 1
        assert conv.prepared_bytes == 0
        assert {'prepared_limit', 'binding_key_invalid'} <= {note['reason'] for note in rec.diagnostics}


def test_configured_key_callback_and_identity_reference_ownership(tmp_path):
    with AgentRecorder(tmp_path / 'identity.sqlite') as rec:
        conv = rec.conversation('chat', key=lambda message: message.get('key'))
        with conv.turn('one'):
            message, _ = selected(conv, message={'role': 'tool', 'content': 'alpha', 'key': 'stable'})
            replay(conv, [deepcopy(message)])
        identity_conv = rec.conversation('identity')
        with identity_conv.turn('one'):
            identity, _ = selected(identity_conv)
        assert identity_conv._bindings[('identity', id(identity))].message is identity
        assert conv._bindings[('key', 'stable')].message is None
        conv.close()
        assert not conv._bindings


def test_local_host_initiated_operation_does_not_invent_model_decision(tmp_path):
    path = tmp_path / 'host.sqlite'
    with AgentRecorder(path, capture='content') as rec:
        conv = rec.conversation('chat')
        with conv.turn('one'):
            with conv.host_call('warmup', {}).run(local=True) as attempt:
                result = attempt.call(lambda: {'ready': True})
            assert result == {'ready': True}
            replay(conv, [attempt.answer({'role': 'tool', 'content': 'ready'})])
    events = rows(path)
    assert not [event for event in events if event['kind'] in ('model.tool_call', 'mcp.request', 'mcp.response')]
    assert not [link for link in rows(path, 'links') if link['relation'] == 'answers']
    assert len([event for event in events if event['kind'] == 'host.dispatch']) == 1


def test_missing_callback_key_cannot_fall_back_to_bound_identity(tmp_path):
    path = tmp_path / 'key-loss.sqlite'
    with AgentRecorder(path, capture='content') as rec:
        conv = rec.conversation('chat', key=lambda message: message.get('key'))
        with conv.turn('one'):
            message, _ = selected(conv, key=None)
            replay(conv, [message])
    assert not [link for link in rows(path, 'links') if link['relation'] == 'answers']
    assert 'binding_key_missing' in json.dumps(rows(path))


def test_strict_overlap_rejection_restores_outer_turn(tmp_path):
    path = tmp_path / 'strict-overlap.sqlite'
    with AgentRecorder(path, capture='content', strict=True) as rec:
        conv = rec.conversation('chat')
        with conv.turn('outer'):
            with pytest.raises(IntegrationError, match='overlapping_turns'):
                with conv.turn('inner'):
                    pass
            replay(conv, [])
    assert len([event for event in rows(path) if event['kind'] == 'model.request']) == 1


def test_predicate_failure_can_only_downgrade_existing_binding(tmp_path):
    path = tmp_path / 'predicate.sqlite'
    def predicate(message):
        raise ValueError('host predicate failure')
    with AgentRecorder(path, capture='content') as rec:
        conv = rec.conversation('chat', is_tool_message=predicate)
        with conv.turn('one'):
            message, _ = selected(conv)
            replay(conv, [message])
    assert not [link for link in rows(path, 'links') if link['relation'] == 'answers']
    assert 'tool_message_predicate_failed' in json.dumps(rows(path))


@pytest.mark.parametrize('strict', [False, True])
def test_invalid_local_context_normal_passthrough_strict_preflight(tmp_path, strict):
    with AgentRecorder(tmp_path / 'invalid-local.sqlite', strict=strict) as rec:
        conv = rec.conversation('chat')
        with conv.turn('one'):
            attempt = conv.host_call('local', {}).run()
            called = []
            def native():
                called.append(True)
                return 'native'
            if strict:
                with pytest.raises(IntegrationError, match='local_attempt_required'):
                    attempt.call(native)
                assert not called
            else:
                assert attempt.call(native) == 'native'
                assert called == [True]


def test_retention_policy_passes_through_and_omits_native_content(tmp_path):
    path = tmp_path / 'retention.sqlite'
    seen = []
    def policy(kind, payload, context):
        seen.append(kind)
        return {'policy_id': 'omit', 'omitted_reason': 'host_policy'}
    with AgentRecorder(path, capture='content', retention_policy=policy) as rec:
        conv = rec.conversation('chat')
        with conv.turn('one'):
            selected(conv)
    assert {'model.request', 'model.response', 'model.tool_call', 'host.tool_result'} <= set(seen)
    artifacts = rows(path, 'artifacts')
    assert any(row['omitted_reason'] == 'host_policy' for row in artifacts)
    protected = {event['payload_ref'] for event in rows(path) if event['kind'] in seen}
    assert all(row['body_text'] is None or row['representation'] == 'request_refs/2'
               for row in artifacts if row['artifact_id'] in protected)


@pytest.mark.parametrize('strict', [False, True])
def test_cleanup_in_foreign_context_preserves_host_error(tmp_path, strict):
    from contextvars import Context
    with AgentRecorder(tmp_path / 'cleanup.sqlite', strict=strict) as rec:
        conv = rec.conversation('chat')
        turn = conv.turn('one')
        turn.__enter__()
        attempt = conv.host_call('local', {}).run(local=True)
        attempt.__enter__()
        error = RuntimeError('authoritative host failure')
        try:
            assert Context().run(attempt.__exit__, type(error), error, None) is False
            assert Context().run(turn.__exit__, type(error), error, None) is False
        finally:
            # The intentionally wrong context cannot restore the owner's tokens.
            from mcp_flightrecorder.easy import _ACTIVE, _TURN
            _ACTIVE.reset(attempt._token)
            _TURN.reset(turn.token)
        assert any(note['reason'] == 'context_cleanup_failed' for note in rec.diagnostics)


def test_unsupported_prepared_cycle_and_surrogate_key_are_diagnostics(tmp_path):
    cycle = []
    cycle.append(cycle)
    with AgentRecorder(tmp_path / 'invalid-values.sqlite') as rec:
        conv = rec.conversation('chat')
        with conv.turn('one'):
            selected(conv, prepared=True, message={'role': 'tool', 'content': cycle})
            selected(conv, key='\ud800')
        assert {'prepared_content_unavailable', 'binding_key_invalid'} <= {note['reason'] for note in rec.diagnostics}


def test_disabled_answer_is_inert_and_does_not_report_capture_loss():
    with AgentRecorder() as rec:
        conv = rec.conversation('disabled')
        with conv.turn('one'):
            selected(conv, prepared=True)
        assert conv.binding_count == 0
        assert rec.diagnostics == ()


def test_late_choice_cannot_attach_dispatch_to_later_turn(tmp_path):
    path = tmp_path / 'late-choice.sqlite'
    with AgentRecorder(path, capture='content') as rec:
        conv = rec.conversation('chat')
        with conv.turn('one'):
            with conv.model_call({'messages': []}) as call:
                call.done({'content': 'decision'})
        with conv.turn('two'):
            late = call.choice('echo', {'text': 'alpha'})
            with late.run(local=True) as attempt:
                assert attempt.call(lambda: 'alpha') == 'alpha'
            replay(conv, [attempt.answer({'role': 'tool', 'content': 'alpha'})])
    assert not [event for event in rows(path) if event['kind'] == 'host.dispatch']
    assert not [link for link in rows(path, 'links') if link['relation'] == 'answers']


@pytest.mark.parametrize('strict', [False, True])
def test_actual_capacity_rejection_preserves_local_execution(tmp_path, strict):
    native = {'result': 'unchanged'}
    with AgentRecorder(tmp_path / 'capacity.sqlite', strict=strict,
                       limits={'ordinary_bytes': 1}) as rec:
        conv = rec.conversation('chat')
        with conv.turn('one'):
            selected_choice = choice(conv)
            with selected_choice.run(local=True) as attempt:
                assert attempt.call(lambda: native) is native
        assert rec.health().reason_counts['capacity'] > 0


@pytest.mark.parametrize('strict', [False, True])
def test_actual_writer_failure_never_controls_native_work(tmp_path, monkeypatch, strict):
    import threading
    with AgentRecorder(tmp_path / 'writer.sqlite', strict=strict, limits={'batch_items': 1}) as rec:
        failed = threading.Event()
        core = rec._recorder
        persist, writer_failure = core._persist_bundle, core._writer_failure
        def fail_write(connection, bundle):
            if bundle.kind == 'model.request':
                raise sqlite3.OperationalError('controlled writer failure')
            return persist(connection, bundle)
        def failure(current):
            writer_failure(current)
            failed.set()
        monkeypatch.setattr(core, '_persist_bundle', fail_write)
        monkeypatch.setattr(core, '_writer_failure', failure)
        conv = rec.conversation('chat')
        with conv.turn('one'):
            with conv.model_call({'messages': []}) as call:
                assert failed.wait(5), 'The actual writer must enter failed state before native continuation'
                response = {'content': 'native response'}
                assert call.done(response) is response
            selected_choice = call.choice('echo', {})
            with selected_choice.run(local=True) as attempt:
                native = {'native': 'result'}
                assert attempt.call(lambda: native) is native
        assert rec.health().writer_errors == 1
        assert rec.health().state == 'failed'
