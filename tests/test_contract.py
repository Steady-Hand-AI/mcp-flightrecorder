"""Synthetic SQLite contract checks; these do not exercise live adapters."""
from pathlib import Path
import hashlib
import json
import sqlite3
import pytest
import pytest_check as check

ROOT = Path(__file__).resolve().parents[1]


class TestContract:
    @pytest.fixture(autouse=True)
    def database(self):
        self.db = sqlite3.connect(':memory:')
        self.db.row_factory = sqlite3.Row
        self.db.executescript((ROOT / 'sql/schema.sql').read_text())
        self.sequence = 0
        self.insert('runs', run_id='r', started_at='2026-10-20T00:00:00Z',
                    environment_json='{}', policy_json='{}',
                    coverage_json=json.dumps({'producers': {b: b for b in ('model','host','mcp')}}))
        self.turn('t')

        try:
            yield
        finally:
            self.db.close()

    def insert(self, table, **fields):
        self.db.execute(f"INSERT INTO {table} ({','.join(fields)}) VALUES ({','.join('?' for _ in fields)})",
                        tuple(fields.values()))

    def turn(self, tid, complete=True):
        self.insert('turns', run_id='r', turn_id=tid, conversation_id='conversation',
                    started_at='now', ended_at='later' if complete else None,
                    conditions_json='{}', condition_key='baseline',
                    capture_state='complete' if complete else 'partial',
                    coverage_json=json.dumps({'model_complete': complete}))

    def artifact(self, aid, value=None):
        text = None if value is None else json.dumps(value, allow_nan=False)
        data = b'' if text is None else text.encode()
        self.insert('artifacts', artifact_id=aid, run_id='r', media_type='application/json',
                    representation='json', body_text=text, stored_bytes=len(data),
                    stored_sha256=None if text is None else hashlib.sha256(data).hexdigest(),
                    redacted=0, truncated=0, omitted_reason='payload_budget' if text is None else None,
                    policy_id='content')
        return aid

    def event(self, eid, kind='model.tool_call', boundary='model', **fields):
        self.sequence += 1
        defaults = dict(event_id=eid, run_id='r', turn_id='t', sequence=self.sequence,
                        kind=kind, boundary=boundary, provenance='observed', source_id=boundary,
                        observed_at='now', details_json='{}')
        defaults.update(fields)
        self.insert('events', **defaults)
        return eid

    def link(self, lid, source, target, relation, **fields):
        self.insert('links', link_id=lid, run_id='r', witness_event_id=target,
                    source_event_id=source, target_event_id=target, relation=relation, **fields)

    def operation(self, oid, *, decision=None, dispatch=None, status='ok', resolution='remote', client='c1'):
        start = self.event(oid+'-start', 'mcp.request', 'mcp', operation_id=oid,
                           client_id=client, payload_ref=self.artifact(oid+'-args', {'arguments': {'x': 2}}))
        end = None if status == 'running' else self.event(oid+'-end', 'mcp.response', 'mcp', operation_id=oid)
        self.insert('operations', run_id='r', operation_id=oid, turn_id='t', client_id=client,
                    method='tools/call', name='lookup', decision_id=decision, dispatch_event_id=dispatch,
                    link_basis='host_hook' if dispatch else 'propagated' if decision else 'none',
                    status=status, resolution=resolution, start_event_id=start, end_event_id=end,
                    started_at='now', ended_at=None if end is None else 'later', duration_ms=None if end is None else 1.5)

    def query(self, name, **parameters):
        return [dict(r) for r in self.db.execute((ROOT / 'sql' / (name+'.sql')).read_text(),
                                                {'run_id': 'r', **parameters})]

    def test_chain_preserves_explicit_no_hook_calls_and_repeated_answers(self):
        self.event('d', decision_id='local-d', name='lookup',
                   payload_ref=self.artifact('a', {'arguments': {'x': 1}}))
        self.operation('o', decision='local-d')
        self.event('next', 'model.request')
        self.link('answer-1', 'd', 'next', 'answers', target_slot='payload', target_pointer='/messages/1')
        self.turn('t2')
        self.event('later', 'model.request', turn_id='t2')
        self.link('answer-2', 'd', 'later', 'answers', target_slot='payload', target_pointer='/messages/2')
        rows = self.query('q2')
        assert len(rows) == 1, 'The propagated decision must produce exactly one association before inspecting its fields.'
        check.equal((rows[0]['operation_id'], rows[0]['link_basis']), ('o', 'propagated'), msg='Explicit propagation must retain its operation and association basis without a host dispatch.')
        check.equal(len(json.loads(rows[0]['answers_json'])), 2, msg='Repeated submitted answers must retain both observed request occurrences.')
        check.equal(json.loads(rows[0]['model_arguments']), {'x': 1}, msg='The chain must preserve the original selected arguments.')

    def test_many_decisions_share_one_dispatch_and_operation(self):
        for d in ('d1','d2'):
            self.event(d, decision_id=d)
        self.event('h', 'host.dispatch', 'host', state='execute')
        for d in ('d1','d2'):
            self.link('l-'+d, d, 'h', 'dispatch_source')
        self.operation('o', dispatch='h')
        check.equal(len(self.query('q2')), 2, msg='A shared dispatch must preserve both explicit source decisions.')
        check.equal(sum(r['operations'] for r in self.query('q3')), 1, msg='A shared decision association must not double-count the logical MCP operation.')

    def test_ambiguous_calls_stay_unlinked_and_omitted_decisions_survive(self):
        for d in ('d1','d2'):
            self.event(d, decision_id=d, name='lookup', payload_ref=self.artifact('a-'+d))
        self.operation('unknown', resolution='unknown')
        rows = self.query('q2')
        check.equal(len(rows), 2, msg='Both omitted decision observations must remain visible in Q2.')
        check.is_true(all(r['operation_id'] is None for r in rows), msg='Equal tool names cannot resolve ambiguous decisions to an operation.')
        check.is_true(all(r['model_omitted_reason']=='payload_budget' for r in rows), msg='Each omitted decision must retain its payload-budget limitation.')
        check.equal(self.query('q5')[0]['resolution'], 'unknown', msg='An unassigned operation retains unknown resolution in the operation aggregate.')
        check.is_true(any(e['operation_id']=='unknown' for e in self.query('q1', turn_id='t')), msg='Unassigned operations remain visible in the turn timeline.')

    def test_atomic_terminal_failure_and_incomplete_close(self):
        self.operation('o', status='running')
        self.db.commit()
        # An invalid terminal lifecycle update must fail its atomic transaction.
        with pytest.raises(sqlite3.IntegrityError), self.db:
            self.event('bad-end', 'mcp.response', 'mcp', operation_id='o')
            self.db.execute("UPDATE operations SET status='ok' WHERE operation_id='o'")
        check.is_none(self.db.execute("SELECT event_id FROM events WHERE event_id='bad-end'").fetchone(), msg='A failed terminal transition must roll back the terminal event too.')
        self.db.execute("UPDATE operations SET status='incomplete' WHERE status='running'")
        row = self.db.execute('SELECT * FROM operations').fetchone()
        check.equal(row['status'], 'incomplete', msg='Shutdown must classify a surviving running operation as incomplete.')
        check.is_none(row['end_event_id'], msg='Incomplete operations must not invent a terminal event.')
        check.is_none(row['duration_ms'], msg='Incomplete operations must not invent a measured duration.')

    def test_duplicate_operation_and_cross_run_witness_rejected(self):
        self.operation('o')
        # Duplicate operation identities must be rejected by the storage contract.
        with pytest.raises(sqlite3.IntegrityError):
            self.db.execute('INSERT INTO operations SELECT * FROM operations')
        self.insert('runs', run_id='other', started_at='now', environment_json='{}', policy_json='{}', coverage_json='{}')
        self.event('other-event', run_id='other', turn_id=None)
        # A link witness cannot refer to an event from another run.
        with pytest.raises(sqlite3.IntegrityError):
            self.insert('links', link_id='bad', run_id='r', witness_event_id='other-event',
                        source_event_id='unknown', target_event_id='other-event', relation='answers')
        # An operation terminal event cannot cross the recording boundary.
        with pytest.raises(sqlite3.IntegrityError):
            self.db.execute("UPDATE operations SET end_event_id='other-event' WHERE operation_id='o'")

    def test_closed_relations_and_authoritative_groups(self):
        self.event('d1', decision_id='d1', client_id='client-one')
        self.event('d2', decision_id='d2', client_id='client-two')
        self.event('unselected', decision_id='d3', source_id='second-producer')
        self.event('next', 'model.request')
        self.link('answer', 'd1', 'next', 'answers')
        # Only the four specified evidence relations may be persisted.
        with pytest.raises(sqlite3.IntegrityError):
            self.db.execute("UPDATE links SET relation='made_up'")
        check.equal(len(self.query('q2')), 2, msg='Only the run-selected producer group supplies authoritative decisions.')
        check.equal(sum(e['authoritative'] == 0 for e in self.query('q1', turn_id='t')), 1, msg='A nonselected producer remains visible but nonauthoritative in the timeline.')

    def test_zero_count_retains_coverage(self):
        self.turn('partial', complete=False)
        self.turn('nonzero')
        self.event('call', decision_id='call', turn_id='nonzero')
        rows = {r['turn_id']:r for r in self.query('q6')}
        check.is_not_in('nonzero', rows, msg='Q6 must omit turns that actually contain an observed decision.')
        check.equal((rows['t']['observed_tool_calls'], rows['t']['count_coverage']), (0,'complete'), msg='A complete declared model boundary supports a complete observed zero count.')
        check.equal((rows['partial']['observed_tool_calls'], rows['partial']['count_coverage']), (0,'partial_or_open'), msg='An incomplete turn cannot upgrade an observed zero count to complete coverage.')

    def test_chain_exposes_retention_and_routing_changes(self):
        self.event('d', decision_id='d', name='chosen',
                   payload_ref=self.artifact('model-args', {'arguments': {'x':1}}))
        self.event('h', 'host.dispatch', 'host', name='forwarded',
                   payload_ref=self.artifact('host-args', {'arguments': {'x':2}}))
        self.link('dispatch', 'd', 'h', 'dispatch_source')
        self.operation('o', dispatch='h')
        self.db.execute("UPDATE artifacts SET redacted=1 WHERE artifact_id='host-args'")
        self.db.execute("UPDATE artifacts SET truncated=1 WHERE artifact_id='o-args'")
        row = self.query('q2')[0]
        check.equal((row['name'], row['dispatched_tool'], row['mcp_tool']), ('chosen','forwarded','lookup'), msg='The query must preserve different selected, forwarded, and executed tool names.')
        check.equal((row['model_payload_ref'], row['host_payload_ref'], row['mcp_payload_ref']), ('model-args','host-args','o-args'), msg='Each chain boundary must expose its own retained payload reference.')
        check.equal((row['model_redacted'], row['host_redacted'], row['mcp_truncated']), (0,1,1), msg='Redaction and truncation flags must remain distinct across chain boundaries.')
        check.is_true(all(row[p+'_omitted_reason'] is None for p in ('model','host','mcp')), msg='Present payloads must not acquire omission reasons merely because other loss flags exist.')
