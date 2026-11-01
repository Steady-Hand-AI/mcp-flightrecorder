"""Independent SQL comparison and retained-loss facts from literal stored tokens."""
from contextlib import closing
import json
from pathlib import Path
import sqlite3

import pytest_check as check

BASE = Path(__file__).resolve().parents[1]


def test_metadata_recording_keeps_original_comparisons_and_loss_flags():
    # These literal stored-data fixtures exercise SQL, not a duplicate digest
    # implementation. Public Recorder cases establish token generation semantics.
    model_args = {"value": "1" * 64, "scope": "r", "projection": "args_json_python/1",
                  "basis": "observed_original"}
    forwarded_args = {**model_args, "value": "2" * 64}
    result_text = {"value": "3" * 64, "scope": "r", "projection": "text_blocks_json/1",
                   "basis": "observed_original"}
    submitted_text = {**result_text, "value": "4" * 64}
    with closing(sqlite3.connect(':memory:')) as db:
        db.row_factory = sqlite3.Row
        db.executescript((BASE / 'sql/schema.sql').read_text())
        db.execute('INSERT INTO runs(run_id,started_at,environment_json,policy_json,coverage_json) '
                   'VALUES(?,?,?,?,?)', ('r','now','{}','{"capture":"metadata"}',
                   json.dumps({'producers':{b:b for b in ('model','host','mcp')}})))
        for seq, (eid, kind, boundary, field, token) in enumerate([
            ('d','model.tool_call','model','args_digest',model_args),
            ('h','host.dispatch','host','args_digest',forwarded_args),
            ('result','mcp.response','mcp','result_text_digest',result_text),
            ('request','model.request','model',None,None),
        ]):
            db.execute('INSERT INTO artifacts(artifact_id,run_id,media_type,representation,stored_bytes,'
                       'redacted,truncated,omitted_reason,policy_id) VALUES(?,?,?,?,?,?,?,?,?)',
                       ('a-'+eid,'r','application/json','json',0, int(eid=='h'),
                        int(eid=='result'),'metadata','metadata'))
            db.execute('INSERT INTO events(event_id,run_id,sequence,kind,boundary,provenance,source_id,'
                       'observed_at,payload_ref,details_json) VALUES(?,?,?,?,?,?,?,?,?,?)',
                       (eid,'r',seq,kind,boundary,'observed',boundary,'now','a-'+eid,
                        json.dumps({field:token} if field else {})))
        db.execute('INSERT INTO links(link_id,run_id,witness_event_id,source_event_id,target_event_id,relation) '
                   'VALUES(?,?,?,?,?,?)',('dispatch','r','h','d','h','dispatch_source'))
        db.execute('INSERT INTO links(link_id,run_id,witness_event_id,source_event_id,target_event_id,'
                   'relation,details_json) VALUES(?,?,?,?,?,?,?)',
                   ('a','r','request','d','request','answers',
                    json.dumps({'result_event_id':'result','submitted_text_digest':submitted_text})))
        query = (BASE / 'sql/q2.sql').read_text()
        chain = dict(db.execute(query, {'run_id':'r'}).fetchone())
        check.equal(chain['forwarded_changed'], 1,
                    msg='Q2 must expose unequal complete model/host original tokens as a known rewrite despite body omission.')
        check.equal(json.loads(chain['answers_json'])[0]['text_equal'], 0,
                    msg='Q2 must expose unequal complete result/submitted text tokens despite body omission.')
        check.equal(json.loads(chain['model_args_digest_json'])['basis'], 'observed_original',
                    msg='Comparison authority must remain the original observation rather than retained bytes.')
        db.execute("UPDATE events SET details_json=? WHERE event_id='h'",
                   (json.dumps({'args_digest':model_args}),))
        check.equal(db.execute(query, {'run_id':'r'}).fetchone()['forwarded_changed'], 0,
                    msg='Equal complete original tokens are a known unchanged control, distinct from unavailable evidence.')
        db.execute("UPDATE events SET details_json=? WHERE event_id='h'",
                   (json.dumps({'args_digest':{**forwarded_args, 'value':None,
                                               'reason':'original_incomplete'}}),))
        check.is_none(db.execute(query, {'run_id':'r'}).fetchone()['forwarded_changed'],
                      msg='A partial original cannot establish whether forwarding changed it.')
        db.execute("UPDATE links SET details_json=? WHERE link_id='a'",
                   (json.dumps({'result_event_id':'result','submitted_text_digest':{
                       **submitted_text, 'value':None, 'reason':'original_incomplete'}}),))
        unknown_answers = db.execute(query, {'run_id':'r'}).fetchone()['answers_json']
        check.is_none(json.loads(unknown_answers)[0]['text_equal'],
                      msg='A partial submitted original cannot establish text equality.')
        check.equal(db.execute('SELECT count(*) FROM artifacts WHERE body_text IS NULL').fetchone()[0], 4,
                    msg='Metadata mode must omit every fixture payload body.')
        check.equal(db.execute("SELECT redacted FROM artifacts WHERE artifact_id='a-h'").fetchone()[0], 1,
                    msg='Redaction remains explicit independently of body omission.')
        check.equal(db.execute("SELECT truncated FROM artifacts WHERE artifact_id='a-result'").fetchone()[0], 1,
                    msg='Truncation remains explicit independently of body omission.')
