-- Lean reference schema, version 6. Seven tables; no running recorder is included.
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;
PRAGMA busy_timeout = 1000;


CREATE TABLE runs (
  run_id TEXT PRIMARY KEY NOT NULL,
  started_at TEXT NOT NULL,
  ended_at TEXT,
  environment_json TEXT NOT NULL CHECK (json_valid(environment_json)),
  policy_json TEXT NOT NULL CHECK (json_valid(policy_json)),
  coverage_json TEXT NOT NULL CHECK (json_valid(coverage_json)),
  dropped_events INTEGER NOT NULL DEFAULT 0 CHECK (dropped_events >= 0),
  writer_errors INTEGER NOT NULL DEFAULT 0 CHECK (writer_errors >= 0),
  close_state TEXT NOT NULL DEFAULT 'open'
    CHECK (close_state IN ('open','clean','incomplete'))
);

CREATE TABLE artifacts (
  artifact_id TEXT PRIMARY KEY NOT NULL,
  run_id TEXT NOT NULL REFERENCES runs(run_id),
  media_type TEXT NOT NULL,
  representation TEXT NOT NULL,
  body_text TEXT,
  stored_sha256 TEXT,
  stored_bytes INTEGER NOT NULL CHECK (stored_bytes >= 0),
  observed_bytes INTEGER CHECK (observed_bytes IS NULL OR observed_bytes >= 0),
  redacted INTEGER NOT NULL CHECK (redacted IN (0,1)),
  truncated INTEGER NOT NULL CHECK (truncated IN (0,1)),
  omitted_reason TEXT,
  policy_id TEXT NOT NULL,
  UNIQUE (run_id, artifact_id),
  CHECK ((body_text IS NULL AND stored_sha256 IS NULL AND stored_bytes = 0
          AND omitted_reason IS NOT NULL)
      OR (body_text IS NOT NULL AND stored_sha256 IS NOT NULL
          AND omitted_reason IS NULL))
);

CREATE TABLE turns (
  run_id TEXT NOT NULL REFERENCES runs(run_id),
  turn_id TEXT NOT NULL,
  conversation_id TEXT,
  started_at TEXT NOT NULL,
  ended_at TEXT,
  prompt_ref TEXT,
  dataset_item_id TEXT,
  repeat_index INTEGER,
  conditions_json TEXT NOT NULL CHECK (json_valid(conditions_json)),
  condition_key TEXT NOT NULL,
  host_outcome TEXT NOT NULL DEFAULT 'open',
  capture_state TEXT NOT NULL DEFAULT 'unknown'
    CHECK (capture_state IN ('unknown','complete','partial')),
  coverage_json TEXT NOT NULL CHECK (json_valid(coverage_json)),
  PRIMARY KEY (run_id, turn_id),
  FOREIGN KEY (run_id, prompt_ref) REFERENCES artifacts(run_id, artifact_id)
);

CREATE TABLE events (
  event_id TEXT PRIMARY KEY NOT NULL,
  run_id TEXT NOT NULL REFERENCES runs(run_id),
  turn_id TEXT,
  sequence INTEGER NOT NULL,
  event_version INTEGER NOT NULL DEFAULT 1,
  kind TEXT NOT NULL,
  boundary TEXT NOT NULL,
  provenance TEXT NOT NULL CHECK (provenance IN ('observed','declared','imported')),
  source_id TEXT NOT NULL,
  observed_at TEXT NOT NULL,
  observed_mono_ms REAL,
  model_call_id TEXT,
  decision_id TEXT,
  operation_id TEXT,
  parent_operation_id TEXT,
  client_id TEXT,
  server_key TEXT,
  method TEXT,
  name TEXT,
  state TEXT,
  payload_ref TEXT,
  catalog_ref TEXT,
  details_json TEXT NOT NULL CHECK (json_valid(details_json)),
  UNIQUE (run_id, sequence),
  FOREIGN KEY (run_id, turn_id) REFERENCES turns(run_id, turn_id),
  FOREIGN KEY (run_id, payload_ref) REFERENCES artifacts(run_id, artifact_id),
  FOREIGN KEY (run_id, catalog_ref) REFERENCES artifacts(run_id, artifact_id)
);

CREATE TABLE findings (
  finding_id TEXT PRIMARY KEY NOT NULL,
  run_id TEXT NOT NULL REFERENCES runs(run_id),
  turn_id TEXT,
  target_event_id TEXT,
  analysis_id TEXT NOT NULL,
  checker TEXT NOT NULL,
  checker_version TEXT NOT NULL,
  config_json TEXT NOT NULL CHECK (json_valid(config_json)),
  config_hash TEXT NOT NULL,
  metric TEXT NOT NULL,
  result TEXT NOT NULL CHECK (result IN ('pass','fail','unknown','not_applicable')),
  score REAL,
  reason TEXT,
  evidence_ids_json TEXT NOT NULL CHECK (json_valid(evidence_ids_json)),
  created_at TEXT NOT NULL,
  UNIQUE (analysis_id, target_event_id, checker, checker_version, config_hash, metric),
  CHECK (target_event_id IS NOT NULL OR turn_id IS NOT NULL),
  FOREIGN KEY (run_id, turn_id) REFERENCES turns(run_id, turn_id)
);

CREATE INDEX events_turn ON events(run_id, turn_id, sequence);
CREATE INDEX events_operation ON events(run_id, operation_id, kind);
CREATE INDEX events_decision ON events(run_id, decision_id, kind);
CREATE INDEX events_kind ON events(kind, run_id);
CREATE INDEX turns_condition ON turns(condition_key, dataset_item_id);
CREATE INDEX findings_analysis ON findings(analysis_id, metric, result);
CREATE UNIQUE INDEX findings_turn_unique ON findings(
  run_id, turn_id, analysis_id, checker, checker_version, config_hash, metric
) WHERE target_event_id IS NULL;

-- Commit links with their witness event. Source/target IDs may be symbolic.
-- Adapters validate known event shapes once when capturing them.
CREATE UNIQUE INDEX events_run_event ON events(run_id, event_id);

CREATE TABLE links (
    link_id TEXT PRIMARY KEY NOT NULL,
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    witness_event_id TEXT NOT NULL,
    source_event_id TEXT NOT NULL,
    target_event_id TEXT NOT NULL,
    relation TEXT NOT NULL CHECK (relation IN
      ('dispatch_source','answers','transformed_from','inserted_into')),
    source_slot TEXT CHECK (source_slot IN ('payload', 'catalog')),
    source_pointer TEXT,
    target_slot TEXT CHECK (target_slot IN ('payload', 'catalog')),
    target_pointer TEXT,
    details_json TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(details_json)),
    CHECK (source_pointer IS NULL OR source_slot IS NOT NULL),
    CHECK (target_pointer IS NULL OR target_slot IS NOT NULL),
    CHECK (source_pointer IS NULL OR source_pointer='' OR substr(source_pointer,1,1)='/'),
    CHECK (target_pointer IS NULL OR target_pointer='' OR substr(target_pointer,1,1)='/'),
    FOREIGN KEY (run_id, witness_event_id) REFERENCES events(run_id, event_id)
);

CREATE INDEX links_by_source ON links(run_id, source_event_id);
CREATE INDEX links_by_target ON links(run_id, target_event_id);
CREATE INDEX links_by_witness ON links(run_id, witness_event_id);
-- Exact message-handle lookup for repeated history; avoid a run-wide link scan
-- for every member in the request array.
CREATE INDEX links_by_message ON links(run_id, json_extract(details_json, '$.message_artifact_id'))
WHERE relation='answers' AND json_extract(details_json, '$.binding_format')='message_binding/2';


-- A row is one logical invocation, including cache service and visible continuations.
-- Host retries are separate operations. No name/argument matching assigns identity.
-- Start event + running row commit together. Completion event + UPDATE commit together.
-- The writer uses UPDATE ... WHERE status='running', requires rowcount=1, and
-- rolls back the whole completion bundle otherwise; readers do not repair lifecycles.
CREATE TABLE operations (
  run_id TEXT NOT NULL REFERENCES runs(run_id),
  operation_id TEXT NOT NULL,
  turn_id TEXT,
  model_call_id TEXT,
  decision_id TEXT,
  dispatch_event_id TEXT,
  link_basis TEXT NOT NULL DEFAULT 'none'
    CHECK (link_basis IN ('host_hook','propagated','none')),
  client_id TEXT NOT NULL,
  server_key TEXT,
  method TEXT NOT NULL,
  name TEXT,
  status TEXT NOT NULL DEFAULT 'running' CHECK (status IN
    ('running','ok','tool_error','protocol_error','transport_error','timeout',
     'cancelled','local_error','incomplete')),
  resolution TEXT NOT NULL DEFAULT 'unknown' CHECK (resolution IN
    ('remote','client_cache','host_cache','local','mixed','unknown')),
  input_rounds INTEGER CHECK (input_rounds IS NULL OR input_rounds >= 0),
  start_event_id TEXT NOT NULL,
  end_event_id TEXT,
  started_at TEXT NOT NULL,
  ended_at TEXT,
  duration_ms REAL CHECK (duration_ms IS NULL OR duration_ms >= 0),
  error_code INTEGER,
  PRIMARY KEY (run_id, operation_id),
  FOREIGN KEY (run_id, turn_id) REFERENCES turns(run_id, turn_id),
  FOREIGN KEY (run_id, start_event_id) REFERENCES events(run_id, event_id),
  FOREIGN KEY (run_id, end_event_id) REFERENCES events(run_id, event_id),
  CHECK ((status IN ('running','incomplete') AND end_event_id IS NULL
          AND ended_at IS NULL AND duration_ms IS NULL)
      OR (status NOT IN ('running','incomplete') AND end_event_id IS NOT NULL
          AND ended_at IS NOT NULL)),
  CHECK ((link_basis='none' AND decision_id IS NULL AND dispatch_event_id IS NULL)
      OR (link_basis='host_hook' AND dispatch_event_id IS NOT NULL)
      OR (link_basis='propagated' AND decision_id IS NOT NULL
          AND dispatch_event_id IS NULL))
);
CREATE INDEX operations_turn ON operations(run_id, turn_id);
CREATE INDEX operations_decision ON operations(run_id, decision_id);
CREATE INDEX operations_dispatch ON operations(run_id, dispatch_event_id);

-- Producer IDs identify the recorder's emitting group, not one client instance.
CREATE VIEW observed_events AS
SELECT e.* FROM events e JOIN runs r ON r.run_id=e.run_id
WHERE e.provenance='observed' AND e.event_version=1
  AND e.source_id=json_extract(r.coverage_json, '$.producers.' || e.boundary);

-- Ordered membership belongs to each actual request, even when retained message
-- bytes and answer bindings are shared. LEFT JOIN keeps missing parts visible.
CREATE VIEW request_members AS
SELECT e.run_id, e.event_id AS request_event_id, e.turn_id, e.model_call_id,
       e.sequence AS request_sequence, e.payload_ref AS manifest_ref,
       CAST(m.key AS INTEGER) AS message_index,
       json_extract(a.body_text, '$.message_path') AS message_path,
       json_extract(a.body_text, '$.message_path') || '/' || m.key AS message_pointer,
       m.value AS message_artifact_id,
       p.artifact_id IS NULL AS artifact_missing,
       p.omitted_reason, p.redacted, p.truncated
FROM observed_events e
JOIN artifacts a ON a.run_id=e.run_id AND a.artifact_id=e.payload_ref
JOIN json_each(CASE WHEN json_valid(a.body_text) THEN a.body_text ELSE '{}' END,
               '$.message_refs') m
LEFT JOIN artifacts p ON p.run_id=e.run_id
                     AND p.artifact_id=m.value
WHERE e.kind='model.request' AND a.representation='request_refs/2'
  AND json_extract(a.body_text, '$.format')='request_refs/2' AND m.type='text';

-- Message handles include explicit binding facts and retention identity. This
-- is reuse of those facts, not provenance inferred from equal message content.
-- Later bindings cannot retroactively attribute earlier request occurrences.
CREATE VIEW answer_occurrences AS
SELECT m.run_id, m.request_event_id, m.turn_id, m.model_call_id, m.request_sequence,
       m.message_index, m.message_artifact_id,
       l.link_id,
       l.source_event_id, l.witness_event_id AS binding_witness_event_id,
       l.target_event_id AS binding_request_event_id,
       m.message_pointer || json_extract(l.details_json, '$.relative_pointer') AS target_pointer,
       l.details_json,
       CASE
         WHEN w.event_id IS NULL OR w.kind<>'model.request'
           OR w.event_id<>l.target_event_id OR w.sequence>m.request_sequence
           THEN 'missing_or_unselected_witness'
         WHEN d.event_id IS NULL THEN 'missing_decision'
         ELSE 'linked'
       END AS association_state
FROM request_members m
JOIN links l ON l.run_id=m.run_id AND l.relation='answers'
  AND json_extract(l.details_json, '$.binding_format')='message_binding/2'
  AND json_extract(l.details_json, '$.message_artifact_id')=m.message_artifact_id
LEFT JOIN observed_events w ON w.run_id=l.run_id AND w.event_id=l.witness_event_id
LEFT JOIN observed_events d ON d.run_id=l.run_id AND d.event_id=l.source_event_id
                          AND d.kind='model.tool_call'
UNION ALL
-- Direct occurrence links remain readable; they are not inferred from content.
SELECT r.run_id, r.event_id, r.turn_id, r.model_call_id, r.sequence,
       NULL, NULL, l.link_id, l.source_event_id, l.witness_event_id,
       l.target_event_id, l.target_pointer, l.details_json,
       CASE WHEN d.event_id IS NULL THEN 'missing_decision' ELSE 'linked' END
FROM links l
JOIN observed_events r ON r.run_id=l.run_id AND r.event_id=l.target_event_id
                       AND r.kind='model.request'
JOIN observed_events w ON w.run_id=l.run_id AND w.event_id=l.witness_event_id
LEFT JOIN observed_events d ON d.run_id=l.run_id AND d.event_id=l.source_event_id
                          AND d.kind='model.tool_call'
WHERE l.relation='answers'
  AND coalesce(json_extract(l.details_json, '$.binding_format'), '')='';

-- Writers validate operation shape/source and manifest/binding shapes at capture.
-- Imports do not write operations. This fresh schema has no migration runner.
PRAGMA user_version = 6;
