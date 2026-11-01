-- One decision/dispatch/operation association per row, NOT an execution count.
-- Repeated submitted answers are a nested array and cannot multiply the rows.
-- Captured tokens are validated by the writer; comparisons concern observed
-- original projections. SQL booleans are 1/0/NULL. Retention flags stay separate.
WITH paths AS (
  SELECT DISTINCT l.source_event_id AS decision_event, l.target_event_id AS dispatch,
                  o.operation_id
  FROM links l
  JOIN observed_events w ON w.run_id=l.run_id AND w.event_id=l.witness_event_id
  JOIN observed_events h ON h.run_id=l.run_id AND h.event_id=l.target_event_id
                         AND h.kind='host.dispatch'
  LEFT JOIN operations o ON o.run_id=l.run_id AND o.dispatch_event_id=h.event_id
  WHERE l.run_id=:run_id AND l.relation='dispatch_source'
  UNION ALL
  SELECT d.event_id, NULL, o.operation_id
  FROM observed_events d JOIN operations o
    ON o.run_id=d.run_id AND o.decision_id=d.decision_id AND o.link_basis='propagated'
  WHERE d.run_id=:run_id AND d.kind='model.tool_call'
)
SELECT d.event_id AS decision_event_id, d.decision_id, d.name,
       d.payload_ref AS model_payload_ref, da.omitted_reason AS model_omitted_reason,
       da.redacted AS model_redacted, da.truncated AS model_truncated,
       CASE WHEN json_valid(da.body_text) THEN json_extract(da.body_text,'$.arguments') END AS model_arguments,
       h.event_id AS dispatch_event_id, h.state AS host_action, h.name AS dispatched_tool,
       h.payload_ref AS host_payload_ref, ha.omitted_reason AS host_omitted_reason,
       ha.redacted AS host_redacted, ha.truncated AS host_truncated,
       CASE WHEN json_valid(ha.body_text) THEN json_extract(ha.body_text,'$.arguments') END AS forwarded_arguments,
       o.operation_id, o.name AS mcp_tool, o.status, o.resolution, o.link_basis,
       s.payload_ref AS mcp_payload_ref, ma.omitted_reason AS mcp_omitted_reason,
       ma.redacted AS mcp_redacted, ma.truncated AS mcp_truncated,
       CASE WHEN json_valid(ma.body_text) THEN json_extract(ma.body_text,'$.arguments') END AS mcp_arguments,
       json_extract(d.details_json,'$.args_digest') AS model_args_digest_json,
       json_extract(h.details_json,'$.args_digest') AS host_args_digest_json,
       json_extract(s.details_json,'$.args_digest') AS mcp_args_digest_json,
       CASE WHEN
         json_extract(d.details_json,'$.args_digest.basis')='observed_original'
         AND json_extract(h.details_json,'$.args_digest.basis')='observed_original'
         AND json_extract(d.details_json,'$.args_digest.projection')='args_json_python/1'
         AND json_extract(h.details_json,'$.args_digest.projection')='args_json_python/1'
         AND json_extract(d.details_json,'$.args_digest.scope')=d.run_id
         AND json_extract(h.details_json,'$.args_digest.scope')=d.run_id
         THEN json_extract(d.details_json,'$.args_digest.value')
           <> json_extract(h.details_json,'$.args_digest.value')
       END AS forwarded_changed,
       (SELECT json_group_array(json(item)) FROM (
          SELECT json_object('request_event_id', a.request_event_id,
            'pointer', a.target_pointer, 'message_artifact_id', a.message_artifact_id,
            'binding_link_id', a.link_id, 'binding_witness_event_id', a.binding_witness_event_id,
            'comparison_basis', 'observed_original',
            'text_equal', CASE WHEN
              json_extract(result.details_json,'$.result_text_digest.basis')='observed_original'
              AND json_extract(a.details_json,'$.submitted_text_digest.basis')='observed_original'
              AND json_extract(result.details_json,'$.result_text_digest.projection')='text_blocks_json/1'
              AND json_extract(a.details_json,'$.submitted_text_digest.projection')='text_blocks_json/1'
              AND json_extract(result.details_json,'$.result_text_digest.scope')=a.run_id
              AND json_extract(a.details_json,'$.submitted_text_digest.scope')=a.run_id
              THEN json_extract(result.details_json,'$.result_text_digest.value')
                = json_extract(a.details_json,'$.submitted_text_digest.value') END,
            'comparison', json(a.details_json)) AS item
          FROM answer_occurrences a
          LEFT JOIN observed_events result ON result.run_id=a.run_id
            AND result.event_id=json_extract(a.details_json,'$.result_event_id')
            AND result.kind='mcp.response'
          WHERE a.run_id=d.run_id AND a.source_event_id=d.event_id AND a.association_state='linked'
          ORDER BY a.request_sequence, a.message_index, a.target_pointer, a.link_id)) AS answers_json
FROM observed_events d
LEFT JOIN paths p ON p.decision_event=d.event_id
LEFT JOIN observed_events h ON h.run_id=d.run_id AND h.event_id=p.dispatch
LEFT JOIN operations o ON o.run_id=d.run_id AND o.operation_id=p.operation_id
LEFT JOIN events s ON s.run_id=o.run_id AND s.event_id=o.start_event_id
LEFT JOIN artifacts da ON da.run_id=d.run_id AND da.artifact_id=d.payload_ref
LEFT JOIN artifacts ha ON ha.run_id=h.run_id AND ha.artifact_id=h.payload_ref
LEFT JOIN artifacts ma ON ma.run_id=s.run_id AND ma.artifact_id=s.payload_ref
WHERE d.run_id=:run_id AND d.kind='model.tool_call'
ORDER BY d.sequence, h.sequence, o.started_at, o.operation_id;
