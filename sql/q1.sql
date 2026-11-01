-- Bind run_id and turn_id. Raw events stay visible; the last column marks authority.
SELECT e.*, a.body_text AS payload, a.omitted_reason, a.redacted, a.truncated,
       a.representation AS payload_representation, c.body_text AS catalog,
       (SELECT json_group_array(json(item)) FROM (
          SELECT json_object('index', m.message_index, 'pointer', m.message_pointer,
            'artifact_id', m.message_artifact_id, 'artifact_missing', m.artifact_missing,
            'omitted_reason', m.omitted_reason, 'redacted', m.redacted,
            'truncated', m.truncated) AS item
          FROM request_members m
          WHERE m.run_id=e.run_id AND m.request_event_id=e.event_id
          ORDER BY m.message_index)) AS request_members_json,
       EXISTS(SELECT 1 FROM observed_events v WHERE v.event_id=e.event_id) AS authoritative
FROM events e
LEFT JOIN artifacts a ON a.run_id=e.run_id AND a.artifact_id=e.payload_ref
LEFT JOIN artifacts c ON c.run_id=e.run_id AND c.artifact_id=e.catalog_ref
WHERE e.run_id=:run_id AND e.turn_id=:turn_id ORDER BY e.sequence;
