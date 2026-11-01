-- Zero observed calls is a measured zero only with complete model capture for that turn.
SELECT t.turn_id, t.condition_key, t.capture_state, t.host_outcome,
       COUNT(e.event_id) AS observed_tool_calls,
       CASE WHEN t.capture_state='complete'
                  AND json_extract(t.coverage_json,'$.model_complete')=1
                  AND t.ended_at IS NOT NULL THEN 'complete' ELSE 'partial_or_open' END AS count_coverage
FROM turns t LEFT JOIN observed_events e
  ON e.run_id=t.run_id AND e.turn_id=t.turn_id AND e.kind='model.tool_call'
WHERE t.run_id=:run_id
GROUP BY t.run_id, t.turn_id HAVING COUNT(e.event_id)=0
ORDER BY t.started_at, t.turn_id;
