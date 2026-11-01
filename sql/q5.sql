-- Unknown is a reported group. Do not infer cache service from absent wire events.
SELECT method, resolution, COUNT(*) AS operations
FROM operations WHERE run_id=:run_id
GROUP BY method, resolution ORDER BY method, resolution;
