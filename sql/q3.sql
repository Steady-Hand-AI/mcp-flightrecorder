-- Operations are written only by capture; one row means one logical invocation.
SELECT server_key, name, status, resolution, COUNT(*) AS operations,
       COUNT(duration_ms) AS measured_durations, ROUND(AVG(duration_ms),1) AS mean_ms
FROM operations WHERE run_id=:run_id AND method='tools/call'
GROUP BY server_key, name, status, resolution ORDER BY server_key, name, status, resolution;
