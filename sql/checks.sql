-- Validaciones rápidas sobre las tablas principales.
SELECT COUNT(*) AS total_rows
FROM network_traffic;

SELECT COUNT(*) AS total_scored_rows
FROM network_traffic_scored;

SELECT COUNT(*) AS total_anomalies
FROM network_traffic_scored
WHERE is_anomaly = 1;
