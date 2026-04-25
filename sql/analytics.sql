-- Consultas útiles para análisis exploratorio y reporting.
SELECT label, COUNT(*) AS records
FROM network_traffic
GROUP BY label
ORDER BY label;

SELECT
    AVG(flow_bytes_per_s) AS avg_flow_bytes_per_s,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY flow_bytes_per_s) AS p95_flow_bytes_per_s,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY flow_bytes_per_s) AS p99_flow_bytes_per_s
FROM network_traffic
WHERE flow_bytes_per_s IS NOT NULL;

SELECT
    AVG(anomaly_score) AS avg_anomaly_score,
    MAX(anomaly_score) AS max_anomaly_score,
    COUNT(*) FILTER (WHERE is_anomaly = 1) AS total_anomalies
FROM network_traffic_scored;
