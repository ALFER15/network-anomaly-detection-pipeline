-- Flujos con mayor score de anomalía.
SELECT *
FROM network_traffic_scored
WHERE is_anomaly = 1
ORDER BY anomaly_score DESC
LIMIT 10;

-- Top de flujos sospechosos aunque no estén marcados como anomalía binaria.
SELECT *
FROM network_traffic_scored
ORDER BY anomaly_score DESC
LIMIT 10;
