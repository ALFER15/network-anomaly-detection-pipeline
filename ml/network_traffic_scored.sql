CREATE TABLE IF NOT EXISTS network_traffic_scored (
    id BIGINT PRIMARY KEY,
    flow_duration DOUBLE PRECISION,
    total_fwd_packets INTEGER,
    total_backward_packets INTEGER,
    total_length_of_fwd_packets DOUBLE PRECISION,
    total_length_of_bwd_packets DOUBLE PRECISION,
    flow_bytes_per_s DOUBLE PRECISION,
    flow_packets_per_s DOUBLE PRECISION,
    fwd_packet_length_mean DOUBLE PRECISION,
    bwd_packet_length_mean DOUBLE PRECISION,
    syn_flag_count INTEGER,
    ack_flag_count INTEGER,
    psh_flag_count INTEGER,
    urg_flag_count INTEGER,
    label INTEGER,
    created_at TIMESTAMPTZ,
    bytes_per_packet DOUBLE PRECISION,
    fwd_bwd_ratio DOUBLE PRECISION,
    flag_ratio DOUBLE PRECISION,
    anomaly_score DOUBLE PRECISION,
    is_anomaly INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_anomaly_score
ON network_traffic_scored(anomaly_score DESC);

CREATE INDEX IF NOT EXISTS idx_is_anomaly
ON network_traffic_scored(is_anomaly);
