CREATE DATABASE network_monitoring_db;

\c network_monitoring_db;

CREATE TABLE IF NOT EXISTS network_traffic (
    id BIGSERIAL PRIMARY KEY,
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
    label INTEGER NOT NULL CHECK (label IN (0, 1)),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_network_traffic_label ON network_traffic (label);
CREATE INDEX IF NOT EXISTS idx_network_traffic_flow_packets_per_s ON network_traffic (flow_packets_per_s);
