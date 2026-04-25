from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from ml.runtime import build_feature_frame, predict_payload


def test_ml_prediction() -> None:
    training = pd.DataFrame(
        [
            {
                "flow_bytes_per_s": 10.0,
                "flow_packets_per_s": 2.0,
                "total_fwd_packets": 1,
                "total_backward_packets": 1,
                "fwd_packet_length_mean": 1.0,
                "bwd_packet_length_mean": 1.0,
                "syn_flag_count": 0,
                "ack_flag_count": 1,
                "psh_flag_count": 0,
                "urg_flag_count": 0,
            },
            {
                "flow_bytes_per_s": 12.0,
                "flow_packets_per_s": 3.0,
                "total_fwd_packets": 2,
                "total_backward_packets": 1,
                "fwd_packet_length_mean": 2.0,
                "bwd_packet_length_mean": 1.5,
                "syn_flag_count": 1,
                "ack_flag_count": 1,
                "psh_flag_count": 1,
                "urg_flag_count": 0,
            },
        ]
    )
    feature_frame = build_feature_frame(training)
    scaler = StandardScaler().fit(feature_frame)
    model = IsolationForest(random_state=42, contamination=0.5).fit(scaler.transform(feature_frame))

    payload = {
        "flow_bytes_per_s": 11.0,
        "flow_packets_per_s": 2.5,
        "total_fwd_packets": 2,
        "total_backward_packets": 1,
        "fwd_packet_length_mean": 1.5,
        "bwd_packet_length_mean": 1.0,
        "syn_flag_count": 1,
        "ack_flag_count": 1,
        "psh_flag_count": 0,
        "urg_flag_count": 0,
    }

    result = predict_payload(payload, model=model, scaler=scaler)

    assert "anomaly_score" in result
    assert result["is_anomaly"] in {0, 1}