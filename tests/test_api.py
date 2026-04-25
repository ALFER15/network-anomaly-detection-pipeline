from __future__ import annotations

from fastapi.testclient import TestClient

import api.main as api_main


def make_client(monkeypatch) -> TestClient:
    monkeypatch.setattr(api_main, "ensure_database_and_table", lambda: None)
    monkeypatch.setattr(api_main, "ensure_scored_table_exists", lambda: None)
    return TestClient(api_main.app)


def test_health_endpoint(monkeypatch) -> None:
    client = make_client(monkeypatch)

    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_predict_endpoint(monkeypatch) -> None:
    monkeypatch.setattr(api_main, "predict_single", lambda payload: {"anomaly_score": 0.25, "is_anomaly": 1})
    client = make_client(monkeypatch)

    response = client.post(
        "/predict",
        json={
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
        },
    )

    assert response.status_code == 200
    assert response.json() == {"anomaly_score": 0.25, "is_anomaly": 1}


def test_metrics_summary_endpoint(monkeypatch) -> None:
    monkeypatch.setattr(
        api_main,
        "fetch_metrics_summary",
        lambda: {
            "total_records": 1,
            "label_distribution": [{"label": 0, "records": 1}],
            "flow_bytes_per_s": {"avg": 1.0, "p95": 1.0, "p99": 1.0, "max": 1.0},
        },
    )
    client = make_client(monkeypatch)

    response = client.get("/metrics/summary")

    assert response.status_code == 200
    assert response.json()["total_records"] == 1