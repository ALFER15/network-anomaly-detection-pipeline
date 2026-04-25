from __future__ import annotations

"""Capa de consultas de solo lectura y predicción para la API."""

from typing import Any

from db.connection import get_database_connection
from ml.runtime import load_bundle, predict_payload

SCORED_TABLE = "network_traffic_scored"

SCORED_TABLE_DDL = f"""
CREATE TABLE IF NOT EXISTS {SCORED_TABLE} (
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
)
"""

SCORED_TABLE_INDEXES = [
    f"CREATE INDEX IF NOT EXISTS idx_anomaly_score ON {SCORED_TABLE}(anomaly_score DESC)",
    f"CREATE INDEX IF NOT EXISTS idx_is_anomaly ON {SCORED_TABLE}(is_anomaly)",
]


def ensure_scored_table_exists() -> None:
    """Crea la tabla scored y sus índices de soporte."""
    with get_database_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(SCORED_TABLE_DDL)
            for statement in SCORED_TABLE_INDEXES:
                cursor.execute(statement)
        connection.commit()


def _rows_to_dicts(cursor) -> list[dict[str, Any]]:
    """Convierte una consulta SQL en una lista de diccionarios serializables."""
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def fetch_metrics_summary() -> dict[str, Any]:
    """Resume volumen, distribución de etiquetas y estadísticas de bytes/s."""
    with get_database_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*) AS total_records
                FROM network_traffic
                """
            )
            total_records = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT label, COUNT(*) AS records
                FROM network_traffic
                GROUP BY label
                ORDER BY label
                """
            )
            label_distribution = _rows_to_dicts(cursor)

            cursor.execute(
                """
                SELECT
                  AVG(flow_bytes_per_s) AS avg_flow_bytes_per_s,
                  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY flow_bytes_per_s) AS p95_flow_bytes_per_s,
                  PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY flow_bytes_per_s) AS p99_flow_bytes_per_s,
                  MAX(flow_bytes_per_s) AS max_flow_bytes_per_s
                FROM network_traffic
                WHERE flow_bytes_per_s IS NOT NULL
                """
            )
            bytes_stats = cursor.fetchone()

    return {
        "total_records": int(total_records),
        "label_distribution": label_distribution,
        "flow_bytes_per_s": {
            "avg": float(bytes_stats[0]) if bytes_stats[0] is not None else 0.0,
            "p95": float(bytes_stats[1]) if bytes_stats[1] is not None else 0.0,
            "p99": float(bytes_stats[2]) if bytes_stats[2] is not None else 0.0,
            "max": float(bytes_stats[3]) if bytes_stats[3] is not None else 0.0,
        },
    }


def fetch_anomalies(z_threshold: float = 3.0, limit: int = 100) -> list[dict[str, Any]]:
    """Detecta anomalías estadísticas sobre la tabla bruta por z-score."""
    with get_database_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                WITH stats AS (
                  SELECT
                    AVG(flow_bytes_per_s) AS mu,
                    STDDEV_POP(flow_bytes_per_s) AS sigma
                  FROM network_traffic
                  WHERE flow_bytes_per_s IS NOT NULL
                )
                SELECT
                  t.id,
                  t.flow_duration,
                  t.total_fwd_packets,
                  t.total_backward_packets,
                  t.total_length_of_fwd_packets,
                  t.total_length_of_bwd_packets,
                  t.flow_bytes_per_s,
                  t.flow_packets_per_s,
                  t.fwd_packet_length_mean,
                  t.bwd_packet_length_mean,
                  t.syn_flag_count,
                  t.ack_flag_count,
                  t.psh_flag_count,
                  t.urg_flag_count,
                  t.label,
                  t.created_at,
                  ((t.flow_bytes_per_s - s.mu) / NULLIF(s.sigma, 0)) AS z_score
                FROM network_traffic t
                CROSS JOIN stats s
                WHERE ABS((t.flow_bytes_per_s - s.mu) / NULLIF(s.sigma, 0)) > %s
                ORDER BY ABS((t.flow_bytes_per_s - s.mu) / NULLIF(s.sigma, 0)) DESC
                LIMIT %s
                """,
                (z_threshold, limit),
            )
            return _rows_to_dicts(cursor)


def fetch_top_traffic(metric: str = "flow_bytes_per_s", limit: int = 20) -> list[dict[str, Any]]:
    """Devuelve los registros con mayor intensidad para una métrica dada."""
    allowed_metrics = {"flow_bytes_per_s", "flow_packets_per_s"}
    order_column = metric if metric in allowed_metrics else "flow_bytes_per_s"

    with get_database_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                  id,
                  flow_duration,
                  total_fwd_packets,
                  total_backward_packets,
                  total_length_of_fwd_packets,
                  total_length_of_bwd_packets,
                  flow_bytes_per_s,
                  flow_packets_per_s,
                  fwd_packet_length_mean,
                  bwd_packet_length_mean,
                  syn_flag_count,
                  ack_flag_count,
                  psh_flag_count,
                  urg_flag_count,
                  label,
                  created_at
                FROM network_traffic
                ORDER BY {order_column} DESC
                LIMIT %s
                """,
                (limit,),
            )
            return _rows_to_dicts(cursor)


def fetch_ml_anomalies(limit: int = 100) -> list[dict[str, Any]]:
    """Recupera los registros marcados como anómalos por el modelo."""
    with get_database_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                  id,
                  flow_duration,
                  total_fwd_packets,
                  total_backward_packets,
                  total_length_of_fwd_packets,
                  total_length_of_bwd_packets,
                  flow_bytes_per_s,
                  flow_packets_per_s,
                  fwd_packet_length_mean,
                  bwd_packet_length_mean,
                  syn_flag_count,
                  ack_flag_count,
                  psh_flag_count,
                  urg_flag_count,
                  label,
                  created_at,
                  bytes_per_packet,
                  fwd_bwd_ratio,
                  flag_ratio,
                  anomaly_score,
                  is_anomaly
                FROM {SCORED_TABLE}
                WHERE is_anomaly = 1
                ORDER BY anomaly_score DESC
                LIMIT %s
                """,
                (limit,),
            )
            return _rows_to_dicts(cursor)


def fetch_top_suspicious(limit: int = 50) -> list[dict[str, Any]]:
    """Ordena los flujos scored por mayor score de anomalía."""
    with get_database_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                  id,
                  flow_duration,
                  total_fwd_packets,
                  total_backward_packets,
                  total_length_of_fwd_packets,
                  total_length_of_bwd_packets,
                  flow_bytes_per_s,
                  flow_packets_per_s,
                  fwd_packet_length_mean,
                  bwd_packet_length_mean,
                  syn_flag_count,
                  ack_flag_count,
                  psh_flag_count,
                  urg_flag_count,
                  label,
                  created_at,
                  bytes_per_packet,
                  fwd_bwd_ratio,
                  flag_ratio,
                  anomaly_score,
                  is_anomaly
                FROM {SCORED_TABLE}
                ORDER BY anomaly_score DESC
                LIMIT %s
                """,
                (limit,),
            )
            return _rows_to_dicts(cursor)


def fetch_advanced_metrics() -> dict[str, Any]:
    """Calcula percentiles y proporción de anomalías en la tabla scored."""
    with get_database_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY anomaly_score) AS p95,
                  PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY anomaly_score) AS p99,
                  COUNT(*) FILTER (WHERE is_anomaly = 1) AS total_anomalies,
                  COUNT(*)::float / NULLIF(COUNT(*), 0) AS anomaly_ratio
                FROM {SCORED_TABLE}
                """
            )
            row = cursor.fetchone()

    return {
        "p95": float(row[0]) if row[0] is not None else 0.0,
        "p99": float(row[1]) if row[1] is not None else 0.0,
        "total_anomalies": int(row[2]) if row[2] is not None else 0,
        "anomaly_ratio": float(row[3]) if row[3] is not None else 0.0,
    }


def predict_single(payload: dict[str, Any]) -> dict[str, Any]:
    """Ejecuta inferencia puntual usando el bundle más reciente cargado."""
    model, scaler = load_bundle()
    return predict_payload(payload, model=model, scaler=scaler)
