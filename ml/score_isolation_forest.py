from __future__ import annotations

"""Scoring batch sobre PostgreSQL para persistir anomalías detectadas."""

import argparse
import logging
import sys
from pathlib import Path
from typing import Iterator

import numpy as np
import pandas as pd
from psycopg2.extras import execute_values
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import settings
from db.connection import get_database_connection
from ml.runtime import add_feature_engineering, build_feature_frame, load_bundle

LOGGER = logging.getLogger(__name__)
TABLE_NAME = "network_traffic_scored"


def iter_source_chunks(chunk_size: int) -> Iterator[pd.DataFrame]:
    """Lee la tabla base por chunks para evitar consumir demasiada memoria."""
    query = """
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
        ORDER BY id
    """
    with get_database_connection() as connection:
        for chunk in pd.read_sql_query(query, connection, chunksize=chunk_size):
            yield chunk


def ensure_scored_table() -> None:
    """Crea la tabla destino que almacenará las predicciones del modelo."""
    with get_database_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
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
            )
            connection.commit()


def reset_scored_table() -> None:
    """Vacía la tabla scored cuando se requiere recalcular desde cero."""
    with get_database_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {TABLE_NAME}")
            connection.commit()


def parse_args() -> argparse.Namespace:
    """Define los argumentos para el proceso de scoring por lotes."""
    parser = argparse.ArgumentParser(description="Score network_traffic with Isolation Forest.")
    parser.add_argument("--chunk-size", type=int, default=settings.chunk_size)
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--model-path", default=None)
    parser.add_argument("--scaler-path", default=None)
    parser.add_argument("--replace-table", action="store_true")
    return parser.parse_args()


def score_chunk(dataframe: pd.DataFrame, model: IsolationForest, scaler: StandardScaler) -> pd.DataFrame:
    """Calcula features derivadas, score y bandera de anomalía por chunk."""
    engineered = add_feature_engineering(dataframe)
    feature_frame = build_feature_frame(dataframe)
    aligned = engineered.loc[feature_frame.index].copy()
    transformed = scaler.transform(feature_frame)
    predictions = model.predict(transformed)
    scores = -model.decision_function(transformed)

    aligned["bytes_per_packet"] = feature_frame["bytes_per_packet"]
    aligned["fwd_bwd_ratio"] = feature_frame["fwd_bwd_ratio"]
    aligned["flag_ratio"] = feature_frame["flag_ratio"]
    aligned["anomaly_score"] = scores
    aligned["is_anomaly"] = np.where(predictions == -1, 1, 0)
    return aligned


def write_scored_frame(dataframe: pd.DataFrame, batch_size: int) -> None:
    """Persiste el chunk scored en PostgreSQL usando insert/update idempotente."""
    if dataframe.empty:
        return

    rows = [tuple(row) for row in dataframe.itertuples(index=False, name=None)]
    insert_statement = f"""
        INSERT INTO {TABLE_NAME} (
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
        ) VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            flow_duration = EXCLUDED.flow_duration,
            total_fwd_packets = EXCLUDED.total_fwd_packets,
            total_backward_packets = EXCLUDED.total_backward_packets,
            total_length_of_fwd_packets = EXCLUDED.total_length_of_fwd_packets,
            total_length_of_bwd_packets = EXCLUDED.total_length_of_bwd_packets,
            flow_bytes_per_s = EXCLUDED.flow_bytes_per_s,
            flow_packets_per_s = EXCLUDED.flow_packets_per_s,
            fwd_packet_length_mean = EXCLUDED.fwd_packet_length_mean,
            bwd_packet_length_mean = EXCLUDED.bwd_packet_length_mean,
            syn_flag_count = EXCLUDED.syn_flag_count,
            ack_flag_count = EXCLUDED.ack_flag_count,
            psh_flag_count = EXCLUDED.psh_flag_count,
            urg_flag_count = EXCLUDED.urg_flag_count,
            label = EXCLUDED.label,
            created_at = EXCLUDED.created_at,
            bytes_per_packet = EXCLUDED.bytes_per_packet,
            fwd_bwd_ratio = EXCLUDED.fwd_bwd_ratio,
            flag_ratio = EXCLUDED.flag_ratio,
            anomaly_score = EXCLUDED.anomaly_score,
            is_anomaly = EXCLUDED.is_anomaly
    """
    values_template = "(" + ", ".join(["%s"] * len(dataframe.columns)) + ")"

    with get_database_connection() as connection:
        with connection.cursor() as cursor:
            execute_values(cursor, insert_statement, rows, template=values_template, page_size=batch_size)
        connection.commit()


def main() -> None:
    """Ejecuta el scoring completo y deja trazabilidad en logs."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = parse_args()
    model, scaler = load_bundle(args.model_path, args.scaler_path)

    ensure_scored_table()
    if args.replace_table:
        reset_scored_table()

    LOGGER.info("starting_scoring chunk_size=%s batch_size=%s replace_table=%s", args.chunk_size, args.batch_size, args.replace_table)

    total_rows = 0
    for chunk in iter_source_chunks(args.chunk_size):
        scored = score_chunk(chunk, model, scaler)
        output = scored[
            [
                "id",
                "flow_duration",
                "total_fwd_packets",
                "total_backward_packets",
                "total_length_of_fwd_packets",
                "total_length_of_bwd_packets",
                "flow_bytes_per_s",
                "flow_packets_per_s",
                "fwd_packet_length_mean",
                "bwd_packet_length_mean",
                "syn_flag_count",
                "ack_flag_count",
                "psh_flag_count",
                "urg_flag_count",
                "label",
                "created_at",
                "bytes_per_packet",
                "fwd_bwd_ratio",
                "flag_ratio",
                "anomaly_score",
                "is_anomaly",
            ]
        ].copy()
        write_scored_frame(output, args.batch_size)
        total_rows += len(output)

    LOGGER.info("scoring_finished rows=%s", total_rows)


if __name__ == "__main__":
    main()
