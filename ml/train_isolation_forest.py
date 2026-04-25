from __future__ import annotations

"""Entrenamiento local del modelo Isolation Forest con persistencia versionada."""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Iterator

import joblib
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import settings
from db.connection import ensure_database_and_table, get_database_connection, get_sqlalchemy_engine
from ml.runtime import build_feature_frame

ENGINE = get_sqlalchemy_engine()
LOGGER = logging.getLogger(__name__)


def iter_source_chunks(csv_path: str | None = None, chunk_size: int | None = None) -> Iterator[pd.DataFrame]:
    """Itera sobre el origen de datos, desde CSV o desde PostgreSQL."""
    if csv_path:
        yield from pd.read_csv(csv_path, chunksize=chunk_size or settings.chunk_size, low_memory=False)
        return

    query = """
        SELECT
            flow_bytes_per_s,
            flow_packets_per_s,
            total_fwd_packets,
            total_backward_packets,
            fwd_packet_length_mean,
            bwd_packet_length_mean,
            syn_flag_count,
            ack_flag_count,
            psh_flag_count,
            urg_flag_count
        FROM network_traffic
    """
    with get_database_connection() as connection:
        for chunk in pd.read_sql_query(query, connection, chunksize=chunk_size or settings.chunk_size):
            yield chunk


def load_training_data(csv_path: str | None = None, chunk_size: int | None = None) -> pd.DataFrame:
    """Concatena los chunks válidos y devuelve la matriz de entrenamiento."""
    frames: list[pd.DataFrame] = []
    for chunk in iter_source_chunks(csv_path=csv_path, chunk_size=chunk_size):
        frame = build_feature_frame(chunk)
        if not frame.empty:
            frames.append(frame)
    if not frames:
        raise RuntimeError("No training data available for Isolation Forest.")
    return pd.concat(frames, ignore_index=True)


def parse_args() -> argparse.Namespace:
    """Define parámetros para controlar la muestra, persistencia y mezcla."""
    parser = argparse.ArgumentParser(description="Train Isolation Forest for network anomalies.")
    parser.add_argument("--csv-path", default=None, help="Optional CSV source path.")
    parser.add_argument("--chunk-size", type=int, default=settings.chunk_size)
    parser.add_argument("--sample-size", type=int, default=200000)
    parser.add_argument("--contamination", type=float, default=0.02)
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--model-path", default=None)
    parser.add_argument("--scaler-path", default=None)
    return parser.parse_args()


def main() -> None:
    """Entrena el modelo, guarda artefactos y deja una copia estable."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = parse_args()
    ensure_database_and_table()

    LOGGER.info("starting_training")
    dataframe = load_training_data(csv_path=args.csv_path, chunk_size=args.chunk_size)
    if len(dataframe) > args.sample_size:
        dataframe = dataframe.sample(n=args.sample_size, random_state=args.random_state)

    scaler = StandardScaler()
    features = scaler.fit_transform(dataframe)

    model = IsolationForest(
        n_estimators=200,
        contamination=args.contamination,
        random_state=args.random_state,
        n_jobs=-1,
    )
    model.fit(features)

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    model_path = Path(args.model_path) if args.model_path else Path(f"ml/model_{timestamp}.joblib")
    scaler_path = Path(args.scaler_path) if args.scaler_path else Path(f"ml/scaler_{timestamp}.joblib")
    model_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, model_path)
    joblib.dump(scaler, scaler_path)
    joblib.dump(model, Path("ml/model.joblib"))
    joblib.dump(scaler, Path("ml/isolation_forest_scaler.joblib"))
    print(f"saved_model={model_path}")
    print(f"saved_scaler={scaler_path}")
    LOGGER.info("training_finished rows=%s model=%s scaler=%s", len(dataframe), model_path, scaler_path)


if __name__ == "__main__":
    main()
