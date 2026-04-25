from __future__ import annotations

"""Utilidades compartidas para feature engineering, carga y predicción."""

from functools import lru_cache
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

FEATURE_COLUMNS = [
    "flow_bytes_per_s",
    "flow_packets_per_s",
    "total_fwd_packets",
    "total_backward_packets",
    "fwd_packet_length_mean",
    "bwd_packet_length_mean",
    "syn_flag_count",
    "ack_flag_count",
    "psh_flag_count",
    "urg_flag_count",
]

ENGINEERED_COLUMNS = ["bytes_per_packet", "fwd_bwd_ratio", "flag_ratio"]
MODEL_VERSION_PATTERN = "model_*.joblib"
SCALER_VERSION_PATTERN = "scaler_*.joblib"
LATEST_MODEL_PATH = Path("ml/model.joblib")
LATEST_SCALER_PATH = Path("ml/isolation_forest_scaler.joblib")


def add_feature_engineering(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Calcula variables derivadas necesarias para entrenamiento y scoring."""
    engineered = dataframe.copy()
    engineered["bytes_per_packet"] = engineered["flow_bytes_per_s"] / (engineered["flow_packets_per_s"] + 1e-6)
    engineered["fwd_bwd_ratio"] = engineered["total_fwd_packets"] / (engineered["total_backward_packets"] + 1)
    engineered["flag_ratio"] = (engineered["syn_flag_count"] + engineered["psh_flag_count"]) / (engineered["ack_flag_count"] + 1)
    return engineered


def build_feature_frame(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Selecciona y limpia las columnas que consume el modelo."""
    engineered = add_feature_engineering(dataframe)
    feature_frame = engineered[FEATURE_COLUMNS + ENGINEERED_COLUMNS].replace([np.inf, -np.inf], pd.NA)
    feature_frame = feature_frame.dropna()
    return feature_frame.astype(float)


def _resolve_latest_artifact(directory: Path, version_pattern: str, fallback_path: Path) -> Path:
    """Resuelve el artefacto más reciente, o cae al archivo estable por defecto."""
    candidates = sorted(directory.glob(version_pattern), key=lambda path: path.stat().st_mtime, reverse=True)
    if candidates:
        return candidates[0]
    return fallback_path


@lru_cache(maxsize=8)
def load_bundle(
    model_path: str | Path | None = None,
    scaler_path: str | Path | None = None,
) -> tuple[IsolationForest, StandardScaler]:
    """Carga el modelo y el scaler, priorizando los artefactos versionados."""
    project_root = Path(__file__).resolve().parents[1]
    model_target = Path(model_path) if model_path else _resolve_latest_artifact(project_root / "ml", MODEL_VERSION_PATTERN, project_root / LATEST_MODEL_PATH)
    scaler_target = Path(scaler_path) if scaler_path else _resolve_latest_artifact(project_root / "ml", SCALER_VERSION_PATTERN, project_root / LATEST_SCALER_PATH)
    model = joblib.load(model_target)
    scaler = joblib.load(scaler_target)
    return model, scaler


def predict_payload(
    payload: dict[str, Any],
    model: IsolationForest,
    scaler: StandardScaler,
) -> dict[str, Any]:
    """Aplica el modelo a un payload individual y devuelve score + flag."""
    dataframe = pd.DataFrame([payload])
    feature_frame = build_feature_frame(dataframe)
    if feature_frame.empty:
        raise ValueError("Payload does not contain valid numeric features.")

    transformed = scaler.transform(feature_frame)
    decision = float(model.decision_function(transformed)[0])
    prediction = int(model.predict(transformed)[0])
    anomaly_score = float(-decision)
    is_anomaly = 1 if prediction == -1 else 0

    return {
        "anomaly_score": anomaly_score,
        "is_anomaly": is_anomaly,
    }
