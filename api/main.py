from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, Request

from api.crud import (
    fetch_advanced_metrics,
    fetch_anomalies,
    fetch_metrics_summary,
    fetch_ml_anomalies,
    ensure_scored_table_exists,
    fetch_top_suspicious,
    fetch_top_traffic,
    predict_single,
)
from db.connection import ensure_database_and_table
from models.schemas import (
    AdvancedMetricsResponse,
    AnomalyResponse,
    MLAnomaliesResponse,
    MetricsSummaryResponse,
    PredictionResponse,
    MLFeatureInput,
    TopSuspiciousResponse,
    TopTrafficResponse,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Inicializa la base de datos y la tabla de scoring antes de servir peticiones."""
    ensure_database_and_table()
    ensure_scored_table_exists()
    yield


app = FastAPI(
    title="Network Monitoring API",
    version="1.0.0",
    description="FastAPI service for CICIDS2017 network traffic monitoring.",
    lifespan=lifespan,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
LOGGER = logging.getLogger(__name__)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Registra cada request con tiempo total de ejecución."""
    start_time = time.perf_counter()
    response = await call_next(request)
    elapsed_ms = (time.perf_counter() - start_time) * 1000
    LOGGER.info("request method=%s path=%s status=%s elapsed_ms=%.2f", request.method, request.url.path, response.status_code, elapsed_ms)
    return response


@app.get("/health")
def health() -> dict[str, str]:
    """Expone un health check simple para monitoreo local."""
    return {"status": "ok"}


@app.get("/metrics/summary", response_model=MetricsSummaryResponse)
def get_metrics_summary() -> MetricsSummaryResponse:
    """Devuelve el resumen global de volumen y distribución de etiquetas."""
    return MetricsSummaryResponse.model_validate(fetch_metrics_summary())


@app.get("/anomalies", response_model=AnomalyResponse)
def get_anomalies(
    limit: int = Query(default=100, ge=1, le=5000),
    z_threshold: float = Query(default=3.0, gt=0),
) -> AnomalyResponse:
    """Devuelve anomalías estadísticas basadas en z-score sobre el tráfico bruto."""
    rows = fetch_anomalies(limit=limit, z_threshold=z_threshold)
    return AnomalyResponse(z_threshold=z_threshold, count=len(rows), items=rows)


@app.get("/top_traffic", response_model=TopTrafficResponse)
def get_top_traffic(
    metric: str = Query(default="flow_bytes_per_s", pattern="^(flow_bytes_per_s|flow_packets_per_s)$"),
    limit: int = Query(default=20, ge=1, le=1000),
) -> TopTrafficResponse:
    """Entrega los flujos con mayor intensidad para la métrica solicitada."""
    rows = fetch_top_traffic(metric=metric, limit=limit)
    return TopTrafficResponse(metric=metric, count=len(rows), items=rows)


@app.get("/anomalies/ml", response_model=MLAnomaliesResponse)
def get_ml_anomalies(limit: int = Query(default=100, ge=1, le=5000)) -> MLAnomaliesResponse:
    """Recupera las filas marcadas como anómalas por el modelo de ML."""
    rows = fetch_ml_anomalies(limit=limit)
    return MLAnomaliesResponse(count=len(rows), items=rows)


@app.get("/top_suspicious", response_model=TopSuspiciousResponse)
def get_top_suspicious(limit: int = Query(default=50, ge=1, le=1000)) -> TopSuspiciousResponse:
    """Ordena el scoring persistido por mayor score de anomalía."""
    rows = fetch_top_suspicious(limit=limit)
    return TopSuspiciousResponse(count=len(rows), items=rows)


@app.post("/predict", response_model=PredictionResponse)
def predict(payload: MLFeatureInput) -> PredictionResponse:
    """Ejecuta inferencia puntual con el payload numérico recibido por API."""
    result = predict_single(payload.model_dump())
    return PredictionResponse(**result)


@app.get("/metrics/advanced", response_model=AdvancedMetricsResponse)
def get_advanced_metrics() -> AdvancedMetricsResponse:
    """Devuelve percentiles y ratio global sobre la tabla scored."""
    return AdvancedMetricsResponse.model_validate(fetch_advanced_metrics())
