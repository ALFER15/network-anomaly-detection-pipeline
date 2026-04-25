from __future__ import annotations

"""Esquemas Pydantic para requests y responses de la API."""

from datetime import datetime
from pydantic import BaseModel, Field


class NetworkTrafficBase(BaseModel):
    """Representa los campos numéricos base de un flujo de red."""
    flow_duration: float
    total_fwd_packets: int
    total_backward_packets: int
    total_length_of_fwd_packets: float
    total_length_of_bwd_packets: float
    flow_bytes_per_s: float
    flow_packets_per_s: float
    fwd_packet_length_mean: float
    bwd_packet_length_mean: float
    syn_flag_count: int
    ack_flag_count: int
    psh_flag_count: int
    urg_flag_count: int
    label: int = Field(ge=0, le=1)


class NetworkTrafficRecord(NetworkTrafficBase):
    """Extiende el flujo base con metadata de persistencia."""
    id: int | None = None
    created_at: datetime | None = None


class LabelCount(BaseModel):
    """Conteo agregado por etiqueta de tráfico."""
    label: int
    records: int


class FlowBytesSummary(BaseModel):
    """Resumen estadístico sobre bytes por segundo."""
    avg: float
    p95: float
    p99: float
    max: float


class MetricsSummaryResponse(BaseModel):
    """Respuesta de `/metrics/summary`."""
    total_records: int
    label_distribution: list[LabelCount]
    flow_bytes_per_s: FlowBytesSummary


class AnomalyRecord(NetworkTrafficRecord):
    """Registro de anomalía estadística con su z-score calculado."""
    z_score: float


class AnomalyResponse(BaseModel):
    """Respuesta paginada para anomalías por z-score."""
    z_threshold: float
    count: int
    items: list[AnomalyRecord]


class TopTrafficResponse(BaseModel):
    """Respuesta para los flujos con mayor intensidad."""
    metric: str
    count: int
    items: list[NetworkTrafficRecord]


class MLFeatureInput(BaseModel):
    """Payload mínimo requerido para inferencia puntual."""
    flow_bytes_per_s: float
    flow_packets_per_s: float
    total_fwd_packets: int
    total_backward_packets: int
    fwd_packet_length_mean: float
    bwd_packet_length_mean: float
    syn_flag_count: int
    ack_flag_count: int
    psh_flag_count: int
    urg_flag_count: int


class PredictionResponse(BaseModel):
    """Respuesta compacta del endpoint `/predict`."""
    anomaly_score: float
    is_anomaly: int


class ScoredTrafficRecord(NetworkTrafficRecord):
    """Registro enriquecido con features derivadas y score del modelo."""
    bytes_per_packet: float | None = None
    fwd_bwd_ratio: float | None = None
    flag_ratio: float | None = None
    anomaly_score: float
    is_anomaly: int


class MLAnomaliesResponse(BaseModel):
    """Respuesta para anomalías detectadas por el modelo de ML."""
    count: int
    items: list[ScoredTrafficRecord]


class TopSuspiciousResponse(BaseModel):
    """Respuesta para los flujos con score más alto."""
    count: int
    items: list[ScoredTrafficRecord]


class AdvancedMetricsResponse(BaseModel):
    """Métricas agregadas sobre la tabla scored."""
    p95: float
    p99: float
    total_anomalies: int
    anomaly_ratio: float
