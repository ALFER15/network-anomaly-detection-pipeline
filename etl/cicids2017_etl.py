from __future__ import annotations

"""ETL para normalizar CICIDS2017 y cargarlo en PostgreSQL."""

from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import pandas as pd

from config import settings
from db.connection import NETWORK_TRAFFIC_COLUMNS, ensure_database_and_table, insert_rows_batch


SELECTED_COLUMNS = [
    "Flow Duration",
    "Total Fwd Packets",
    "Total Backward Packets",
    "Total Length of Fwd Packets",
    "Total Length of Bwd Packets",
    "Flow Bytes/s",
    "Flow Packets/s",
    "Fwd Packet Length Mean",
    "Bwd Packet Length Mean",
    "SYN Flag Count",
    "ACK Flag Count",
    "PSH Flag Count",
    "URG Flag Count",
    "Label",
]

COLUMN_ALIASES = {
    "Total Length of Fwd Packets": ["Fwd Packets Length Total"],
    "Total Length of Bwd Packets": ["Bwd Packets Length Total"],
}

LABEL_MAPPING = {
    "BENIGN": 0,
    "ATTACK": 1,
}

RENAME_MAP = {
    "Flow Duration": "flow_duration",
    "Total Fwd Packets": "total_fwd_packets",
    "Total Backward Packets": "total_backward_packets",
    "Total Length of Fwd Packets": "total_length_of_fwd_packets",
    "Total Length of Bwd Packets": "total_length_of_bwd_packets",
    "Flow Bytes/s": "flow_bytes_per_s",
    "Flow Packets/s": "flow_packets_per_s",
    "Fwd Packet Length Mean": "fwd_packet_length_mean",
    "Bwd Packet Length Mean": "bwd_packet_length_mean",
    "SYN Flag Count": "syn_flag_count",
    "ACK Flag Count": "ack_flag_count",
    "PSH Flag Count": "psh_flag_count",
    "URG Flag Count": "urg_flag_count",
    "Label": "label",
}

NUMERIC_COLUMNS = [
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
]

INTEGER_COLUMNS = {
    "total_fwd_packets",
    "total_backward_packets",
    "syn_flag_count",
    "ack_flag_count",
    "psh_flag_count",
    "urg_flag_count",
}


@dataclass
class ETLResult:
    """Resultado agregado del proceso de carga."""
    processed_rows: int
    inserted_rows: int


class CICIDS2017ETL:
    """Lee, limpia y persiste el dataset CICIDS2017 en lotes."""

    def __init__(self, csv_path: str, chunk_size: int = 100000, batch_size: int = 5000) -> None:
        """Guarda la configuración de lectura y escritura por lotes."""
        self.csv_path = Path(csv_path)
        self.chunk_size = chunk_size
        self.batch_size = batch_size

    def read_chunks(self) -> Iterator[pd.DataFrame]:
        """Lee el CSV por chunks conservando solo las columnas esperadas."""
        allowed_columns = set(SELECTED_COLUMNS)
        for aliases in COLUMN_ALIASES.values():
            allowed_columns.update(aliases)

        for chunk in pd.read_csv(
            self.csv_path,
            chunksize=self.chunk_size,
            usecols=lambda column: column.strip() in allowed_columns,
            low_memory=False,
        ):
            yield chunk

    def clean_chunk(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """Normaliza nombres, tipos y etiquetas antes de insertar en PostgreSQL."""
        cleaned = chunk.copy()
        cleaned.columns = [column.strip() for column in cleaned.columns]

        for canonical_name, aliases in COLUMN_ALIASES.items():
            if canonical_name not in cleaned.columns:
                for alias in aliases:
                    if alias in cleaned.columns:
                        cleaned[canonical_name] = cleaned[alias]
                        break

        cleaned = cleaned.replace(["Infinity", "inf", "-inf", float("inf"), float("-inf")], pd.NA)
        cleaned = cleaned.dropna(subset=SELECTED_COLUMNS)
        cleaned["Label"] = cleaned["Label"].astype(str).str.strip().str.upper()
        cleaned["Label"] = cleaned["Label"].where(cleaned["Label"] == "BENIGN", "ATTACK")
        cleaned["Label"] = cleaned["Label"].map(LABEL_MAPPING)
        cleaned = cleaned.rename(columns=RENAME_MAP)

        for column in NUMERIC_COLUMNS:
            cleaned[column] = pd.to_numeric(cleaned[column], errors="coerce")

        cleaned = cleaned.dropna(subset=NETWORK_TRAFFIC_COLUMNS)
        for column in INTEGER_COLUMNS:
            cleaned[column] = cleaned[column].astype("int64")

        cleaned["label"] = cleaned["label"].astype("int64")
        return cleaned[NETWORK_TRAFFIC_COLUMNS]

    def dataframe_to_rows(self, dataframe: pd.DataFrame) -> list[tuple[object, ...]]:
        """Convierte un DataFrame limpio en tuplas aptas para execute_values()."""
        return [tuple(row) for row in dataframe.itertuples(index=False, name=None)]

    def process_file(self) -> ETLResult:
        """Ejecuta la carga completa y devuelve métricas de procesamiento."""
        ensure_database_and_table()

        total_processed_rows = 0
        total_inserted_rows = 0

        for raw_chunk in self.read_chunks():
            total_processed_rows += len(raw_chunk)
            cleaned_chunk = self.clean_chunk(raw_chunk)

            if cleaned_chunk.empty:
                continue

            rows = self.dataframe_to_rows(cleaned_chunk)
            for start_index in range(0, len(rows), self.batch_size):
                batch = rows[start_index : start_index + self.batch_size]
                total_inserted_rows += insert_rows_batch(batch)

        return ETLResult(
            processed_rows=total_processed_rows,
            inserted_rows=total_inserted_rows,
        )


def run_etl(csv_path: str | None = None) -> ETLResult:
    """Punto de entrada del ETL reutilizable desde CLI o tests."""
    etl = CICIDS2017ETL(
        csv_path=csv_path or settings.csv_path,
        chunk_size=settings.chunk_size,
        batch_size=settings.batch_size,
    )
    return etl.process_file()
