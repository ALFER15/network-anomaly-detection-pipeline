from __future__ import annotations

"""Utilidades de conexión y DDL para PostgreSQL."""

from contextlib import contextmanager
from typing import Iterator, Sequence

import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config import settings


TABLE_NAME = "network_traffic"
DATABASE_NAME = settings.postgres_db


NETWORK_TRAFFIC_COLUMNS = [
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
]


TABLE_DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
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
)
"""


def _admin_connection_parameters() -> dict[str, object]:
    """Construye los parámetros de conexión al catálogo `postgres`."""
    return {
        "host": settings.postgres_host,
        "port": settings.postgres_port,
        "user": settings.postgres_user,
        "password": settings.postgres_password,
        "dbname": "postgres",
    }


@contextmanager
def get_admin_connection() -> Iterator[psycopg2.extensions.connection]:
    """Abre una conexión administrativa para crear bases de datos."""
    connection = psycopg2.connect(**_admin_connection_parameters())
    try:
        yield connection
    finally:
        connection.close()


@contextmanager
def get_database_connection(database_name: str | None = None) -> Iterator[psycopg2.extensions.connection]:
    """Abre una conexión normal contra la base de datos objetivo."""
    connection = psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        user=settings.postgres_user,
        password=settings.postgres_password,
        dbname=database_name or DATABASE_NAME,
    )
    try:
        yield connection
    finally:
        connection.close()


def get_sqlalchemy_engine(database_name: str | None = None) -> Engine:
    """Crea un engine de SQLAlchemy reutilizable para operaciones DDL."""
    target_database = database_name or DATABASE_NAME
    url = (
        f"postgresql+psycopg2://{settings.postgres_user}:{settings.postgres_password}"
        f"@{settings.postgres_host}:{settings.postgres_port}/{target_database}"
    )
    return create_engine(url, pool_pre_ping=True, pool_size=5, max_overflow=10)


def ensure_database_exists() -> None:
    """Crea la base de datos de trabajo si todavía no existe."""
    with get_admin_connection() as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (DATABASE_NAME,),
            )
            exists = cursor.fetchone() is not None
            if not exists:
                cursor.execute(f'CREATE DATABASE "{DATABASE_NAME}"')


def ensure_table_exists() -> None:
    """Crea la tabla principal de tráfico si no está presente."""
    engine = get_sqlalchemy_engine()
    with engine.begin() as connection:
        connection.execute(text(TABLE_DDL))


def ensure_database_and_table() -> None:
    """Garantiza que la base y la tabla principal existan antes de cargar datos."""
    ensure_database_exists()
    ensure_table_exists()


def insert_rows_batch(rows: Sequence[Sequence[object]]) -> int:
    """Inserta un lote de filas ya normalizadas y devuelve cuántas cargó."""
    if not rows:
        return 0

    values_template = "(" + ", ".join(["%s"] * len(NETWORK_TRAFFIC_COLUMNS)) + ")"
    insert_statement = (
        f"INSERT INTO {TABLE_NAME} ({', '.join(NETWORK_TRAFFIC_COLUMNS)}) VALUES %s"
    )

    with get_database_connection() as connection:
        with connection.cursor() as cursor:
            execute_values(cursor, insert_statement, rows, template=values_template, page_size=5000)
        connection.commit()
    return len(rows)
