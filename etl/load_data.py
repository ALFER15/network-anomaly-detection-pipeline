from __future__ import annotations

"""CLI mínima para ejecutar la carga ETL desde consola."""

import argparse
import logging

from etl.cicids2017_etl import run_etl


LOGGER = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Define los argumentos de entrada para el proceso de carga."""
    parser = argparse.ArgumentParser(description="Load CICIDS2017 data into PostgreSQL.")
    parser.add_argument("--csv-path", required=False, help="Path to the CICIDS2017 CSV file.")
    return parser.parse_args()


def main() -> None:
    """Ejecuta el ETL y muestra un resumen simple por consola."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = parse_args()
    LOGGER.info("etl_start csv_path=%s", args.csv_path)
    result = run_etl(csv_path=args.csv_path)
    print(f"Processed rows: {result.processed_rows}")
    print(f"Inserted rows: {result.inserted_rows}")
    LOGGER.info("etl_finished processed_rows=%s inserted_rows=%s", result.processed_rows, result.inserted_rows)


if __name__ == "__main__":
    main()
