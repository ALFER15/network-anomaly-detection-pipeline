# Network Monitoring System

[![Python](https://img.shields.io/badge/Python-3.11-blue)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115.8-009688)](https://fastapi.tiangolo.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16+-336791)](https://www.postgresql.org/)
[![scikit-learn](https://img.shields.io/badge/scikit--learn-1.8.0-f7931e)](https://scikit-learn.org/)

## ¿Qué es este proyecto?

Este proyecto toma tráfico de red del dataset CICIDS2017, lo limpia, lo guarda en PostgreSQL, detecta anomalías con Machine Learning y expone resultados en una API con FastAPI.

Está pensado para portafolio: muestra un flujo completo de datos, desde ETL hasta visualización en Power BI.

## Resultado principal

- 380000 registros procesados y almacenados
- 380000 registros scoreados en `network_traffic_scored`
- 7553 anomalías detectadas

## Arquitectura

```text
ETL -> PostgreSQL -> ML Scoring -> PostgreSQL (scored) -> FastAPI -> Dashboard
```

## Stack

- Python 3.11
- pandas
- PostgreSQL
- FastAPI
- scikit-learn (Isolation Forest)

## Endpoints principales

- GET `/health` estado del servicio
- GET `/metrics/summary` resumen general
- GET `/anomalies/ml` anomalías detectadas por el modelo
- GET `/top_suspicious` flujos con mayor score
- POST `/predict` predicción para un solo flujo

## Ejecución local (rápida)

### 1. Configurar `.env`

```dotenv
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=network_monitoring_db
CSV_PATH=C:\Users\blak_\Documents\cicids2017\Benign-Monday-no-metadata.csv
CHUNK_SIZE=100000
BATCH_SIZE=5000
API_HOST=0.0.0.0
API_PORT=8000
```

### 2. Instalar dependencias

```powershell
py -3.11 -m pip install -r requirements.txt
```

### 3. Cargar datos (ETL)

```powershell
py -3.11 -m etl.load_data --csv-path "C:\Users\blak_\Documents\cicids2017\Benign-Monday-no-metadata.csv"
```

### 4. Entrenar modelo

```powershell
py -3.11 .\ml\train_isolation_forest.py --sample-size 50000 --chunk-size 50000
```

### 5. Generar scoring

```powershell
py -3.11 .\ml\score_isolation_forest.py --chunk-size 100000 --replace-table
```

### 6. Levantar API

```powershell
py -3.11 -m uvicorn api.main:app --reload
```

## Dashboard (Power BI)

![Dashboard](images/dashboard.jpeg)
![Scatter Plot](images/scatter_plot.jpeg)
![Anomalies Table](images/anomalies_table.jpeg)

Archivo Power BI: `dashboards/traffic.pbix`

Conéctalo a PostgreSQL y usa la tabla `network_traffic_scored`.

## SQL útil

```sql
SELECT COUNT(*) FROM network_traffic;
SELECT COUNT(*) FROM network_traffic_scored;
SELECT COUNT(*) FROM network_traffic_scored WHERE is_anomaly = 1;
```

## Estado

Proyecto funcional de punta a punta y listo para presentación en GitHub.
