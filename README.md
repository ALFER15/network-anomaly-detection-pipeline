# Network Anomaly Detection Pipeline

[![Python](https://img.shields.io/badge/Python-3.11-blue)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115.8-009688)](https://fastapi.tiangolo.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16+-336791)](https://www.postgresql.org/)
[![scikit-learn](https://img.shields.io/badge/scikit--learn-1.8.0-f7931e)](https://scikit-learn.org/)

## Network Monitoring & Anomaly Analytics

Plataforma de análisis de tráfico de red basada en CICIDS2017. El proyecto combina ETL en Python, modelado relacional en PostgreSQL, detección de anomalías con Isolation Forest y visualización en Power BI para detectar patrones sospechosos, picos de tráfico y flujos críticos.

## Problema que resuelve

En escenarios con alto volumen de tráfico, detectar comportamiento anómalo en tiempo útil es complejo cuando los datos están crudos o dispersos. Este proyecto centraliza el flujo completo para responder rápidamente preguntas como:

- Qué flujos tienen mayor probabilidad de ser anómalos.
- Qué segmentos presentan mayor intensidad de tráfico.
- Qué porcentaje del tráfico total cae en zona de riesgo.

## Arquitectura

- `etl/cicids2017_etl.py` limpia y normaliza el dataset CICIDS2017.
- PostgreSQL almacena la capa base (`network_traffic`) y la capa scoreada (`network_traffic_scored`).
- `ml/train_isolation_forest.py` entrena el modelo no supervisado.
- `ml/score_isolation_forest.py` calcula `anomaly_score` e `is_anomaly` para cada flujo.
- FastAPI expone métricas, anomalías y predicción puntual.
- Power BI consume la tabla scoreada para dashboard ejecutivo.

## Estructura del proyecto

```text
network_monitoring/
├── api/
│   ├── main.py
│   └── crud.py
├── db/
│   ├── connection.py
│   └── schema.sql
├── etl/
│   ├── cicids2017_etl.py
│   └── load_data.py
├── ml/
│   ├── runtime.py
│   ├── train_isolation_forest.py
│   └── score_isolation_forest.py
├── models/
│   └── schemas.py
├── sql/
│   ├── checks.sql
│   ├── analytics.sql
│   └── anomalies.sql
├── dashboards/
│   └── traffic.pbix
├── images/
├── tests/
├── README.md
├── requirements.txt
└── .env.example
```

## Modelo de datos

- `network_traffic`: tabla base con features de flujo normalizadas desde ETL.
- `network_traffic_scored`: tabla enriquecida con features derivadas y resultado de ML.

Relación principal:

- `network_traffic (1)` -> `network_traffic_scored (1)` por `id`.

## Métricas principales

- Total de flujos procesados.
- Total de anomalías detectadas (`is_anomaly = 1`).
- Ratio de anomalía sobre el total.
- P95 / P99 de `anomaly_score`.
- Endpoints de consulta para top tráfico y top sospechosos.

## Insights del dashboard

- Identificación rápida de flujos con score extremo.
- Detección de patrones de tráfico con comportamiento atípico.
- Priorización de investigación por volumen + score de anomalía.
- Comparación de distribución de tráfico benigno vs sospechoso.

## Resultados reales

- Registros cargados en `network_traffic`: `380000`
- Registros scoreados en `network_traffic_scored`: `380000`
- Anomalías detectadas por ML: `7553`

## Vista previa

### Dashboard general

![Dashboard](images/dashboard.jpeg)

### Latencia/Distribución de tráfico

![Latency](images/scatter_plot.jpeg)

### Error/Anomaly view 

![Error](images/anomalies_table.jpeg)

## Endpoints disponibles

- `GET /health`
- `GET /metrics/summary`
- `GET /metrics/advanced`
- `GET /anomalies`
- `GET /anomalies/ml`
- `GET /top_traffic`
- `GET /top_suspicious`
- `POST /predict`

## Tecnologías utilizadas

- Python 3.11
- pandas
- scikit-learn
- PostgreSQL
- SQL analítico
- FastAPI
- Power BI

## Cómo ejecutar el proyecto

### 1. Instalar dependencias

```bash
py -3.11 -m pip install -r requirements.txt
```

### 2. Configurar base de datos y entorno

- Crea la base en PostgreSQL.
- Configura variables en `.env` a partir de `.env.example`.

### 3. Cargar datos (ETL)

```bash
py -3.11 -m etl.load_data --csv-path "C:\Users\blak_\Documents\cicids2017\Benign-Monday-no-metadata.csv"
```

### 4. Entrenar modelo

```bash
py -3.11 .\ml\train_isolation_forest.py --sample-size 50000 --chunk-size 50000
```

### 5. Ejecutar scoring

```bash
py -3.11 .\ml\score_isolation_forest.py --chunk-size 100000 --replace-table
```

### 6. Levantar API

```bash
py -3.11 -m uvicorn api.main:app --reload
```

### 7. Cargar Power BI

Abre `dashboards/traffic.pbix` y conecta la fuente a PostgreSQL usando `network_traffic_scored` como tabla principal.

## SQL incluido

- `sql/checks.sql`: validaciones rápidas de volumen y anomalías.
- `sql/analytics.sql`: consultas agregadas para análisis exploratorio.
- `sql/anomalies.sql`: top de flujos sospechosos por score.

## Cierre

Este proyecto presenta una base sólida para monitoreo de red con enfoque analítico: ETL robusto, persistencia relacional, scoring de anomalías con ML y visualización ejecutiva para priorizar decisiones técnicas con datos.
