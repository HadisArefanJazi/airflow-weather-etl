# Airflow Weather ETL

A small ETL pipeline that collects current New York weather from Open-Meteo and runs daily with Apache Airflow.

## Flow

```text
Open-Meteo API
      ↓
Extract
      ↓
Raw JSON
      ↓
Transform
      ↓
Processed CSV
      ↓
Load
      ↓
data/weather_history.csv
```

## Setup

Requires Python 3.11+.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Run locally

```bash
python weather_etl.py
```

Generated files are written under:

```text
data/raw/
data/processed/
data/weather_history.csv
```

## Test

```bash
ruff check .
pytest
```

## Airflow with Docker

Build and start:

```bash
docker compose build
docker compose up airflow-init
docker compose up airflow-webserver airflow-scheduler
```

Open:

```text
http://localhost:8080
```

Login:

```text
username: admin
password: admin
```

The DAG is:

```text
daily_weather_etl
```

It runs:

```text
extract → transform → load
```

once per day.

Stop Airflow:

```bash
docker compose down
```
