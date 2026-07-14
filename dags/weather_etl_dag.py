"""Airflow DAG definition for the daily Open-Meteo weather ETL."""

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

try:
    from airflow.decorators import dag, task
except ImportError:  # Allows lightweight local import checks without Airflow installed.
    dag = None
    task = None

from weather_etl.config import Settings
from weather_etl.extract import extract_weather_data
from weather_etl.load import load_weather_data
from weather_etl.transform import transform_weather_data

SETTINGS = Settings()
DAG_ID = "daily_weather_etl"


if dag is not None and task is not None:

    @dag(
        dag_id=DAG_ID,
        description="Extract, transform, and load daily NYC weather data from Open-Meteo.",
        schedule="@daily",
        start_date=datetime(2026, 1, 1, tzinfo=ZoneInfo(SETTINGS.timezone)),
        catchup=False,
        default_args={
            "retries": 2,
            "retry_delay": timedelta(minutes=1),
        },
        tags=["etl", "weather", "open-meteo", "csv"],
    )
    def weather_etl_dag():
        """Define Airflow task dependencies without executing ETL work during import."""

        @task(task_id="extract_weather")
        def extract_task() -> str:
            return extract_weather_data(SETTINGS)

        @task(task_id="transform_weather")
        def transform_task(raw_file: str) -> str:
            return transform_weather_data(raw_file, SETTINGS)

        @task(task_id="load_weather_history")
        def load_task(clean_file: str) -> str:
            return load_weather_data(clean_file, SETTINGS)

        raw_file = extract_task()
        clean_file = transform_task(raw_file)
        load_task(clean_file)

    weather_etl = weather_etl_dag()
else:
    weather_etl = None
