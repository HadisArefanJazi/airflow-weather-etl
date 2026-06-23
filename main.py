"""
Apache Airflow DAG for the Daily Weather ETL Pipeline.

The DAG schedules and orchestrates the three ETL stages defined in
weather_pipeline.py:

1. Extract raw weather data.
2. Transform raw JSON into clean CSV format.
3. Load the clean record into the historical CSV dataset.
"""

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

from weather_pipeline import extract_weather_data
from weather_pipeline import load_weather_data
from weather_pipeline import transform_weather_data


@dag(
    dag_id="daily_weather_etl",
    description="Extract, transform, and load daily NYC weather data.",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/New_York"),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["beginner", "etl", "weather"],
)
def daily_weather_etl():
    """
    Define the Airflow task graph for the weather ETL workflow.

    Task outputs are passed directly into downstream tasks using Airflow's
    TaskFlow API, allowing Airflow to manage dependencies and XCom values.
    """

    @task
    def extract_task() -> str:
        """
        Retrieve the latest NYC weather forecast and store the raw JSON payload.

        Returns:
            Path to the saved raw JSON file.
        """
        return extract_weather_data(
            city="New York",
            latitude=40.7128,
            longitude=-74.0060,
        )

    @task
    def transform_task(raw_file_path: str) -> str:
        """
        Convert the raw weather JSON file into a clean one-row CSV file.

        Args:
            raw_file_path: Path produced by the extract task.

        Returns:
            Path to the processed CSV file.
        """
        return transform_weather_data(raw_file_path)

    @task
    def load_task(clean_file_path: str) -> str:
        """
        Load the processed weather record into the historical CSV dataset.

        Args:
            clean_file_path: Path produced by the transform task.

        Returns:
            Path to the final historical CSV file.
        """
        return load_weather_data(clean_file_path)

    raw_file = extract_task()
    clean_file = transform_task(raw_file)
    load_task(clean_file)


daily_weather_etl()
