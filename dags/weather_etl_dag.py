from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from weather_etl import (
    Settings,
    extract_weather,
    load_weather,
    transform_weather,
)

try:
    from airflow.decorators import dag, task
except ImportError:
    dag = None
    task = None


SETTINGS = Settings()
DAG_ID = "daily_weather_etl"


if dag and task:

    @dag(
        dag_id=DAG_ID,
        schedule="@daily",
        start_date=datetime(
            2026,
            1,
            1,
            tzinfo=ZoneInfo(SETTINGS.timezone),
        ),
        catchup=False,
        default_args={
            "retries": 2,
            "retry_delay": timedelta(minutes=1),
        },
    )
    def weather_etl_dag():

        @task
        def extract():
            return extract_weather(SETTINGS)

        @task
        def transform(raw_file):
            return transform_weather(
                raw_file,
                SETTINGS,
            )

        @task
        def load(clean_file):
            return load_weather(
                clean_file,
                SETTINGS,
            )

        raw_file = extract()
        clean_file = transform(raw_file)
        load(clean_file)

    weather_etl = weather_etl_dag()

else:
    weather_etl = None
