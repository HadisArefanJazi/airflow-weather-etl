"""
Daily NYC Weather ETL Pipeline with Apache Airflow.
 
1. Extracts current weather data from the public Open-Meteo API.
2. Saves the raw JSON response.
3. Transforms nested JSON into a clean one-row CSV file.
4. Loads the clean record into a historical CSV dataset.
5. Orchestrates the workflow with Apache Airflow TaskFlow API.
 
"""

from __future__ import annotations

import csv
import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests

try:
    from airflow.decorators import dag, task
except ImportError:
    dag = None
    task = None


# ============================================================
# Project configuration
# ============================================================

CITY = "New York"
LATITUDE = 40.7128
LONGITUDE = -74.0060
TIMEZONE = "America/New_York"

API_URL = "https://api.open-meteo.com/v1/forecast"

PROJECT_DIR = Path(__file__).resolve().parent
DATA_DIR = PROJECT_DIR / "weather_data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
HISTORICAL_FILE = DATA_DIR / "weather_history.csv"


# ============================================================
# Helper functions
# ============================================================

def create_directories() -> None:
    """Create local folders for raw, processed, and historical data."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def slugify(value: str) -> str:
    """Convert a city name into a safe filename."""
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def current_timestamp() -> str:
    """Return a timestamp in the project timezone."""
    return datetime.now(ZoneInfo(TIMEZONE)).strftime("%Y%m%d_%H%M%S")


def write_json(data: dict[str, Any], path: Path) -> None:
    """Write JSON data to disk."""
    with path.open("w", encoding="utf-8") as file:
        json.dump(data, file, indent=2)


def read_json(path: Path) -> dict[str, Any]:
    """Read JSON data from disk."""
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


# ============================================================
# Extract
# ============================================================

def extract_weather_data(
    city: str = CITY,
    latitude: float = LATITUDE,
    longitude: float = LONGITUDE,
) -> str:
    """
    Extract current weather data from Open-Meteo and save raw JSON.

    Returns:
        Path to the saved raw JSON file.
    """
    create_directories()

    run_id = current_timestamp()

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current_weather": "true",
        "timezone": TIMEZONE,
    }

    response = requests.get(API_URL, params=params, timeout=30)
    response.raise_for_status()

    payload = response.json()

    payload["metadata"] = {
        "run_id": run_id,
        "city": city,
        "latitude": latitude,
        "longitude": longitude,
        "extracted_at": datetime.now(ZoneInfo(TIMEZONE)).isoformat(),
        "source": "Open-Meteo API",
    }

    raw_file_path = RAW_DIR / f"{slugify(city)}_weather_raw_{run_id}.json"
    write_json(payload, raw_file_path)

    return str(raw_file_path)


# ============================================================
# Transform
# ============================================================

def transform_weather_data(raw_file_path: str) -> str:
    """
    Transform raw nested weather JSON into a clean one-row CSV file.

    Returns:
        Path to the processed CSV file.
    """
    create_directories()

    raw_path = Path(raw_file_path)
    payload = read_json(raw_path)

    metadata = payload.get("metadata", {})
    current_weather = payload.get("current_weather", {})

    if not current_weather:
        raise ValueError("Missing 'current_weather' data in API response.")

    clean_record = {
        "run_id": metadata.get("run_id"),
        "city": metadata.get("city"),
        "latitude": metadata.get("latitude"),
        "longitude": metadata.get("longitude"),
        "extracted_at": metadata.get("extracted_at"),
        "weather_time": current_weather.get("time"),
        "temperature_c": current_weather.get("temperature"),
        "windspeed_kmh": current_weather.get("windspeed"),
        "winddirection_deg": current_weather.get("winddirection"),
        "weather_code": current_weather.get("weathercode"),
        "is_day": current_weather.get("is_day"),
        "timezone": payload.get("timezone"),
        "source": metadata.get("source"),
    }

    processed_file_path = (
        PROCESSED_DIR
        / f"{slugify(str(clean_record['city']))}_weather_clean_{clean_record['run_id']}.csv"
    )

    with processed_file_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=clean_record.keys())
        writer.writeheader()
        writer.writerow(clean_record)

    return str(processed_file_path)


# ============================================================
# Load
# ============================================================

def load_weather_data(clean_file_path: str) -> str:
    """
    Append the processed weather record to a historical CSV dataset.

    Returns:
        Path to the historical CSV file.
    """
    clean_path = Path(clean_file_path)

    with clean_path.open("r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        rows = list(reader)

    if not rows:
        raise ValueError("Processed CSV file is empty.")

    new_record = rows[0]
    file_exists = HISTORICAL_FILE.exists()

    with HISTORICAL_FILE.open("a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=new_record.keys())

        if not file_exists:
            writer.writeheader()

        writer.writerow(new_record)

    return str(HISTORICAL_FILE)


# ============================================================
# Local pipeline runner
# ============================================================

def run_pipeline_locally() -> str:
    """
    Run the full ETL pipeline without Airflow.

    Useful for testing before placing the file inside the Airflow dags folder.
    """
    raw_file = extract_weather_data()
    clean_file = transform_weather_data(raw_file)
    historical_file = load_weather_data(clean_file)

    return historical_file


# ============================================================
# Airflow DAG
# ============================================================

if dag is not None and task is not None:

    @dag(
        dag_id="daily_weather_etl",
        description="Extract, transform, and load daily NYC weather data.",
        schedule="@daily",
        start_date=datetime(2026, 1, 1, tzinfo=ZoneInfo(TIMEZONE)),
        catchup=False,
        default_args={
            "retries": 2,
            "retry_delay": timedelta(minutes=1),
        },
        tags=["etl", "weather", "open-meteo", "csv"],
    )
    def daily_weather_etl():
        """
        Define the Airflow task graph.

        Flow:
        extract_task -> transform_task -> load_task
        """

        @task
        def extract_task() -> str:
            """Extract raw weather data and save it as JSON."""
            return extract_weather_data(
                city=CITY,
                latitude=LATITUDE,
                longitude=LONGITUDE,
            )

        @task
        def transform_task(raw_file_path: str) -> str:
            """Transform raw JSON into a clean CSV record."""
            return transform_weather_data(raw_file_path)

        @task
        def load_task(clean_file_path: str) -> str:
            """Append the clean CSV record to the historical dataset."""
            return load_weather_data(clean_file_path)

        raw_file = extract_task()
        clean_file = transform_task(raw_file)
        load_task(clean_file)

    daily_weather_etl()


# ============================================================
# Manual execution
# ============================================================

if __name__ == "__main__":
    output_path = run_pipeline_locally()
    print("Weather ETL pipeline completed successfully.")
    print(f"Historical dataset saved to: {output_path}")
