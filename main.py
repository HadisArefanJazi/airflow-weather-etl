"""
Daily NYC Weather ETL Pipeline with Apache Airflow.

Flow:
1. Get weather data from Open-Meteo API.
2. Save raw JSON.
3. Convert JSON into one clean CSV row.
4. Add the row to a history CSV file.
5. Print the history CSV content.
"""

import csv
import json
import re
from datetime import datetime, timedelta
from pathlib  import Path
from zoneinfo import ZoneInfo

import requests

try:
    from airflow.decorators import dag, task
except ImportError:
    dag = None
    task = None


city = "New York"
lat  = 40.7128
lon  = -74.0060
tz   = "America/New_York"
url  = "https://api.open-meteo.com/v1/forecast"

base       = Path(__file__).resolve().parent
data_dir   = base / "weather_data"
raw_dir    = data_dir / "raw"
clean_dir  = data_dir / "processed"
history    = data_dir / "weather_history.csv"


def make_dirs():
    """Create needed folders."""
    raw_dir.mkdir(parents=True, exist_ok=True)
    clean_dir.mkdir(parents=True, exist_ok=True)


def slug(text):
    """Make safe filename text."""
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def now_id():
    """Create unique run id."""
    return datetime.now(ZoneInfo(tz)).strftime("%Y%m%d_%H%M%S")


def extract():
    """Get API data and save raw JSON."""
    make_dirs()

    run = now_id()

    params = {
        "latitude": lat,
        "longitude": lon,
        "current_weather": "true",
        "timezone": tz,
    }

    res = requests.get(url, params=params, timeout=30)
    res.raise_for_status()

    data = res.json()

    data["metadata"] = {
        "run": run,
        "city": city,
        "lat": lat,
        "lon": lon,
        "time": datetime.now(ZoneInfo(tz)).isoformat(),
        "source": "Open-Meteo API",
    }

    raw_file = raw_dir / f"{slug(city)}_raw_{run}.json"

    with raw_file.open("w", encoding="utf-8") as file:
        json.dump(data, file, indent=2)

    return str(raw_file)


def transform(raw_file):
    """Convert raw JSON to clean CSV."""
    make_dirs()

    with open(raw_file, "r", encoding="utf-8") as file:
        data = json.load(file)

    meta = data["metadata"]
    weather = data["current_weather"]

    row = {
        "run": meta["run"],
        "city": meta["city"],
        "lat": meta["lat"],
        "lon": meta["lon"],
        "extracted_at": meta["time"],
        "weather_time": weather["time"],
        "temp_c": weather["temperature"],
        "wind_kmh": weather["windspeed"],
        "wind_deg": weather["winddirection"],
        "weather_code": weather["weathercode"],
        "is_day": weather["is_day"],
        "timezone": data["timezone"],
        "source": meta["source"],
    }

    clean_file = clean_dir / f"{slug(city)}_clean_{row['run']}.csv"

    with clean_file.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=row.keys())
        writer.writeheader()
        writer.writerow(row)

    return str(clean_file)


def load(clean_file):
    """Add clean row to history CSV."""
    with open(clean_file, "r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        row = next(reader)

    exists = history.exists()

    with history.open("a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=row.keys())

        if not exists:
            writer.writeheader()

        writer.writerow(row)

    return str(history)


def run():
    """Run full pipeline."""
    raw_file = extract()
    clean_file = transform(raw_file)
    history_file = load(clean_file)
    return history_file


if dag and task:

    @dag(
        dag_id="daily_weather_etl",
        schedule="@daily",
        start_date=datetime(2026, 1, 1, tzinfo=ZoneInfo(tz)),
        catchup=False,
        default_args={
            "retries": 2,
            "retry_delay": timedelta(minutes=1),
        },
        tags=["etl", "weather", "api", "csv"],
    )
    def weather_etl():

        @task
        def extract_task():
            return extract()

        @task
        def transform_task(raw_file):
            return transform(raw_file)

        @task
        def load_task(clean_file):
            return load(clean_file)

        raw_file = extract_task()
        clean_file = transform_task(raw_file)
        load_task(clean_file)

    weather_etl()


if __name__ == "__main__":
    result = run()

    print("Weather ETL completed.")
    print(f"History file saved to: {result}")
    print("\nHistory CSV content:\n")

    with open(result, "r", encoding="utf-8") as file:
        print(file.read())
