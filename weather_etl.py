import csv
import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

FIELDS = [
    "run",
    "city",
    "lat",
    "lon",
    "extracted_at",
    "weather_time",
    "temp_c",
    "wind_kmh",
    "wind_deg",
    "weather_code",
    "is_day",
    "timezone",
    "source",
]


@dataclass
class Settings:
    city: str = os.getenv("WEATHER_CITY", "New York")
    latitude: float = float(os.getenv("WEATHER_LATITUDE", "40.7128"))
    longitude: float = float(os.getenv("WEATHER_LONGITUDE", "-74.0060"))
    timezone: str = os.getenv("WEATHER_TIMEZONE", "America/New_York")
    api_url: str = os.getenv(
        "OPEN_METEO_API_URL",
        "https://api.open-meteo.com/v1/forecast",
    )
    timeout: int = int(os.getenv("WEATHER_REQUEST_TIMEOUT_SECONDS", "30"))
    data_dir: Path = Path(os.getenv("WEATHER_DATA_DIR", "data"))

    @property
    def raw_dir(self):
        return self.data_dir / "raw"

    @property
    def processed_dir(self):
        return self.data_dir / "processed"

    @property
    def history_file(self):
        return self.data_dir / "weather_history.csv"

    def create_directories(self):
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)


def city_filename(city):
    return "_".join(city.lower().split())


def extract_weather(settings=None):
    settings = settings or Settings()
    settings.create_directories()

    response = requests.get(
        settings.api_url,
        params={
            "latitude": settings.latitude,
            "longitude": settings.longitude,
            "current_weather": "true",
            "timezone": settings.timezone,
        },
        timeout=settings.timeout,
    )

    response.raise_for_status()
    payload = response.json()

    weather = payload.get("current_weather")

    required = {
        "time",
        "temperature",
        "windspeed",
        "winddirection",
        "weathercode",
        "is_day",
    }

    if not isinstance(weather, dict) or not required.issubset(weather) or "timezone" not in payload:
        raise ValueError("Invalid weather response")

    now = datetime.now(ZoneInfo(settings.timezone))
    run_id = now.strftime("%Y%m%d_%H%M%S")

    payload["metadata"] = {
        "run": run_id,
        "city": settings.city,
        "lat": settings.latitude,
        "lon": settings.longitude,
        "time": now.isoformat(),
        "source": "Open-Meteo API",
    }

    raw_file = settings.raw_dir / f"{city_filename(settings.city)}_raw_{run_id}.json"

    raw_file.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )

    return str(raw_file)


def transform_weather(raw_file, settings=None):
    settings = settings or Settings()
    settings.create_directories()

    payload = json.loads(Path(raw_file).read_text(encoding="utf-8"))

    metadata = payload["metadata"]
    weather = payload["current_weather"]

    record = {
        "run": metadata["run"],
        "city": metadata["city"],
        "lat": metadata["lat"],
        "lon": metadata["lon"],
        "extracted_at": metadata["time"],
        "weather_time": weather["time"],
        "temp_c": weather["temperature"],
        "wind_kmh": weather["windspeed"],
        "wind_deg": weather["winddirection"],
        "weather_code": weather["weathercode"],
        "is_day": weather["is_day"],
        "timezone": payload["timezone"],
        "source": metadata["source"],
    }

    clean_file = (
        settings.processed_dir / f"{city_filename(settings.city)}_clean_{record['run']}.csv"
    )

    with clean_file.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=FIELDS,
        )
        writer.writeheader()
        writer.writerow(record)

    return str(clean_file)


def load_weather(clean_file, settings=None):
    settings = settings or Settings()
    settings.create_directories()

    with open(
        clean_file,
        newline="",
        encoding="utf-8",
    ) as file:
        record = next(csv.DictReader(file), None)

    if not record:
        raise ValueError("Processed weather file is empty")

    existing_runs = set()

    if settings.history_file.exists():
        with settings.history_file.open(
            newline="",
            encoding="utf-8",
        ) as file:
            existing_runs = {row["run"] for row in csv.DictReader(file)}

    if record["run"] in existing_runs:
        return str(settings.history_file)

    write_header = not settings.history_file.exists() or settings.history_file.stat().st_size == 0

    with settings.history_file.open(
        "a",
        newline="",
        encoding="utf-8",
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=FIELDS,
        )

        if write_header:
            writer.writeheader()

        writer.writerow(record)

    return str(settings.history_file)


def run_pipeline(settings=None):
    settings = settings or Settings()

    raw_file = extract_weather(settings)
    clean_file = transform_weather(raw_file, settings)

    return load_weather(clean_file, settings)


if __name__ == "__main__":
    print(run_pipeline())
