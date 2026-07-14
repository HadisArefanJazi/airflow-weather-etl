"""Transform raw Open-Meteo JSON into the project's CSV schema."""

from __future__ import annotations

import csv
import json
import logging
from pathlib import Path
from typing import Any

from weather_etl.config import Settings, slugify

LOGGER = logging.getLogger(__name__)

CSV_FIELDS = [
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


class WeatherTransformationError(ValueError):
    """Raised when raw weather data cannot be transformed."""


def read_raw_payload(raw_file: str | Path) -> dict[str, Any]:
    """Read a raw JSON weather payload from disk."""
    raw_path = Path(raw_file)
    if not raw_path.exists():
        raise WeatherTransformationError(f"Raw weather file not found: {raw_path}")

    try:
        with raw_path.open("r", encoding="utf-8") as file:
            payload = json.load(file)
    except json.JSONDecodeError as exc:
        raise WeatherTransformationError(f"Raw weather file is malformed JSON: {raw_path}") from exc

    if not isinstance(payload, dict):
        raise WeatherTransformationError("Raw weather payload must be a JSON object.")
    return payload


def transform_payload_to_record(payload: dict[str, Any]) -> dict[str, Any]:
    """Convert a validated Open-Meteo payload into one CSV-ready record."""
    metadata = payload.get("metadata")
    weather = payload.get("current_weather")

    if not isinstance(metadata, dict):
        raise WeatherTransformationError("Raw weather payload is missing metadata.")
    if not isinstance(weather, dict):
        raise WeatherTransformationError("Raw weather payload is missing current_weather.")

    required_metadata = {"run", "city", "lat", "lon", "time", "source"}
    required_weather = {"time", "temperature", "windspeed", "winddirection", "weathercode", "is_day"}
    missing_metadata = sorted(field for field in required_metadata if field not in metadata)
    missing_weather = sorted(field for field in required_weather if field not in weather)
    if missing_metadata:
        raise WeatherTransformationError(f"Missing metadata fields: {', '.join(missing_metadata)}")
    if missing_weather:
        raise WeatherTransformationError(f"Missing current_weather fields: {', '.join(missing_weather)}")
    if "timezone" not in payload:
        raise WeatherTransformationError("Raw weather payload is missing timezone.")

    return {
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


def write_processed_record(record: dict[str, Any], settings: Settings | None = None) -> Path:
    """Write one weather record to the processed CSV directory."""
    settings = settings or Settings()
    settings.ensure_directories()

    missing = [field for field in CSV_FIELDS if field not in record]
    if missing:
        raise WeatherTransformationError(f"Processed record is missing fields: {', '.join(missing)}")

    clean_file = settings.processed_dir / f"{slugify(str(record['city']))}_clean_{record['run']}.csv"
    with clean_file.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerow({field: record[field] for field in CSV_FIELDS})

    LOGGER.info("Saved processed weather record to %s.", clean_file)
    return clean_file


def transform_weather_data(raw_file: str | Path, settings: Settings | None = None) -> str:
    """Transform raw weather JSON into a one-row CSV and return its path."""
    payload = read_raw_payload(raw_file)
    record = transform_payload_to_record(payload)
    return str(write_processed_record(record, settings))
