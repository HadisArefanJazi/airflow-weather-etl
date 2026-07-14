"""Extract current weather data from the Open-Meteo API."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests
from requests import Response

from weather_etl.config import Settings, slugify

LOGGER = logging.getLogger(__name__)


class WeatherExtractionError(RuntimeError):
    """Raised when weather data cannot be extracted or validated."""


def build_open_meteo_params(settings: Settings) -> dict[str, str | float]:
    """Build the Open-Meteo request parameters used by the ETL."""
    return {
        "latitude": settings.latitude,
        "longitude": settings.longitude,
        "current_weather": "true",
        "timezone": settings.timezone,
    }


def _parse_json_response(response: Response) -> dict[str, Any]:
    try:
        payload = response.json()
    except ValueError as exc:
        raise WeatherExtractionError("Open-Meteo response was not valid JSON.") from exc

    if not isinstance(payload, dict):
        raise WeatherExtractionError("Open-Meteo response JSON must be an object.")
    return payload


def validate_weather_payload(payload: dict[str, Any]) -> None:
    """Validate the Open-Meteo fields required by the downstream transform."""
    current_weather = payload.get("current_weather")
    if not isinstance(current_weather, dict):
        raise WeatherExtractionError("Missing 'current_weather' object in Open-Meteo response.")

    required_fields = {"time", "temperature", "windspeed", "winddirection", "weathercode", "is_day"}
    missing = sorted(field for field in required_fields if field not in current_weather)
    if missing:
        raise WeatherExtractionError(f"Missing weather fields: {', '.join(missing)}")

    if "timezone" not in payload:
        raise WeatherExtractionError("Missing 'timezone' in Open-Meteo response.")


def request_weather_payload(settings: Settings | None = None) -> dict[str, Any]:
    """Request and validate a current-weather payload from Open-Meteo."""
    settings = settings or Settings()
    params = build_open_meteo_params(settings)

    try:
        LOGGER.info("Requesting weather data for %s from Open-Meteo.", settings.city)
        response = requests.get(settings.api_url, params=params, timeout=settings.request_timeout_seconds)
        response.raise_for_status()
    except requests.Timeout as exc:
        raise WeatherExtractionError("Open-Meteo request timed out.") from exc
    except requests.HTTPError as exc:
        raise WeatherExtractionError(f"Open-Meteo request failed with HTTP {response.status_code}.") from exc
    except requests.RequestException as exc:
        raise WeatherExtractionError("Open-Meteo request failed.") from exc

    payload = _parse_json_response(response)
    validate_weather_payload(payload)
    return payload


def write_raw_payload(payload: dict[str, Any], settings: Settings | None = None, run_id: str | None = None) -> Path:
    """Add metadata to an API payload and persist it as raw JSON."""
    settings = settings or Settings()
    settings.ensure_directories()
    run_id = run_id or settings.run_id()

    payload_with_metadata = dict(payload)
    payload_with_metadata["metadata"] = {
        "run": run_id,
        "city": settings.city,
        "lat": settings.latitude,
        "lon": settings.longitude,
        "time": datetime.now(ZoneInfo(settings.timezone)).isoformat(),
        "source": "Open-Meteo API",
    }

    raw_file = settings.raw_dir / f"{slugify(settings.city)}_raw_{run_id}.json"
    with raw_file.open("w", encoding="utf-8") as file:
        json.dump(payload_with_metadata, file, indent=2)

    LOGGER.info("Saved raw weather payload to %s.", raw_file)
    return raw_file


def extract_weather_data(settings: Settings | None = None) -> str:
    """Extract current weather data and return the saved raw JSON path."""
    settings = settings or Settings()
    payload = request_weather_payload(settings)
    return str(write_raw_payload(payload, settings))
