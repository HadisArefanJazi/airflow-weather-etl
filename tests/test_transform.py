"""Tests for weather transformation behavior."""

from __future__ import annotations

import csv
import json

import pytest

from weather_etl.config import Settings
from weather_etl.transform import CSV_FIELDS, WeatherTransformationError, transform_payload_to_record, transform_weather_data


def raw_payload():
    return {
        "metadata": {
            "run": "20260714_120000",
            "city": "New York",
            "lat": 40.7128,
            "lon": -74.0060,
            "time": "2026-07-14T12:00:00-04:00",
            "source": "Open-Meteo API",
        },
        "current_weather": {
            "time": "2026-07-14T12:00",
            "temperature": 28.0,
            "windspeed": 11.2,
            "winddirection": 180,
            "weathercode": 1,
            "is_day": 1,
        },
        "timezone": "America/New_York",
    }


def test_transform_payload_preserves_expected_csv_schema():
    record = transform_payload_to_record(raw_payload())

    assert list(record.keys()) == CSV_FIELDS
    assert record["temp_c"] == 28.0
    assert record["wind_kmh"] == 11.2


def test_transform_weather_data_writes_processed_csv(tmp_path):
    raw_file = tmp_path / "raw.json"
    raw_file.write_text(json.dumps(raw_payload()), encoding="utf-8")
    settings = Settings(data_dir=tmp_path)

    processed_file = transform_weather_data(raw_file, settings)

    with open(processed_file, newline="", encoding="utf-8") as file:
        rows = list(csv.DictReader(file))

    assert len(rows) == 1
    assert rows[0]["city"] == "New York"
    assert rows[0]["weather_code"] == "1"


def test_transform_rejects_missing_current_weather():
    payload = raw_payload()
    payload.pop("current_weather")

    with pytest.raises(WeatherTransformationError, match="current_weather"):
        transform_payload_to_record(payload)


def test_transform_rejects_malformed_json(tmp_path):
    raw_file = tmp_path / "raw.json"
    raw_file.write_text("{bad", encoding="utf-8")

    with pytest.raises(WeatherTransformationError, match="malformed JSON"):
        transform_weather_data(raw_file, Settings(data_dir=tmp_path))
