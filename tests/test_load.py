"""Tests for loading processed weather records into history."""

from __future__ import annotations

import csv

import pytest

from weather_etl.config import Settings
from weather_etl.load import WeatherLoadError, load_weather_data, read_processed_records
from weather_etl.transform import CSV_FIELDS


def write_processed_csv(path, run="20260714_120000"):
    record = {
        "run": run,
        "city": "New York",
        "lat": "40.7128",
        "lon": "-74.0060",
        "extracted_at": "2026-07-14T12:00:00-04:00",
        "weather_time": "2026-07-14T12:00",
        "temp_c": "28.0",
        "wind_kmh": "11.2",
        "wind_deg": "180",
        "weather_code": "1",
        "is_day": "1",
        "timezone": "America/New_York",
        "source": "Open-Meteo API",
    }
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerow(record)


def read_history(path):
    with path.open("r", newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))


def test_load_creates_history_file(tmp_path):
    settings = Settings(data_dir=tmp_path)
    clean_file = tmp_path / "processed.csv"
    write_processed_csv(clean_file)

    history_file = load_weather_data(clean_file, settings)

    rows = read_history(settings.history_file)
    assert history_file == str(settings.history_file)
    assert len(rows) == 1
    assert rows[0]["run"] == "20260714_120000"


def test_load_skips_duplicate_run_records(tmp_path):
    settings = Settings(data_dir=tmp_path)
    clean_file = tmp_path / "processed.csv"
    write_processed_csv(clean_file)

    load_weather_data(clean_file, settings)
    load_weather_data(clean_file, settings)

    assert len(read_history(settings.history_file)) == 1


def test_load_appends_new_run_records(tmp_path):
    settings = Settings(data_dir=tmp_path)
    first = tmp_path / "first.csv"
    second = tmp_path / "second.csv"
    write_processed_csv(first, "20260714_120000")
    write_processed_csv(second, "20260714_130000")

    load_weather_data(first, settings)
    load_weather_data(second, settings)

    assert [row["run"] for row in read_history(settings.history_file)] == ["20260714_120000", "20260714_130000"]


def test_load_rejects_missing_processed_file(tmp_path):
    with pytest.raises(WeatherLoadError, match="not found"):
        read_processed_records(tmp_path / "missing.csv")


def test_load_rejects_missing_columns(tmp_path):
    bad_file = tmp_path / "bad.csv"
    bad_file.write_text("run,city\n20260714_120000,New York\n", encoding="utf-8")

    with pytest.raises(WeatherLoadError, match="missing columns"):
        read_processed_records(bad_file)
