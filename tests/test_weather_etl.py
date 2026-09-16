import csv
import importlib
import json

import pytest

import weather_etl


class FakeResponse:
    def raise_for_status(self):
        pass

    def json(self):
        return {
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


@pytest.fixture
def settings(tmp_path):
    return weather_etl.Settings(
        data_dir=tmp_path,
    )


def test_extract(monkeypatch, settings):
    monkeypatch.setattr(
        weather_etl.requests,
        "get",
        lambda *args, **kwargs: FakeResponse(),
    )

    raw_file = weather_etl.extract_weather(settings)

    with open(raw_file, encoding="utf-8") as file:
        data = json.load(file)

    assert data["metadata"]["city"] == "New York"
    assert data["current_weather"]["temperature"] == 28.0


def test_transform(settings, tmp_path):
    payload = {
        "metadata": {
            "run": "20260714_120000",
            "city": "New York",
            "lat": 40.7128,
            "lon": -74.006,
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

    raw_file = tmp_path / "raw.json"
    raw_file.write_text(
        json.dumps(payload),
        encoding="utf-8",
    )

    clean_file = weather_etl.transform_weather(
        raw_file,
        settings,
    )

    with open(
        clean_file,
        newline="",
        encoding="utf-8",
    ) as file:
        row = next(csv.DictReader(file))

    assert row["city"] == "New York"
    assert row["temp_c"] == "28.0"


def test_load_skips_duplicates(settings, tmp_path):
    clean_file = tmp_path / "clean.csv"

    record = {field: "1" for field in weather_etl.FIELDS}

    record["run"] = "20260714_120000"

    with clean_file.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=weather_etl.FIELDS,
        )
        writer.writeheader()
        writer.writerow(record)

    weather_etl.load_weather(
        clean_file,
        settings,
    )

    weather_etl.load_weather(
        clean_file,
        settings,
    )

    with settings.history_file.open(
        newline="",
        encoding="utf-8",
    ) as file:
        rows = list(csv.DictReader(file))

    assert len(rows) == 1


def test_dag_import():
    module = importlib.import_module("dags.weather_etl_dag")

    assert module.DAG_ID == "daily_weather_etl"
