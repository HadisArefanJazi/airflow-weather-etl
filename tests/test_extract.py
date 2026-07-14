"""Tests for weather extraction behavior."""

from __future__ import annotations

import json

import pytest
import requests

from weather_etl.config import Settings
from weather_etl.extract import WeatherExtractionError, request_weather_payload, write_raw_payload


class FakeResponse:
    def __init__(self, payload=None, status_code: int = 200, json_error: Exception | None = None):
        self.payload = payload
        self.status_code = status_code
        self.json_error = json_error

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError("bad response")

    def json(self):
        if self.json_error:
            raise self.json_error
        return self.payload


@pytest.fixture
def settings(tmp_path):
    return Settings(data_dir=tmp_path)


def valid_payload():
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


def test_successful_extraction_writes_raw_json(monkeypatch, settings):
    monkeypatch.setattr("weather_etl.extract.requests.get", lambda *_, **__: FakeResponse(valid_payload()))

    payload = request_weather_payload(settings)
    raw_path = write_raw_payload(payload, settings, run_id="20260714_120000")

    saved = json.loads(raw_path.read_text(encoding="utf-8"))
    assert saved["metadata"]["city"] == "New York"
    assert saved["current_weather"]["temperature"] == 28.0


def test_extraction_timeout_raises_clear_error(monkeypatch, settings):
    def raise_timeout(*_, **__):
        raise requests.Timeout("timeout")

    monkeypatch.setattr("weather_etl.extract.requests.get", raise_timeout)

    with pytest.raises(WeatherExtractionError, match="timed out"):
        request_weather_payload(settings)


def test_extraction_http_error_raises_clear_error(monkeypatch, settings):
    monkeypatch.setattr("weather_etl.extract.requests.get", lambda *_, **__: FakeResponse(valid_payload(), 500))

    with pytest.raises(WeatherExtractionError, match="HTTP 500"):
        request_weather_payload(settings)


def test_extraction_malformed_json_raises_clear_error(monkeypatch, settings):
    monkeypatch.setattr(
        "weather_etl.extract.requests.get",
        lambda *_, **__: FakeResponse(json_error=ValueError("not json")),
    )

    with pytest.raises(WeatherExtractionError, match="not valid JSON"):
        request_weather_payload(settings)


def test_extraction_missing_required_fields_raises_clear_error(monkeypatch, settings):
    payload = {"current_weather": {"temperature": 28.0}, "timezone": "America/New_York"}
    monkeypatch.setattr("weather_etl.extract.requests.get", lambda *_, **__: FakeResponse(payload))

    with pytest.raises(WeatherExtractionError, match="Missing weather fields"):
        request_weather_payload(settings)
