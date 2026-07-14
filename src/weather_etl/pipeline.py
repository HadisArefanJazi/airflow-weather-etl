"""Local pipeline runner for the weather ETL process."""

from __future__ import annotations

from weather_etl.config import Settings
from weather_etl.extract import extract_weather_data
from weather_etl.load import load_weather_data
from weather_etl.logging_config import configure_logging
from weather_etl.transform import transform_weather_data


def run_pipeline(settings: Settings | None = None) -> str:
    """Run extract, transform, and load steps sequentially."""
    configure_logging()
    settings = settings or Settings()
    raw_file = extract_weather_data(settings)
    clean_file = transform_weather_data(raw_file, settings)
    return load_weather_data(clean_file, settings)
