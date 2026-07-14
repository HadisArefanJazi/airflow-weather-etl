"""Load processed weather records into the historical CSV dataset."""

from __future__ import annotations

import csv
import logging
from pathlib import Path

from weather_etl.config import Settings
from weather_etl.transform import CSV_FIELDS

LOGGER = logging.getLogger(__name__)


class WeatherLoadError(ValueError):
    """Raised when a processed weather record cannot be loaded."""


def read_processed_records(clean_file: str | Path) -> list[dict[str, str]]:
    """Read processed weather records from a CSV file."""
    clean_path = Path(clean_file)
    if not clean_path.exists():
        raise WeatherLoadError(f"Processed CSV file not found: {clean_path}")

    with clean_path.open("r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        rows = list(reader)

    if not rows:
        raise WeatherLoadError("Processed CSV file is empty.")

    missing = [field for field in CSV_FIELDS if field not in (reader.fieldnames or [])]
    if missing:
        raise WeatherLoadError(f"Processed CSV file is missing columns: {', '.join(missing)}")

    return rows


def read_history_records(history_file: Path) -> list[dict[str, str]]:
    """Read existing history rows, returning an empty list when history is absent."""
    if not history_file.exists():
        return []

    with history_file.open("r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        rows = list(reader)

    if reader.fieldnames and any(field not in reader.fieldnames for field in CSV_FIELDS):
        raise WeatherLoadError(f"History CSV has an unexpected schema: {history_file}")
    return rows


def load_weather_data(clean_file: str | Path, settings: Settings | None = None) -> str:
    """Append processed records to history and skip exact duplicate run records."""
    settings = settings or Settings()
    settings.ensure_directories()

    new_records = read_processed_records(clean_file)
    existing_records = read_history_records(settings.history_file)
    existing_runs = {row.get("run") for row in existing_records}
    records_to_add = [row for row in new_records if row.get("run") not in existing_runs]

    history_exists = settings.history_file.exists()
    mode = "a" if history_exists else "w"
    with settings.history_file.open(mode, newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=CSV_FIELDS)
        if not history_exists:
            writer.writeheader()
        for record in records_to_add:
            writer.writerow({field: record[field] for field in CSV_FIELDS})

    if records_to_add:
        LOGGER.info("Loaded %s weather record(s) into %s.", len(records_to_add), settings.history_file)
    else:
        LOGGER.info("No new weather records loaded; duplicate run id already present.")

    return str(settings.history_file)
