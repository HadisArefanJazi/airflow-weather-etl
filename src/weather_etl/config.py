"""Configuration for the Open-Meteo weather ETL pipeline."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_DIR = PROJECT_ROOT / "data"


@dataclass(frozen=True)
class Settings:
    """Runtime settings for the weather ETL pipeline."""

    city: str = os.getenv("WEATHER_CITY", "New York")
    latitude: float = float(os.getenv("WEATHER_LATITUDE", "40.7128"))
    longitude: float = float(os.getenv("WEATHER_LONGITUDE", "-74.0060"))
    timezone: str = os.getenv("WEATHER_TIMEZONE", "America/New_York")
    api_url: str = os.getenv("OPEN_METEO_API_URL", "https://api.open-meteo.com/v1/forecast")
    request_timeout_seconds: int = int(os.getenv("WEATHER_REQUEST_TIMEOUT_SECONDS", "30"))
    data_dir: Path = Path(os.getenv("WEATHER_DATA_DIR", str(DEFAULT_DATA_DIR)))

    @property
    def raw_dir(self) -> Path:
        """Directory where raw API JSON payloads are stored."""
        return self.data_dir / "raw"

    @property
    def processed_dir(self) -> Path:
        """Directory where one-row processed CSV files are stored."""
        return self.data_dir / "processed"

    @property
    def history_file(self) -> Path:
        """Historical CSV file containing loaded weather records."""
        return self.data_dir / "weather_history.csv"

    def ensure_directories(self) -> None:
        """Create data directories needed by the pipeline."""
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)

    def run_id(self) -> str:
        """Return a timestamp-based run identifier in the configured timezone."""
        return datetime.now(ZoneInfo(self.timezone)).strftime("%Y%m%d_%H%M%S")


def slugify(value: str) -> str:
    """Convert a label into safe lowercase filename text."""
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
