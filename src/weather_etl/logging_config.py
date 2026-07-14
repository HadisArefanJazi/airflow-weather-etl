"""Reusable logging setup for the weather ETL project."""

from __future__ import annotations

import logging
import os


def configure_logging(level: str | None = None) -> None:
    """Configure application logging once for local and Airflow runs."""
    logging.basicConfig(
        level=(level or os.getenv("LOG_LEVEL", "INFO")).upper(),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
