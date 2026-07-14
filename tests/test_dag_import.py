"""Tests for DAG import safety."""

from __future__ import annotations

import importlib


def test_dag_module_imports_without_running_pipeline():
    module = importlib.import_module("dags.weather_etl_dag")

    assert module.DAG_ID == "daily_weather_etl"
