"""Unit tests for shared Dagster resources."""

from datetime import datetime

from src.core.resources import ExecutionDateResource


def test_execution_date_resource_defaults_to_today():
    assert ExecutionDateResource().resolved_date == datetime.now().strftime("%Y-%m-%d")


def test_execution_date_resource_is_overridable():
    resource = ExecutionDateResource(date="2026-05-01")
    assert resource.date == "2026-05-01"
    assert resource.resolved_date == "2026-05-01"
