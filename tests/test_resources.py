"""Unit tests for shared Dagster resources."""

from src.core.config import BASE_DATE
from src.core.resources import ExecutionDateResource


def test_execution_date_resource_defaults_to_base_date():
    assert ExecutionDateResource().date == BASE_DATE


def test_execution_date_resource_is_overridable():
    assert ExecutionDateResource(date="2026-05-01").date == "2026-05-01"
