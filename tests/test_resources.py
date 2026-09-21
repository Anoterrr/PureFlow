"""Unit tests for shared Dagster resources and the partition-date resolution."""

import os
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest
from src.core import config
from src.core.resources import ExecutionDateResource


@pytest.fixture
def clean_date_env(monkeypatch):
    """BASE_DATE leaking in from a developer's .env would silently pass these."""
    monkeypatch.delenv("BASE_DATE", raising=False)
    return monkeypatch


def test_resolved_date_defaults_to_today(clean_date_env):
    assert ExecutionDateResource().resolved_date == config.today()


def test_resolved_date_is_overridable_per_run(clean_date_env):
    resource = ExecutionDateResource(date="2026-05-01")
    assert resource.date == "2026-05-01"
    assert resource.resolved_date == "2026-05-01"


def test_resolved_date_honours_base_date(clean_date_env):
    """BASE_DATE used to move the generators without moving the pipeline."""
    clean_date_env.setenv("BASE_DATE", "2026-02-02")
    assert ExecutionDateResource().resolved_date == "2026-02-02"


def test_run_config_wins_over_base_date(clean_date_env):
    clean_date_env.setenv("BASE_DATE", "2026-02-02")
    assert ExecutionDateResource(date="2026-03-03").resolved_date == "2026-03-03"


def test_today_uses_the_configured_timezone(monkeypatch):
    """The whole point: host and container must agree on which day it is.

    Pinned to a real instant where UTC and Sao Paulo (-03:00) fall on different
    calendar days, which is exactly the window that produced mismatched dt=
    partitions between the dashboard on the host and the pipeline in Docker.
    """
    instant = datetime(2026, 9, 21, 1, 30, tzinfo=ZoneInfo("UTC"))

    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return instant.astimezone(tz) if tz else instant.replace(tzinfo=None)

    monkeypatch.setattr(config, "datetime", FrozenDatetime)

    monkeypatch.setattr(config, "PIPELINE_TIMEZONE", ZoneInfo("UTC"))
    assert config.today() == "2026-09-21"

    monkeypatch.setattr(config, "PIPELINE_TIMEZONE", ZoneInfo("America/Sao_Paulo"))
    assert config.today() == "2026-09-20"


def test_base_date_is_not_frozen_at_import(clean_date_env):
    """It was a module constant, so a process past midnight kept yesterday's date."""
    clean_date_env.setenv("BASE_DATE", "2026-01-01")
    assert config.get_base_date() == "2026-01-01"
    os.environ["BASE_DATE"] = "2026-01-02"
    assert config.get_base_date() == "2026-01-02"
