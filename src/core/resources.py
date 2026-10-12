"""Shared Dagster resources for PureFlow."""

from dagster import ConfigurableResource

from core.config import get_base_date


class ExecutionDateResource(ConfigurableResource):
    """Partition date for a run: `date` from run config, then BASE_DATE, then today.

    Resolved per step, so pin `date` for backfills or runs that may cross midnight.
    """

    date: str = ""

    @property
    def resolved_date(self) -> str:
        return self.date or get_base_date()
