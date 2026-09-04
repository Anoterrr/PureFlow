"""Shared Dagster resources for PureFlow."""

from datetime import datetime

from dagster import ConfigurableResource


class ExecutionDateResource(ConfigurableResource):
    """Supplies the pipeline execution date to assets.

    No fixed date is baked in: `date` defaults to empty, and `resolved_date`
    falls back to "today" computed fresh at the moment each asset runs. A
    specific run can still pin a date via the resource's own run config
    (Launchpad -> resources -> execution_date_resource -> date), e.g. for a
    backfill or to reproduce a past run.
    """

    date: str = ""

    @property
    def resolved_date(self) -> str:
        """The date to use for this run: the configured override, or today."""
        return self.date or datetime.now().strftime("%Y-%m-%d")
