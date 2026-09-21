"""Shared Dagster resources for PureFlow."""

from dagster import ConfigurableResource

from core.config import get_base_date


class ExecutionDateResource(ConfigurableResource):
    """Supplies the pipeline execution date to assets.

    Resolution order: this resource's `date` (set per run in the Launchpad
    under resources -> execution_date_resource -> date), then the BASE_DATE
    environment variable, then today in core.config.PIPELINE_TIMEZONE.

    BASE_DATE used to be ignored here while the generators and the engine did
    honour it, so setting it to reproduce a past run silently moved the data
    without moving the pipeline. One knob now drives both.

    Known limitation: the date is resolved per step, not per run. A run that
    crosses midnight can materialize one partition and validate another. Pin
    `date` in the run config for anything where that matters (a backfill, or a
    demo started close to midnight).
    """

    date: str = ""

    @property
    def resolved_date(self) -> str:
        """The date to use for this run."""
        return self.date or get_base_date()
