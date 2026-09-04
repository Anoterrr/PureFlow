"""Shared Dagster resources for PureFlow-Arch."""

from dagster import ConfigurableResource

from core.config import BASE_DATE


class ExecutionDateResource(ConfigurableResource):
    """Supplies the pipeline execution date to assets, defaulting to BASE_DATE.

    Replaces per-asset `config_schema={"execution_date": str}` + ops config: every
    asset gets a working default for free, and a specific run can still override it
    via the resource's own run config (Launchpad -> resources -> execution_date_resource).
    """

    date: str = BASE_DATE
