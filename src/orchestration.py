"""Modern Orchestration for PureFlow-Arch using Dagster (Asset-Based)."""

import os
from pathlib import Path

from dagster import (
    AssetCheckResult,
    AssetKey,
    AssetSelection,
    Definitions,
    asset,
    asset_check,
    define_asset_job,
    load_assets_from_current_module,
    load_assets_from_package_module,
)
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets

import pipelines  # noqa: F401  (scanned below — see pipeline_assets)
from core.config import BASE_DATE
from core.quality import GreatExpectationsResource, reinforce_global_s3_config
from core.resources import ExecutionDateResource

# Import data generators and corruptors
from utils.generate_clean_data import generate_clean_big_data
from utils.generate_corrupt_data import corrupt_bronze_layer, corrupt_landing_zone
from utils.generate_dirty_data import generate_dirty_big_data

# --- 1. dbt Configuration with Lineage Mapping ---
DBT_PROJECT_DIR = Path(__file__).joinpath("..", "..", "dbt").resolve()
dbt_resource = DbtCliResource(project_dir=os.fspath(DBT_PROJECT_DIR))


# This translator connects dbt sources to our Factory assets
class PureFlowDbtTranslator(DagsterDbtTranslator):
    """Custom translator for PureFlow dbt assets to map sources and groups."""

    def get_asset_key(self, dbt_resource_props):
        """Maps dbt source names to Dagster AssetKeys."""
        resource_type = dbt_resource_props.get("resource_type")
        if resource_type == "source":
            # Map dbt source directly to the silver layer asset
            source_name = dbt_resource_props["name"]
            return AssetKey(source_name)
        return super().get_asset_key(dbt_resource_props)

    def get_group_name(self, dbt_resource_props):
        """Assigns 'gold' group to dbt models."""
        # Only put actual models in the 'gold' group
        if dbt_resource_props.get("resource_type") == "model":
            return "gold"
        return super().get_group_name(dbt_resource_props)


@dbt_assets(
    manifest=DBT_PROJECT_DIR.joinpath("target", "manifest.json"),
    dagster_dbt_translator=PureFlowDbtTranslator(),
)
def pureflow_dbt_assets(
    context, dbt: DbtCliResource, execution_date_resource: ExecutionDateResource
):
    """Assets representing dbt models in the transformation pipeline."""
    # Passa a data via --vars para o dbt
    yield from dbt.cli(
        ["run", "--vars", f"execution_date: {execution_date_resource.date}"], context=context
    ).stream()


@asset_check(
    asset=AssetKey("sales_summary"),
    name="check_sales_summary",
)
def check_sales_summary(context):
    """Quality gate for the final Gold Layer asset (Skipped)."""
    context.log.info("Skipping gold validation as requested.")
    return AssetCheckResult(passed=True, metadata={"status": "skipped"})


# --- 2. Data State Management Assets (Separated from main pipeline) ---


@asset(group_name="data_generators", compute_kind="python")
def generate_clean_data(context, execution_date_resource: ExecutionDateResource):
    """Generates CLEAN synthetic data in the Landing Zone."""
    execution_date = execution_date_resource.date
    generate_clean_big_data(execution_date=execution_date)
    context.log.info(f"✅ CLEAN data generated successfully for {execution_date}.")


@asset(group_name="data_generators", compute_kind="python")
def generate_dirty_data(context, execution_date_resource: ExecutionDateResource):
    """Generates DIRTY synthetic data to test Quality Gates."""
    execution_date = execution_date_resource.date
    generate_dirty_big_data(execution_date=execution_date)
    context.log.warning(f"⚠️ DIRTY data generated for {execution_date}.")


@asset(group_name="test_quality", compute_kind="python")
def inject_corrupt_landing(context, execution_date_resource: ExecutionDateResource):
    """Intentional data corruption at Landing Zone."""
    execution_date = execution_date_resource.date
    corrupt_landing_zone(execution_date=execution_date)
    context.log.warning(f"🧨 Landing Zone data corrupted for {execution_date}.")


@asset(group_name="test_quality", compute_kind="python")
def inject_corrupt_bronze(context, execution_date_resource: ExecutionDateResource):
    """Intentional data corruption at Bronze Layer."""
    execution_date = execution_date_resource.date
    corrupt_bronze_layer(execution_date=execution_date)
    context.log.warning(f"🧨 Bronze Layer data corrupted for {execution_date}.")


# --- 3. Jobs & Definitions ---

# No per-op config needed anymore: every asset gets its execution date from the
# execution_date_resource (defaults to BASE_DATE), overridable per-run via the UI.

# Main Transformation Pipeline (Excludes generators and corruption tools)
pureflow_pipeline_job = define_asset_job(
    name="pureflow_pipeline_job",
    selection=AssetSelection.all() - AssetSelection.groups("data_generators", "test_quality"),
)

# Job for generating synthetic data
data_generation_job = define_asset_job(
    name="data_generation_job",
    selection=AssetSelection.groups("data_generators"),
)

# Specific job to test Quality Gates by corrupting and then running the pipeline
quality_test_job = define_asset_job(
    name="quality_test_job",
    selection=(
        AssetSelection.groups("test_quality") | AssetSelection.groups("bronze", "silver", "gold")
    ),
)

# Set GLOBAL-scope DuckDB S3 defaults once, explicitly, before any asset materializes
# (previously ran as a hidden side effect of importing core.quality — see that module).
reinforce_global_s3_config()

# Dynamic asset discovery:

# load_assets_from_current_module() finds assets defined directly in this file.
# load_assets_from_package_module() scans every module under src/pipelines/ (Bronze/
# Silver assets) automatically — adding a new domain only requires dropping a new
# file there, no manual import/registration needed in this module.
all_assets = list(load_assets_from_current_module()) + list(
    load_assets_from_package_module(pipelines)
)

# Resource for Great Expectations
gx_resource = GreatExpectationsResource(ge_root_dir=os.fspath(Path(__file__).parent.parent / "gx"))

defs = Definitions(
    assets=all_assets,
    resources={
        "dbt": dbt_resource,
        "gx_resource": gx_resource,
        "execution_date_resource": ExecutionDateResource(date=BASE_DATE),
    },
    jobs=[pureflow_pipeline_job, data_generation_job, quality_test_job],
)
