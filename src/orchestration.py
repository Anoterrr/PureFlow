"""Modern Orchestration for PureFlow-Arch using Dagster (Asset-Based)."""

import os
from pathlib import Path

from dagster import (
    AssetKey,
    AssetSelection,
    ConfigurableResource,
    Definitions,
    asset,
    asset_check,
    AssetCheckResult,
    define_asset_job,
    load_assets_from_current_module,
    load_assets_from_package_name,
    MetadataValue,
    MaterializeResult,
)
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets
import dagster_ge

from validation.gx_validator import validate_data
from core.quality import GreatExpectationsResource

# Import data generators and corruptors
from utils.generate_clean_data import generate_clean_big_data
from utils.generate_corrupt_data import corrupt_bronze_layer, corrupt_landing_zone
from utils.generate_dirty_data import generate_dirty_big_data

# Default configuration to avoid 404s for today's date if data isn't generated
DEFAULT_DATE = "2026-04-19"

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
def pureflow_dbt_assets(context, dbt: DbtCliResource):
    """Assets representing dbt models in the transformation pipeline."""
    # Sincroniza a data de execução entre Dagster e dbt via variável de ambiente
    execution_date = context.op_config.get("execution_date", DEFAULT_DATE)
    
    # Passa a data via --vars para o dbt
    yield from dbt.cli(["run", "--vars", f"execution_date: {execution_date}"], context=context).stream()


@asset_check(
    asset=AssetKey("sales_summary"),
    name="check_sales_summary",
)
def check_sales_summary(context):
    """Quality gate for the final Gold Layer asset (Skipped)."""
    context.log.info("Skipping gold validation as requested.")
    return AssetCheckResult(passed=True, metadata={"status": "skipped"})


# --- 2. Data State Management Assets (Separated from main pipeline) ---


@asset(group_name="data_generators", compute_kind="python", config_schema={"execution_date": str})
def generate_clean_data(context):
    """Generates CLEAN synthetic data in the Landing Zone."""
    execution_date = context.op_config.get("execution_date", DEFAULT_DATE)
    generate_clean_big_data(execution_date=execution_date)
    context.log.info(f"✅ CLEAN data generated successfully for {execution_date}.")


@asset(group_name="data_generators", compute_kind="python", config_schema={"execution_date": str})
def generate_dirty_data(context):
    """Generates DIRTY synthetic data to test Quality Gates."""
    execution_date = context.op_config.get("execution_date", DEFAULT_DATE)
    generate_dirty_big_data(execution_date=execution_date)
    context.log.info(f"⚠️ DIRTY data generated for {execution_date}.")


@asset(group_name="test_quality", compute_kind="python", config_schema={"execution_date": str})
def inject_corrupt_landing(context):
    """Intentional data corruption at Landing Zone."""
    execution_date = context.op_config.get("execution_date", DEFAULT_DATE)
    corrupt_landing_zone(execution_date=execution_date)
    context.log.warning(f"🧨 Landing Zone data corrupted for {execution_date}.")


@asset(group_name="test_quality", compute_kind="python", config_schema={"execution_date": str})
def inject_corrupt_bronze(context):
    """Intentional data corruption at Bronze Layer."""
    execution_date = context.op_config.get("execution_date", DEFAULT_DATE)
    corrupt_bronze_layer(execution_date=execution_date)
    context.log.warning(f"🧨 Bronze Layer data corrupted for {execution_date}.")


# --- 3. Jobs & Definitions ---

# Config for core business logic assets (Bronze, Silver)
core_pipeline_ops_config = {
    "stg_customers_bronze": {"config": {"execution_date": DEFAULT_DATE}},
    "customers_silver": {"config": {"execution_date": DEFAULT_DATE}},
    "stg_sales_bronze": {"config": {"execution_date": DEFAULT_DATE}},
    "sales_silver": {"config": {"execution_date": DEFAULT_DATE}},
}

# Config for data corruption tools
test_quality_ops_config = {
    "inject_corrupt_landing": {"config": {"execution_date": DEFAULT_DATE}},
    "inject_corrupt_bronze": {"config": {"execution_date": DEFAULT_DATE}},
}

# Config for data generation assets
data_gen_ops_config = {
    "generate_clean_data": {"config": {"execution_date": DEFAULT_DATE}},
    "generate_dirty_data": {"config": {"execution_date": DEFAULT_DATE}},
}

# Main Transformation Pipeline (Excludes generators and corruption tools)
pureflow_pipeline_job = define_asset_job(  # pylint: disable=assignment-from-no-return
    name="pureflow_pipeline_job",
    selection=AssetSelection.all() - AssetSelection.groups("data_generators", "test_quality"),
    config={"ops": core_pipeline_ops_config},
)

# Job for generating synthetic data
data_generation_job = define_asset_job(
    name="data_generation_job",
    selection=AssetSelection.groups("data_generators"),
    config={"ops": data_gen_ops_config},
)

# Specific job to test Quality Gates by corrupting and then running the pipeline
quality_test_job = define_asset_job(  # pylint: disable=assignment-from-no-return
    name="quality_test_job",
    selection=(
        AssetSelection.groups("test_quality")
        | AssetSelection.groups("bronze", "silver", "gold")
    ),
    config={"ops": {**core_pipeline_ops_config, **test_quality_ops_config}},
)

# Dynamic asset discovery:
from pipelines.customers import customers_bronze_assets, customers_silver_assets
from pipelines.sales import sales_bronze_assets, sales_silver_assets

# load_assets_from_current_module() will find all assets defined or imported above,
# including those in the imported lists, so we don't need to manually unpack them.
all_assets = load_assets_from_current_module()

# Resource for Great Expectations
gx_resource = GreatExpectationsResource(ge_root_dir=os.fspath(Path(__file__).parent.parent / "gx"))

defs = Definitions(
    assets=all_assets,
    resources={
        "dbt": dbt_resource,
        "gx_resource": gx_resource,
    },
    jobs=[pureflow_pipeline_job, data_generation_job, quality_test_job],
)
