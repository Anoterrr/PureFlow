"""Modern Orchestration for PureFlow-Arch using Dagster (Asset-Based).

Bronze/Silver/Gold transformations all live as dbt models (dbt/models/) — this
module wires them into Dagster, plus the Great Expectations quality gates that
wrap them: a pre-flight check on raw landing files (source circuit breaker,
mirrors the old inline factory.py behavior) and a post-write @asset_check per
model (target circuit breaker), both reusing PureFlowEngine.quarantine_data()
and validation.gx_validator.validate_data() unchanged.
"""

import os
from pathlib import Path

from dagster import (
    AssetCheckResult,
    AssetKey,
    AssetSelection,
    Definitions,
    MetadataValue,
    asset,
    asset_check,
    define_asset_job,
    load_assets_from_current_module,
)
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets

from core.engine import PureFlowEngine
from core.quality import GreatExpectationsResource, reinforce_global_s3_config
from core.resources import ExecutionDateResource

# Import data generators and corruptors
from utils.generate_clean_data import generate_clean_big_data
from utils.generate_corrupt_data import corrupt_bronze_layer, corrupt_landing_zone
from utils.generate_dirty_data import generate_dirty_big_data
from validation.gx_validator import validate_data

# --- 1. dbt Configuration with Lineage Mapping ---
DBT_PROJECT_DIR = Path(__file__).joinpath("..", "..", "dbt").resolve()
dbt_resource = DbtCliResource(project_dir=os.fspath(DBT_PROJECT_DIR))


# This translator connects dbt sources/models to Dagster asset keys and groups
class PureFlowDbtTranslator(DagsterDbtTranslator):
    """Custom translator for PureFlow dbt assets to map sources and groups."""

    def get_asset_key(self, dbt_resource_props):
        """Flat, schema-agnostic AssetKeys for both sources and models.

        Sources map onto the landing-zone gate assets below by name; models get
        a plain AssetKey(name) (instead of dagster-dbt's default schema-prefixed
        key) so the @asset_check definitions below — which target AssetKey(name)
        directly — actually attach to the right asset.
        """
        resource_type = dbt_resource_props.get("resource_type")
        if resource_type in ("source", "model"):
            return AssetKey(dbt_resource_props["name"])
        return super().get_asset_key(dbt_resource_props)

    def get_group_name(self, dbt_resource_props):
        """Groups each model by its medallion layer (inferred from the model's folder/FQN)."""
        if dbt_resource_props.get("resource_type") == "model":
            for layer in ("bronze", "silver", "gold"):
                if layer in dbt_resource_props.get("fqn", []):
                    return layer
        return super().get_group_name(dbt_resource_props)


@dbt_assets(
    manifest=DBT_PROJECT_DIR.joinpath("target", "manifest.json"),
    dagster_dbt_translator=PureFlowDbtTranslator(),
)
def pureflow_dbt_assets(
    context, dbt: DbtCliResource, execution_date_resource: ExecutionDateResource
):
    """Bronze/Silver/Gold — every dbt model, materialized as Delta via the delta_rw plugin."""
    execution_date = execution_date_resource.resolved_date
    # The landing_read plugin (dbt_plugins/landing_source.py) can't see dbt's --vars
    # (it only Jinja-renders .sql, not sources.yml meta), so it reads this env var
    # directly — set it here to keep both resolution paths in agreement.
    os.environ["EXECUTION_DATE"] = execution_date
    yield from dbt.cli(
        ["run", "--vars", f"execution_date: {execution_date}"],
        context=context,
    ).stream()


# --- 2. Quality Gates (Great Expectations) ---
# Expectations moved here from the retired src/pipelines/{sales,customers}.py —
# unchanged in content, just relocated next to where they're now consumed.

SALES_LANDING_EXPECTATIONS = [
    {
        "expectation": "ExpectTableRowCountToBeBetween",
        "kwargs": {"min_value": 1, "max_value": 2000000},
    },
    {"expectation": "ExpectColumnValuesToNotBeNull", "kwargs": {"column": "customer_id"}},
]
CUSTOMERS_LANDING_EXPECTATIONS = [
    {"expectation": "ExpectColumnValuesToNotBeNull", "kwargs": {"column": "id"}},
]
STG_SALES_BRONZE_TARGET_EXPECTATIONS = [
    {"expectation": "ExpectColumnValuesToNotBeNull", "kwargs": {"column": "id"}},
]
STG_CUSTOMERS_BRONZE_TARGET_EXPECTATIONS = [
    {
        "expectation": "ExpectColumnValuesToMatchRegex",
        "kwargs": {"column": "email", "regex": r"[^@]+@[^@]+\.[^@]+"},
    },
]
SALES_SILVER_TARGET_EXPECTATIONS = [
    {
        "expectation": "ExpectColumnValuesToBeBetween",
        "kwargs": {"column": "price", "min_value": 0, "max_value": 10000},
    },
]
CUSTOMERS_SILVER_TARGET_EXPECTATIONS = [
    {"expectation": "ExpectColumnValuesToNotBeNull", "kwargs": {"column": "email"}},
]
SALES_SUMMARY_TARGET_EXPECTATIONS = [
    {
        "expectation": "ExpectColumnValuesToBeBetween",
        "kwargs": {"column": "avg_ticket", "min_value": 0, "max_value": 100000},
    },
    {"expectation": "ExpectColumnValuesToNotBeNull", "kwargs": {"column": "total_revenue"}},
]


def _run_source_gate(context, gx_resource, execution_date, path, data_format, expectations, name):
    """Pre-flight circuit breaker: validates raw input *before* a dbt model reads it. Raises on failure."""
    engine = PureFlowEngine(execution_date=execution_date)
    success, report_url, error_msg = validate_data(
        path=path,
        expectations=expectations,
        data_format=data_format,
        suite_name=f"check_{name}_source",
        context=gx_resource.get_context(),
    )
    if not success:
        q_path = engine.quarantine_data(
            path, reason=f"source_fail_{name}", source_format=data_format
        )
        context.log.error(
            f"❌ [Gatekeeper] {name} source validation FAILED. Quarantined to {q_path}. Report: {report_url}"
        )
        raise ValueError(f"Circuit Breaker: {name} source validation failed. Report: {report_url}")
    context.log.info(f"✅ [Gatekeeper] {name} source validation passed. Report: {report_url}")


def _run_target_check(gx_resource, execution_date, path, data_format, expectations, name):
    """Post-write circuit breaker: validates a dbt model's output, quarantining it on failure."""
    engine = PureFlowEngine(execution_date=execution_date)
    success, report_url, error_msg = validate_data(
        path=path,
        expectations=expectations,
        data_format=data_format,
        suite_name=f"check_{name}_target",
        context=gx_resource.get_context(),
    )
    metadata = {"report_url": MetadataValue.url(report_url)}
    if not success:
        q_path = engine.quarantine_data(
            path, reason=f"target_fail_{name}", source_format=data_format
        )
        metadata["quarantine_path"] = MetadataValue.path(q_path)
        metadata["error"] = MetadataValue.text(error_msg or "")
    return AssetCheckResult(passed=success, metadata=metadata)


@asset(
    name="sales_landing",
    group_name="landing",
    compute_kind="python",
    # Only actually enforced when a job selects both (quality_test_job) — jobs
    # that don't include inject_corrupt_landing (e.g. pureflow_pipeline_job)
    # simply treat it as already-satisfied, per normal Dagster subset semantics.
    # Without this edge, quality_test_job races: this gate can read landing
    # before the corruptor finishes writing it.
    deps=[AssetKey("inject_corrupt_landing")],
)
def sales_landing(
    context, gx_resource: GreatExpectationsResource, execution_date_resource: ExecutionDateResource
):
    """Pre-flight gate on the raw landing CSV — dbt source `landing.sales_landing`, read by stg_sales_bronze."""
    execution_date = execution_date_resource.resolved_date
    _run_source_gate(
        context,
        gx_resource,
        execution_date,
        path=f"s3://landing-zone/sales_erp/dt={execution_date}/sales.csv",
        data_format="csv",
        expectations=SALES_LANDING_EXPECTATIONS,
        name="sales_landing",
    )


@asset(
    name="customers_landing",
    group_name="landing",
    compute_kind="python",
    deps=[AssetKey("inject_corrupt_landing")],
)
def customers_landing(
    context, gx_resource: GreatExpectationsResource, execution_date_resource: ExecutionDateResource
):
    """Pre-flight gate on the raw landing JSON — dbt source `landing.customers_landing`, read by stg_customers_bronze."""
    execution_date = execution_date_resource.resolved_date
    _run_source_gate(
        context,
        gx_resource,
        execution_date,
        path=f"s3://landing-zone/customers_crm/dt={execution_date}/customers.json",
        data_format="json",
        expectations=CUSTOMERS_LANDING_EXPECTATIONS,
        name="customers_landing",
    )


@asset_check(
    asset=AssetKey("stg_sales_bronze"), name="check_stg_sales_bronze_target", blocking=True
)
def check_stg_sales_bronze_target(
    gx_resource: GreatExpectationsResource, execution_date_resource: ExecutionDateResource
):
    execution_date = execution_date_resource.resolved_date
    return _run_target_check(
        gx_resource,
        execution_date,
        path=f"s3://bronze/sales_erp/dt={execution_date}/stg_sales_bronze",
        data_format="delta",
        expectations=STG_SALES_BRONZE_TARGET_EXPECTATIONS,
        name="stg_sales_bronze",
    )


@asset_check(
    asset=AssetKey("stg_customers_bronze"), name="check_stg_customers_bronze_target", blocking=True
)
def check_stg_customers_bronze_target(
    gx_resource: GreatExpectationsResource, execution_date_resource: ExecutionDateResource
):
    execution_date = execution_date_resource.resolved_date
    return _run_target_check(
        gx_resource,
        execution_date,
        path=f"s3://bronze/customers_crm/dt={execution_date}/stg_customers_bronze",
        data_format="delta",
        expectations=STG_CUSTOMERS_BRONZE_TARGET_EXPECTATIONS,
        name="stg_customers_bronze",
    )


@asset_check(asset=AssetKey("sales_silver"), name="check_sales_silver_target", blocking=True)
def check_sales_silver_target(
    gx_resource: GreatExpectationsResource, execution_date_resource: ExecutionDateResource
):
    execution_date = execution_date_resource.resolved_date
    return _run_target_check(
        gx_resource,
        execution_date,
        path=f"s3://silver/sales/dt={execution_date}",
        data_format="delta",
        expectations=SALES_SILVER_TARGET_EXPECTATIONS,
        name="sales_silver",
    )


@asset_check(
    asset=AssetKey("customers_silver"), name="check_customers_silver_target", blocking=True
)
def check_customers_silver_target(
    gx_resource: GreatExpectationsResource, execution_date_resource: ExecutionDateResource
):
    execution_date = execution_date_resource.resolved_date
    return _run_target_check(
        gx_resource,
        execution_date,
        path=f"s3://silver/customers/dt={execution_date}",
        data_format="delta",
        expectations=CUSTOMERS_SILVER_TARGET_EXPECTATIONS,
        name="customers_silver",
    )


@asset_check(asset=AssetKey("sales_summary"), name="check_sales_summary")
def check_sales_summary(
    gx_resource: GreatExpectationsResource, execution_date_resource: ExecutionDateResource
):
    """Gold quality gate — non-blocking since nothing in the DAG consumes Gold downstream."""
    execution_date = execution_date_resource.resolved_date
    return _run_target_check(
        gx_resource,
        execution_date,
        path=f"s3://gold/sales_summary/dt={execution_date}",
        data_format="delta",
        expectations=SALES_SUMMARY_TARGET_EXPECTATIONS,
        name="sales_summary",
    )


# --- 3. Data State Management Assets (Separated from main pipeline) ---


@asset(group_name="data_generators", compute_kind="python")
def generate_clean_data(context, execution_date_resource: ExecutionDateResource):
    """Generates CLEAN synthetic data in the Landing Zone."""
    execution_date = execution_date_resource.resolved_date
    generate_clean_big_data(execution_date=execution_date)
    context.log.info(f"✅ CLEAN data generated successfully for {execution_date}.")


@asset(group_name="data_generators", compute_kind="python")
def generate_dirty_data(context, execution_date_resource: ExecutionDateResource):
    """Generates DIRTY synthetic data to test Quality Gates."""
    execution_date = execution_date_resource.resolved_date
    generate_dirty_big_data(execution_date=execution_date)
    context.log.warning(f"⚠️ DIRTY data generated for {execution_date}.")


@asset(group_name="test_quality", compute_kind="python")
def inject_corrupt_landing(context, execution_date_resource: ExecutionDateResource):
    """Intentional data corruption at Landing Zone."""
    execution_date = execution_date_resource.resolved_date
    corrupt_landing_zone(execution_date=execution_date)
    context.log.warning(f"🧨 Landing Zone data corrupted for {execution_date}.")


@asset(group_name="test_quality", compute_kind="python")
def inject_corrupt_bronze(context, execution_date_resource: ExecutionDateResource):
    """Intentional data corruption at Bronze Layer."""
    execution_date = execution_date_resource.resolved_date
    corrupt_bronze_layer(execution_date=execution_date)
    context.log.warning(f"🧨 Bronze Layer data corrupted for {execution_date}.")


# --- 4. Jobs & Definitions ---

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
        AssetSelection.groups("test_quality")
        | AssetSelection.groups("landing", "bronze", "silver", "gold")
    ),
)

# Set GLOBAL-scope DuckDB S3 defaults once, explicitly, before any asset materializes
# (previously ran as a hidden side effect of importing core.quality — see that module).
reinforce_global_s3_config()

# All assets are defined directly in this module now — bronze/silver/gold live as
# dbt models (dbt/models/), discovered via the manifest passed to @dbt_assets above.
# load_assets_from_current_module() only picks up AssetsDefinition/SourceAsset —
# @asset_check produces a distinct AssetChecksDefinition, so those are collected
# separately and passed to Definitions(asset_checks=...) below.
all_assets = list(load_assets_from_current_module())
all_asset_checks = [
    check_stg_sales_bronze_target,
    check_stg_customers_bronze_target,
    check_sales_silver_target,
    check_customers_silver_target,
    check_sales_summary,
]

# Resource for Great Expectations
gx_resource = GreatExpectationsResource(ge_root_dir=os.fspath(Path(__file__).parent.parent / "gx"))

defs = Definitions(
    assets=all_assets,
    asset_checks=all_asset_checks,
    resources={
        "dbt": dbt_resource,
        "gx_resource": gx_resource,
        "execution_date_resource": ExecutionDateResource(),
    },
    jobs=[pureflow_pipeline_job, data_generation_job, quality_test_job],
)
