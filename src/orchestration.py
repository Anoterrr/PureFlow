"""Dagster definitions: dbt models per medallion layer plus the GX quality gates."""

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
)
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets

from core.config import get_s3_paths
from core.engine import PureFlowEngine
from core.gates import SOURCE_GATES, TARGET_GATES, SourceGate, TargetGate
from core.quality import GreatExpectationsResource, prime_s3_environment
from core.resources import ExecutionDateResource
from utils.generate_clean_data import generate_clean_big_data
from utils.generate_corrupt_data import corrupt_bronze_layer, corrupt_landing_zone
from utils.generate_dirty_data import generate_dirty_big_data
from validation.gx_validator import ValidationTechnicalError, validate_data

DBT_PROJECT_DIR = Path(__file__).joinpath("..", "..", "dbt").resolve()
dbt_resource = DbtCliResource(project_dir=os.fspath(DBT_PROJECT_DIR))


class PureFlowDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        # Plain names, so sources match the landing gate assets and checks can
        # target AssetKey(model_name).
        resource_type = dbt_resource_props.get("resource_type")
        if resource_type in ("source", "model"):
            return AssetKey(dbt_resource_props["name"])
        return super().get_asset_key(dbt_resource_props)

    def get_group_name(self, dbt_resource_props):
        if dbt_resource_props.get("resource_type") == "model":
            for layer in ("bronze", "silver", "gold"):
                if layer in dbt_resource_props.get("fqn", []):
                    return layer
        return super().get_group_name(dbt_resource_props)


# One dbt op per layer: a blocking check can only hold back assets built by a
# different op, so a single `dbt run` would write every layer before any check.
DBT_LAYERS = ("bronze", "silver", "gold")


def _build_dbt_layer_assets(layer: str):
    @dbt_assets(
        manifest=DBT_PROJECT_DIR.joinpath("target", "manifest.json"),
        select=f"fqn:*.{layer}.*",
        name=f"dbt_{layer}",
        dagster_dbt_translator=PureFlowDbtTranslator(),
    )
    def _dbt_layer(context, dbt: DbtCliResource, execution_date_resource: ExecutionDateResource):
        execution_date = execution_date_resource.resolved_date
        # Read by the landing_read plugin, which has no access to --vars.
        os.environ["EXECUTION_DATE"] = execution_date
        yield from dbt.cli(
            ["run", "--vars", f"execution_date: {execution_date}"],
            context=context,
        ).stream()

    return _dbt_layer


dbt_layer_assets = [_build_dbt_layer_assets(layer) for layer in DBT_LAYERS]


def _run_source_gate(context, gx_resource, execution_date, path, data_format, expectations, name):
    """Validates a landing file before Bronze reads it; quarantines and raises on failure."""
    engine = PureFlowEngine(execution_date=execution_date)
    success, report_url, _ = validate_data(
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
        where = (
            f"Quarantined to {q_path}." if q_path else "QUARANTINE ALSO FAILED, data left in place."
        )
        context.log.error(
            f"❌ [Gatekeeper] {name} source validation FAILED. {where} Report: {report_url}"
        )
        raise ValueError(f"Circuit Breaker: {name} source validation failed. Report: {report_url}")
    context.log.info(f"✅ [Gatekeeper] {name} source validation passed. Report: {report_url}")


def _run_target_check(gx_resource, execution_date, path, data_format, expectations, name):
    """Validates a dbt model's output; quarantines it on a data-quality failure."""
    engine = PureFlowEngine(execution_date=execution_date)
    try:
        success, report_url, error_msg = validate_data(
            path=path,
            expectations=expectations,
            data_format=data_format,
            suite_name=f"check_{name}_target",
            context=gx_resource.get_context(),
        )
    except ValidationTechnicalError as e:
        return AssetCheckResult(
            passed=False,
            metadata={
                "failure_type": MetadataValue.text("technical"),
                "error": MetadataValue.text(str(e)),
            },
        )

    metadata = {
        "report_url": MetadataValue.url(report_url),
        "failure_type": MetadataValue.text("none" if success else "data_quality"),
    }
    if not success:
        q_path = engine.quarantine_data(
            path, reason=f"target_fail_{name}", source_format=data_format
        )
        metadata["quarantine_path"] = (
            MetadataValue.path(q_path) if q_path else MetadataValue.text("QUARANTINE FAILED")
        )
        metadata["error"] = MetadataValue.text(error_msg or "")
    return AssetCheckResult(passed=success, metadata=metadata)


def _build_source_gate_asset(gate: SourceGate):
    @asset(
        name=gate.name,
        group_name="landing",
        compute_kind="python",
        # Orders the gate after the corruptor in quality_test_landing_job.
        deps=[AssetKey("inject_corrupt_landing")],
    )
    def _source_gate(
        context,
        gx_resource: GreatExpectationsResource,
        execution_date_resource: ExecutionDateResource,
    ):
        execution_date = execution_date_resource.resolved_date
        _run_source_gate(
            context,
            gx_resource,
            execution_date,
            path=get_s3_paths(execution_date)[gate.path_key],
            data_format=gate.data_format,
            expectations=gate.expectations,
            name=gate.name,
        )

    return _source_gate


def _build_target_gate_check(gate: TargetGate):
    @asset_check(
        asset=AssetKey(gate.name),
        name=gate.dagster_check_name,
        blocking=gate.blocking,
        additional_deps=[AssetKey(dep) for dep in gate.additional_deps],
    )
    def _target_check(
        gx_resource: GreatExpectationsResource,
        execution_date_resource: ExecutionDateResource,
    ):
        execution_date = execution_date_resource.resolved_date
        return _run_target_check(
            gx_resource,
            execution_date,
            path=get_s3_paths(execution_date)[gate.path_key],
            data_format=gate.data_format,
            expectations=gate.expectations,
            name=gate.name,
        )

    return _target_check


def _build_generator_asset(
    name: str, group: str, generate, message: str, level: str = "info", deps: tuple[str, ...] = ()
):
    @asset(
        name=name,
        group_name=group,
        compute_kind="python",
        deps=[AssetKey(d) for d in deps],
    )
    def _generator(context, execution_date_resource: ExecutionDateResource):
        execution_date = execution_date_resource.resolved_date
        generate(execution_date=execution_date)
        getattr(context.log, level)(message.format(date=execution_date))

    return _generator


GENERATORS = [
    (
        "generate_clean_data",
        "clean_data",
        generate_clean_big_data,
        "✅ CLEAN data generated for {date}.",
        "info",
        (),
    ),
    (
        "generate_dirty_data",
        "dirty_data",
        generate_dirty_big_data,
        "⚠️ DIRTY data generated for {date}.",
        "warning",
        (),
    ),
    (
        "inject_corrupt_landing",
        "test_quality",
        corrupt_landing_zone,
        "🧨 Landing Zone corrupted for {date}.",
        "warning",
        (),
    ),
    (
        "inject_corrupt_bronze",
        "test_quality",
        corrupt_bronze_layer,
        "🧨 Bronze Layer corrupted for {date}.",
        "warning",
        (),
    ),
]

source_gate_assets = [_build_source_gate_asset(g) for g in SOURCE_GATES]
target_gate_checks = [_build_target_gate_check(g) for g in TARGET_GATES]
checks_by_gate = dict(zip([g.name for g in TARGET_GATES], target_gate_checks, strict=True))
generator_assets = [_build_generator_asset(*spec) for spec in GENERATORS]

all_assets = [*dbt_layer_assets, *source_gate_assets, *generator_assets]
all_asset_checks = target_gate_checks

# Clean and dirty generators write the same paths, so each gets its own job.
GENERATOR_GROUPS = ("clean_data", "dirty_data", "test_quality")

pureflow_pipeline_job = define_asset_job(
    name="pureflow_pipeline_job",
    selection=AssetSelection.all() - AssetSelection.groups(*GENERATOR_GROUPS),
)

# Scenario A of scripts/run_experiments.py: same dbt ops, no gates.
baseline_no_gates_job = define_asset_job(
    name="baseline_no_gates_job",
    selection=AssetSelection.groups("bronze", "silver", "gold").without_checks(),
)

data_generation_job = define_asset_job(
    name="data_generation_job",
    selection=AssetSelection.groups("clean_data"),
)

dirty_data_generation_job = define_asset_job(
    name="dirty_data_generation_job",
    selection=AssetSelection.groups("dirty_data"),
)

quality_test_landing_job = define_asset_job(
    name="quality_test_landing_job",
    selection=(
        AssetSelection.assets(AssetKey("inject_corrupt_landing")) | AssetSelection.groups("landing")
    ),
)

# Run after a clean pipeline. dbt is left out on purpose: it would rebuild
# Bronze from clean landing and erase the corruption before the checks run.
quality_test_bronze_job = define_asset_job(
    name="quality_test_bronze_job",
    selection=(
        AssetSelection.assets(AssetKey("inject_corrupt_bronze"))
        | AssetSelection.checks(
            checks_by_gate["stg_sales_bronze"], checks_by_gate["stg_customers_bronze"]
        )
    ),
)

prime_s3_environment()

gx_resource = GreatExpectationsResource(ge_root_dir=os.fspath(Path(__file__).parent.parent / "gx"))

defs = Definitions(
    assets=all_assets,
    asset_checks=all_asset_checks,
    resources={
        "dbt": dbt_resource,
        "gx_resource": gx_resource,
        "execution_date_resource": ExecutionDateResource(),
    },
    jobs=[
        pureflow_pipeline_job,
        baseline_no_gates_job,
        data_generation_job,
        dirty_data_generation_job,
        quality_test_landing_job,
        quality_test_bronze_job,
    ],
)
