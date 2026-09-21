"""Dagster orchestration for PureFlow.

Bronze/Silver/Gold live as dbt models (dbt/models/). This module wires them
into Dagster, plus the Great Expectations quality gates around them: a
pre-flight check on raw landing files and a post-write @asset_check per model.
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
    """Custom translator for PureFlow dbt assets to map sources and groups."""

    def get_asset_key(self, dbt_resource_props):
        """Flat, schema-agnostic AssetKeys for both sources and models.

        Sources map onto the landing-zone gate assets below by name; models get
        a plain AssetKey(name) (instead of dagster-dbt's default schema-prefixed
        key) so the @asset_check definitions below, which target AssetKey(name)
        directly, actually attach to the right asset.
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
    """Bronze/Silver/Gold: every dbt model, materialized as Delta via the delta_rw plugin."""
    execution_date = execution_date_resource.resolved_date
    # The landing_read plugin (dbt_plugins/landing_source.py) can't see dbt's --vars
    # (it only Jinja-renders .sql, not sources.yml meta), so it reads this env var
    # directly, so it is set here to keep both resolution paths in agreement.
    os.environ["EXECUTION_DATE"] = execution_date
    yield from dbt.cli(
        ["run", "--vars", f"execution_date: {execution_date}"],
        context=context,
    ).stream()


def _run_source_gate(context, gx_resource, execution_date, path, data_format, expectations, name):
    """Pre-flight circuit breaker: validates raw input *before* a dbt model reads it.

    Raises on failure. A ValidationTechnicalError propagates untouched and
    skips quarantine: if the gate could not read the data, it has
    no basis to condemn it.
    """
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
    """Post-write circuit breaker: validates a dbt model's output, quarantining it on failure."""
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
        # The check still fails, but it is tagged as technical and nothing is
        # quarantined: the data was never read, so it was never judged.
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
    """One pre-flight asset per SOURCE_GATES entry."""

    @asset(
        name=gate.name,
        group_name="landing",
        compute_kind="python",
        # Only actually enforced when a job selects both, which is the case in
        # quality_test_landing_job. Jobs that leave inject_corrupt_landing out
        # (e.g. pureflow_pipeline_job) treat it as already-satisfied, per normal
        # Dagster subset semantics. Without this edge the gate can read landing
        # before the corruptor has finished writing it.
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
    """One post-write @asset_check per TARGET_GATES entry."""

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
    """One synthetic-data asset per GENERATORS entry."""

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
        # No deps on the Bronze models here. Depending on them would be
        # the obvious way to land after `dbt run`, but the Bronze checks are
        # blocking, so Dagster puts the check node between the asset and its
        # consumers and the graph becomes corruptor -> check -> corruptor.
        # quality_test_bronze_job solves it by leaving dbt out of the run.
        (),
    ),
]

source_gate_assets = [_build_source_gate_asset(g) for g in SOURCE_GATES]
target_gate_checks = [_build_target_gate_check(g) for g in TARGET_GATES]
checks_by_gate = dict(zip([g.name for g in TARGET_GATES], target_gate_checks, strict=True))
generator_assets = [_build_generator_asset(*spec) for spec in GENERATORS]

# Built explicitly rather than via load_assets_from_current_module(): the gate
# assets are produced by a factory, and that helper only picks up assets bound
# to a module-level name. It also never collected @asset_check at all, which is
# why the check list used to be maintained by hand, and could miss one.
all_assets = [pureflow_dbt_assets, *source_gate_assets, *generator_assets]
all_asset_checks = target_gate_checks

# The two data generators write to the *same* landing paths, so a job selecting
# both races and the winner is undefined. They get one job each: you generate a
# clean landing zone or a dirty one, never both at once.
GENERATOR_GROUPS = ("clean_data", "dirty_data", "test_quality")

pureflow_pipeline_job = define_asset_job(
    name="pureflow_pipeline_job",
    selection=AssetSelection.all() - AssetSelection.groups(*GENERATOR_GROUPS),
)

data_generation_job = define_asset_job(
    name="data_generation_job",
    selection=AssetSelection.groups("clean_data"),
)

dirty_data_generation_job = define_asset_job(
    name="dirty_data_generation_job",
    selection=AssetSelection.groups("dirty_data"),
)

# Two separate demos, one per circuit breaker. They cannot be shown in a single
# run any more: now that the pre-flight gate actually fires, corrupt landing
# halts the pipeline before dbt materializes Bronze, so a Bronze corruption in
# the same run would never be reached by anything.
quality_test_landing_job = define_asset_job(
    name="quality_test_landing_job",
    # Pre-flight breaker: corrupt the raw files, watch the landing gates refuse
    # them and quarantine before a single dbt model reads them.
    selection=(
        AssetSelection.assets(AssetKey("inject_corrupt_landing")) | AssetSelection.groups("landing")
    ),
)

quality_test_bronze_job = define_asset_job(
    name="quality_test_bronze_job",
    # Post-write breaker, run as step two: `pureflow_pipeline_job` first to
    # materialize a clean Bronze, then this, which corrupts those Delta tables
    # and re-runs only their blocking gates. dbt is NOT in this
    # job - when it was, `dbt run` rebuilt Bronze from clean landing and wiped
    # the corruption before the gate could see it.
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
        data_generation_job,
        dirty_data_generation_job,
        quality_test_landing_job,
        quality_test_bronze_job,
    ],
)
