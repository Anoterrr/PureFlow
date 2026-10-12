"""Every quality gate must point at a model, source, asset and S3 path that exist.

Dagster silently drops a check whose asset does not exist. orchestration.py is
parsed with `ast` instead of imported, since importing it needs a dbt manifest.
"""

import ast
from pathlib import Path

import yaml
from src.core.config import get_s3_paths
from src.core.gates import SOURCE_GATES, TARGET_GATES

REPO_ROOT = Path(__file__).parent.parent
ORCHESTRATION_PATH = REPO_ROOT / "src" / "orchestration.py"
DBT_MODELS_DIR = REPO_ROOT / "dbt" / "models"

TREE = ast.parse(ORCHESTRATION_PATH.read_text(encoding="utf-8"))


def _dbt_model_names():
    return {p.stem for p in DBT_MODELS_DIR.rglob("*.sql")}


def _dbt_source_names():
    names = set()
    for sources_file in DBT_MODELS_DIR.rglob("sources.yml"):
        doc = yaml.safe_load(sources_file.read_text(encoding="utf-8")) or {}
        for source in doc.get("sources", []):
            for table in source.get("tables", []):
                names.add(table["name"])
    return names


def _generator_asset_names():
    for node in ast.walk(TREE):
        if isinstance(node, ast.Assign) and any(
            getattr(t, "id", None) == "GENERATORS" for t in node.targets
        ):
            return {
                spec.elts[0].value
                for spec in node.value.elts
                if isinstance(spec, ast.Tuple) and isinstance(spec.elts[0], ast.Constant)
            }
    return set()


def test_the_gate_tables_are_not_empty():
    assert SOURCE_GATES, "SOURCE_GATES is empty - no pre-flight gate would ever run"
    assert TARGET_GATES, "TARGET_GATES is empty - no post-write check would ever run"


def test_every_target_gate_attaches_to_a_real_dbt_model():
    models = _dbt_model_names()
    orphans = [g.name for g in TARGET_GATES if g.name not in models]
    assert not orphans, (
        f"TargetGate(s) attached to a model that does not exist: {orphans}. "
        f"Dagster ignores these silently, so the quality gate is simply gone. "
        f"dbt models found: {sorted(models)}"
    )


def test_every_source_gate_matches_a_real_dbt_source():
    sources = _dbt_source_names()
    orphans = [g.name for g in SOURCE_GATES if g.name not in sources]
    assert not orphans, (
        f"SourceGate(s) with no matching dbt source: {orphans}. The gate would "
        f"stop being an upstream dependency of the Bronze model and run beside "
        f"it instead. dbt sources found: {sorted(sources)}"
    )


def test_every_path_key_resolves():
    paths = get_s3_paths("2026-01-01")
    missing = [
        (g.name, g.path_key) for g in [*SOURCE_GATES, *TARGET_GATES] if g.path_key not in paths
    ]
    assert not missing, (
        f"Gate(s) whose path_key is not in get_s3_paths(): {missing}. "
        f"Available keys: {sorted(paths)}"
    )


def test_every_additional_dep_is_a_real_asset():
    known = _generator_asset_names() | {g.name for g in SOURCE_GATES} | _dbt_model_names()
    dangling = [
        (g.name, dep) for g in TARGET_GATES for dep in g.additional_deps if dep not in known
    ]
    assert not dangling, (
        f"additional_deps pointing at nothing: {dangling}. The ordering they "
        f"exist to enforce would never happen."
    )


def test_check_names_are_unique():
    names = [g.dagster_check_name for g in TARGET_GATES]
    assert len(names) == len(set(names)), f"duplicate asset_check names: {names}"


def test_orchestration_builds_its_gates_from_the_tables():
    source = ORCHESTRATION_PATH.read_text(encoding="utf-8")
    for table in ("SOURCE_GATES", "TARGET_GATES"):
        assert f"for g in {table}" in source, (
            f"orchestration.py no longer builds its gates from {table} - the "
            f"table would become documentation that nothing enforces"
        )


def test_dbt_models_take_their_bucket_from_the_environment():
    hardcoded = [
        p.relative_to(REPO_ROOT).as_posix()
        for p in DBT_MODELS_DIR.rglob("*.sql")
        for bucket in ("bronze", "silver", "gold")
        if f'"s3://{bucket}/' in p.read_text(encoding="utf-8")
    ]
    assert not hardcoded, f"models with a hard-coded bucket: {hardcoded}"
