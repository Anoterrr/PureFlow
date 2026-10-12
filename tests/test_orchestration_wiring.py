"""Regression guard: orchestration.py must auto-discover dbt models via the manifest.

Bronze/Silver/Gold all live as dbt models under dbt/models/ now (see
src/orchestration.py's module docstring). A new model is picked up automatically
by dbt's own project scanning when `dbt run`/`dbt compile` regenerates the
manifest, with no per-model Python registration step. This test fails fast if
that wiring regresses back to requiring manual asset registration.
"""

import ast
from pathlib import Path

ORCHESTRATION_PATH = Path(__file__).parent.parent / "src" / "orchestration.py"


def test_orchestration_wires_dbt_assets_from_manifest():
    tree = ast.parse(ORCHESTRATION_PATH.read_text(encoding="utf-8"))

    dbt_assets_decorated_functions = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef)
        and any(
            (isinstance(dec, ast.Call) and getattr(dec.func, "id", None) == "dbt_assets")
            or (isinstance(dec, ast.Name) and dec.id == "dbt_assets")
            for dec in node.decorator_list
        )
    ]

    assert dbt_assets_decorated_functions, (
        "orchestration.py must have a @dbt_assets-decorated function pointed at the dbt "
        "manifest.json. Without it, models added under dbt/models/ are never registered "
        "with Dagster"
    )


def test_dbt_runs_one_op_per_medallion_layer():
    """A blocking check only holds back assets computed by a *different* op.

    With one @dbt_assets covering the whole project, Bronze, Silver and Gold
    were written by the same `dbt run` before any post-write check ran, so the
    blocking Bronze/Silver gates blocked nothing. Each layer must be its own op.
    """
    source = ORCHESTRATION_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)

    layers = next(
        (
            tuple(e.value for e in node.value.elts)
            for node in ast.walk(tree)
            if isinstance(node, ast.Assign)
            and any(getattr(t, "id", None) == "DBT_LAYERS" for t in node.targets)
        ),
        (),
    )
    assert layers == ("bronze", "silver", "gold"), f"DBT_LAYERS is {layers}"

    selects = [
        kw
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "dbt_assets"
        for kw in node.keywords
        if kw.arg == "select"
    ]
    assert selects, "@dbt_assets has no select=, so a single op runs every layer again"
