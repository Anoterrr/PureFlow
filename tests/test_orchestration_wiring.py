"""Regression guard: orchestration.py must auto-discover dbt models via the manifest.

Bronze/Silver/Gold all live as dbt models under dbt/models/ now (see
src/orchestration.py's module docstring) — a new model is picked up automatically
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
        "manifest.json — without it, models added under dbt/models/ are never registered "
        "with Dagster"
    )
