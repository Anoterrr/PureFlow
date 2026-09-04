"""Regression guard: orchestration.py must auto-discover the pipelines package.

Dagster only registers assets it can actually find. orchestration.py relies on
`load_assets_from_package_module(pipelines)` to scan every module under
src/pipelines/ for Bronze/Silver assets — a new domain is picked up just by
dropping a file there, with no manual import/registration step. We previously
shipped a version that required (and forgot) an explicit per-module import,
which silently dropped the Bronze/Silver assets from Dagster entirely. This
test fails fast if the auto-discovery wiring regresses.
"""

import ast
from pathlib import Path

ORCHESTRATION_PATH = Path(__file__).parent.parent / "src" / "orchestration.py"


def test_orchestration_auto_discovers_pipeline_package():
    tree = ast.parse(ORCHESTRATION_PATH.read_text(encoding="utf-8"))

    imports_pipelines_package = any(
        isinstance(node, ast.Import) and any(alias.name == "pipelines" for alias in node.names)
        for node in ast.walk(tree)
    )
    calls_package_loader = any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "load_assets_from_package_module"
        for node in ast.walk(tree)
    )

    assert imports_pipelines_package, (
        "orchestration.py must `import pipelines` so load_assets_from_package_module() "
        "can scan every module under src/pipelines/ for assets"
    )
    assert calls_package_loader, (
        "orchestration.py must call load_assets_from_package_module(pipelines) — without "
        "it, new files added to src/pipelines/ are silently never registered with Dagster"
    )
