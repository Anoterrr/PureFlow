"""Core Quality Engine: Integrates Great Expectations with DuckDB and S3."""

import os
from pathlib import Path

import great_expectations as gx
import yaml
from dagster import ConfigurableResource
from great_expectations.core.expectation_suite import ExpectationSuite

from core.config import get_s3_connection_config
from core.logger import logger

# Resolved from this file, not os.getcwd(): the context used to break whenever
# the process was started from any directory other than the repo root.
DEFAULT_GX_ROOT = Path(__file__).resolve().parents[2] / "gx"


class GreatExpectationsResource(ConfigurableResource):
    """Custom resource to manage Great Expectations context."""

    ge_root_dir: str

    def get_context(self):
        """Returns the GX Data Context rooted at this resource's ge_root_dir.

        ge_root_dir used to be declared, set in orchestration.py and then
        ignored: get_gx_context() resolved "gx" against the current working
        directory instead. It is honoured now.
        """
        return get_gx_context(self.ge_root_dir)


def prime_s3_environment() -> None:
    """Normalizes the process-wide S3 environment once, at startup.

    Was `reinforce_global_s3_config()`, which also opened a throwaway DuckDB
    connection to run `SET GLOBAL` and create a named secret. That half was
    measured against a live MinIO to be a no-op: DuckDB scopes both to the
    database instance, and every `duckdb.connect(":memory:")` elsewhere in the
    process gets its own instance, so nothing it configured was ever visible to
    another connection. An unauthenticated connection failed identically before
    and after calling it.

    What does carry across the process is the environment. This is still called
    at import in orchestration.py because dbt/profiles.yml resolves its own
    endpoint from S3_ENDPOINT, which get_s3_connection_config() normalizes
    (localhost -> 127.0.0.1, or the Docker service name) before dbt is invoked.
    """
    cfg = get_s3_connection_config()
    logger.info("🌍 [S3] Environment primed for endpoint: %s", cfg["s3_endpoint"])


def get_gx_context(ge_root_dir: str | os.PathLike | None = None):
    """Returns an in-memory GX context built from <gx_root>/great_expectations.yml.

    `mode="ephemeral"` is not cosmetic. Without it GX resolves a
    FileDataContext against gx/, and every asset_check in a run is a separate
    process adding and deleting datasources in that one on-disk project. They
    corrupt each other: a process reading great_expectations.yml while another
    rewrites it dies with a yaml ScannerError, which surfaced as quality gates
    failing at random on perfectly good data. Ephemeral gives each process its
    own copy of the config and writes nothing back.

    Two consequences worth knowing:
    - store paths must be absolute, since an ephemeral context has no root
      directory to resolve them against. They are normalized below.
    - `fluent_datasources` is dropped. Every run registers its own uniquely
      named datasource, so the ones persisted in the file are leftovers from
      past runs (14 of them had accumulated) and nothing reads them.

    The root is resolved from this file's location, not os.getcwd(), so it no
    longer depends on which directory the process happened to start in.
    """
    root = Path(ge_root_dir).resolve() if ge_root_dir else DEFAULT_GX_ROOT
    (root / "uncommitted" / "data_docs").mkdir(parents=True, exist_ok=True)

    with open(root / "great_expectations.yml", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}

    def absolutize(value):
        """Paths in the file may be relative to gx/, or absolute from another machine."""
        text = str(value).strip()
        if os.path.isabs(text):
            # An absolute path baked in elsewhere (e.g. /app/gx/... from Docker)
            # only resolves here if that directory exists; otherwise re-root it.
            if Path(text).parent.exists():
                return text
            tail = text.split("/gx/", 1)
            text = tail[1] if len(tail) == 2 else Path(text).name
        return os.fspath(root / text)

    for key in ("plugins_directory", "config_variables_file_path"):
        if config.get(key):
            config[key] = absolutize(config[key])

    for group in ("stores", "data_docs_sites"):
        for entry in (config.get(group) or {}).values():
            backend = entry.get("store_backend", {})
            if "base_directory" in backend:
                backend["base_directory"] = absolutize(backend["base_directory"])

    config.pop("fluent_datasources", None)

    return gx.get_context(project_config=config, mode="ephemeral")


def get_or_create_suite(context, suite_name):
    """Abstraction for GX suite management."""
    try:
        return context.suites.get(name=suite_name)
    except Exception:
        try:
            suite = ExpectationSuite(name=suite_name)
            return context.suites.add(suite)
        except Exception:
            return context.suites.get(name=suite_name)
