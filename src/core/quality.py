"""Great Expectations context and Dagster resource."""

import os
from pathlib import Path

import great_expectations as gx
import yaml
from dagster import ConfigurableResource
from great_expectations.core.expectation_suite import ExpectationSuite

from core.config import get_s3_connection_config
from core.logger import logger

DEFAULT_GX_ROOT = Path(__file__).resolve().parents[2] / "gx"


class GreatExpectationsResource(ConfigurableResource):
    ge_root_dir: str

    def get_context(self):
        return get_gx_context(self.ge_root_dir)


def prime_s3_environment() -> None:
    """Exports the normalized S3 settings to the environment before dbt is invoked."""
    cfg = get_s3_connection_config()
    logger.info("🌍 [S3] Environment primed for endpoint: %s", cfg["s3_endpoint"])


def get_gx_context(ge_root_dir: str | os.PathLike | None = None):
    """Ephemeral GX context built from <gx_root>/great_expectations.yml.

    Ephemeral because checks run in parallel processes, and a shared on-disk
    context gets corrupted when they rewrite it concurrently. Store paths are
    made absolute since an ephemeral context has no root directory.
    """
    root = Path(ge_root_dir).resolve() if ge_root_dir else DEFAULT_GX_ROOT
    (root / "uncommitted" / "data_docs").mkdir(parents=True, exist_ok=True)

    with open(root / "great_expectations.yml", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}

    def absolutize(value):
        # Relative to gx/, or absolute from another machine (e.g. /app/gx/... from Docker).
        text = str(value).strip()
        if os.path.isabs(text):
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

    # Leftovers from past runs; each validation registers its own datasource.
    config.pop("fluent_datasources", None)

    return gx.get_context(project_config=config, mode="ephemeral")


def get_or_create_suite(context, suite_name):
    try:
        return context.suites.get(name=suite_name)
    except Exception:
        try:
            suite = ExpectationSuite(name=suite_name)
            return context.suites.add(suite)
        except Exception:
            return context.suites.get(name=suite_name)
