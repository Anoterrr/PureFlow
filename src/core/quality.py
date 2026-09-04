"""Core Quality Engine: Integrates Great Expectations with DuckDB and S3."""

import os

import duckdb
import great_expectations as gx
import yaml
from dagster import ConfigurableResource
from great_expectations.core.expectation_suite import ExpectationSuite

from core.config import get_s3_connection_config
from core.logger import logger


# --- Custom Great Expectations Resource ---
class GreatExpectationsResource(ConfigurableResource):
    """Custom resource to manage Great Expectations context."""

    ge_root_dir: str

    def get_context(self):
        """Returns the GX Data Context."""
        return get_gx_context()


# --- Process-level S3 Reinforcement ---
# Sets GLOBAL-scope DuckDB S3 defaults once per process, as a fallback for any
# connection that isn't explicitly configured via ConnectionFactory.setup_s3_auth().
# Call this once, explicitly, at process startup (see orchestration.py) — it must
# NOT run as a module-import side effect (that made every import of this module
# open a live DuckDB/S3 connection, slowing down and destabilizing tests).
def reinforce_global_s3_config():
    """Reinforces S3 configuration at the process level for all DuckDB connections."""
    try:
        s3_cfg = get_s3_connection_config()
        logger.debug(
            "🌍 [Global-Reinforce] Starting reinforcement with endpoint: %s", s3_cfg["s3_endpoint"]
        )
        with duckdb.connect() as global_conn:
            global_conn.execute("INSTALL httpfs; LOAD httpfs;")

            # Global-level reinforcement
            global_conn.execute("SET GLOBAL s3_url_style = 'path';")
            global_conn.execute("SET GLOBAL s3_use_ssl = false;")
            global_conn.execute(f"SET GLOBAL s3_endpoint = '{s3_cfg['s3_endpoint']}';")

            # Also create a named secret for extra redundancy with explicit endpoint
            global_conn.execute(f"""
                CREATE OR REPLACE SECRET minio_global (
                    TYPE S3,
                    KEY_ID '{s3_cfg["s3_access_key_id"]}',
                    SECRET '{s3_cfg["s3_secret_access_key"]}',
                    ENDPOINT '{s3_cfg["s3_endpoint"]}',
                    URL_STYLE 'path',
                    USE_SSL false,
                    REGION 'us-east-1',
                    SCOPE 's3://'
                );
            """)
        logger.info("🌍 [Global-Reinforce] DuckDB environment reinforced.")
    except Exception as e:
        logger.warning("⚠️ [Global-Reinforce] Initial reinforcement failed: %s", str(e))


def get_gx_context():
    """
    Initializes and returns an Ephemeral GX context.
    """
    context_root_dir = os.path.abspath("gx")
    config_path = os.path.join(context_root_dir, "great_expectations.yml")

    # Ensure necessary directories exist
    os.makedirs(os.path.join(context_root_dir, "uncommitted/data_docs"), exist_ok=True)

    with open(config_path, encoding="utf-8") as f:
        project_config_dict = yaml.safe_load(f) or {}

    # Path normalization for local vs docker environments
    for key in ["plugins_directory", "config_variables_file_path"]:
        if key in project_config_dict:
            val = project_config_dict[key]
            if val and not os.path.isabs(val):
                project_config_dict[key] = os.path.abspath(os.path.join(context_root_dir, val))
            # Force local path if /app/ prefix found but we are not in docker
            elif (
                val
                and os.path.isabs(val)
                and val.startswith("/app/gx/")
                and not os.path.exists("/.dockerenv")
            ):
                local_val = val.replace("/app/gx/", "./gx/")
                project_config_dict[key] = os.path.abspath(os.path.join(os.getcwd(), local_val))

    return gx.get_context(project_config=project_config_dict)


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
