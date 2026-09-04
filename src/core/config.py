"""Shared configuration for data generation and ingestion."""

import os
from datetime import datetime

from core.logger import logger

# Base date for data generation and processing. No fixed date is baked in:
# falls back to today (at process start) unless BASE_DATE is set explicitly,
# e.g. to reproduce a specific past run.
BASE_DATE = os.getenv("BASE_DATE") or datetime.now().strftime("%Y-%m-%d")

# S3 / MinIO Configuration
S3_BUCKET_LANDING = os.getenv("S3_BUCKET_LANDING", "landing-zone")
S3_BUCKET_BRONZE = os.getenv("S3_BUCKET_BRONZE", "bronze")
S3_BUCKET_SILVER = os.getenv("S3_BUCKET_SILVER", "silver")
S3_BUCKET_GOLD = os.getenv("S3_BUCKET_GOLD", "gold")
S3_BUCKET_QUARANTINE = os.getenv("S3_BUCKET_QUARANTINE", "quarantine")


def get_s3_connection_config():
    """
    Returns a dictionary with DuckDB S3 configuration settings.
    Handles Docker detection and endpoint resolution.
    Also injects standard AWS environment variables for process-wide consistency.
    """
    # Robust Docker detection
    is_docker_env = os.getenv("IS_DOCKER", "false").lower() == "true"
    has_dockerenv = os.path.exists("/.dockerenv")
    # Modern cgroup v2 check
    is_cgroup_docker = False
    if os.path.exists("/proc/self/cgroup"):
        with open("/proc/self/cgroup") as f:
            content = f.read()
            if "docker" in content or "kubepods" in content:
                is_cgroup_docker = True

    is_docker = is_docker_env or has_dockerenv or is_cgroup_docker

    s3_endpoint = os.getenv("S3_ENDPOINT")

    logger.debug(
        "🔍 [Config] is_docker_env: %s, has_dockerenv: %s, is_cgroup_docker: %s -> is_docker: %s",
        is_docker_env,
        has_dockerenv,
        is_cgroup_docker,
        is_docker,
    )
    logger.debug("🔍 [Config] Environment S3_ENDPOINT: %s", s3_endpoint)

    if not s3_endpoint:
        s3_endpoint = "http://minio:9000" if is_docker else "http://127.0.0.1:9000"

    # CRITICAL: DuckDB 1.1.0+ often ignores s3_url_style='path' when using 'localhost'.
    # Using '127.0.0.1' or the service name 'minio' forces path-style addressing.
    if is_docker:
        if any(local in s3_endpoint for local in ["localhost", "127.0.0.1"]):
            logger.info("🐳 [Config] Docker detected. Forcing 'minio:9000' endpoint.")
            s3_endpoint = "http://minio:9000"
    else:
        if "localhost" in s3_endpoint:
            logger.info(
                "🏠 [Config] Local environment. Replacing 'localhost' with '127.0.0.1' to force path-style."
            )
            s3_endpoint = s3_endpoint.replace("localhost", "127.0.0.1")

    logger.info("🚀 [Config] Final S3 Endpoint resolved to: %s", s3_endpoint)

    # Set standard AWS/S3 environment variables to ensure official SDKs and
    # tools (like DuckDB httpfs) pick them up automatically.
    os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("STORAGE_USER", "admin")
    os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("STORAGE_PASSWORD", "strongpassword123")
    os.environ["AWS_ENDPOINT_URL"] = s3_endpoint
    os.environ["AWS_REGION"] = "us-east-1"

    # Hints for DuckDB and other tools
    os.environ["DUCKDB_S3_URL_STYLE"] = "path"
    os.environ["DUCKDB_S3_USE_SSL"] = "false"
    os.environ["AWS_S3_ADDRESSING_STYLE"] = "path"
    os.environ["AWS_S3_PATH_STYLE_ACCESS"] = "true"

    # Clean endpoint for DuckDB (remove http:// or https://)
    clean_endpoint = s3_endpoint.replace("http://", "").replace("https://", "")

    # DuckDB specific environment variables (Hyper-Redundant)
    os.environ["DUCKDB_S3_ENDPOINT"] = clean_endpoint
    os.environ["DUCKDB_S3_REGION"] = "us-east-1"
    os.environ["S3_URL_STYLE"] = "path"
    os.environ["S3_ENDPOINT"] = (
        clean_endpoint  # DuckDB sometimes looks for this without DUCKDB_ prefix
    )

    return {
        "s3_endpoint": clean_endpoint,
        "s3_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
        "s3_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
        "s3_use_ssl": "false",
        "s3_url_style": "path",
        "s3_region": "us-east-1",
    }


def get_delta_storage_options() -> dict[str, str]:
    """Storage options for deltalake (delta-rs) reads/writes against MinIO/S3."""
    s3_cfg = get_s3_connection_config()
    endpoint = s3_cfg["s3_endpoint"]
    if not endpoint.startswith("http"):
        endpoint = f"http://{endpoint}"

    return {
        "endpoint_url": endpoint,
        "access_key_id": s3_cfg["s3_access_key_id"],
        "secret_access_key": s3_cfg["s3_secret_access_key"],
        "region": s3_cfg["s3_region"],
        "allow_http": "true",
        "s3_allow_unsafe_rename": "true",  # MinIO/S3 non-atomic renames
    }


def get_s3_paths(base_date: str):
    """Returns the S3 URI paths for ingestion and validation layers."""
    return {
        # Landing (Input)
        "sales_landing": (f"s3://{S3_BUCKET_LANDING}/sales_erp/dt={base_date}/sales.csv"),
        "customers_landing": (
            f"s3://{S3_BUCKET_LANDING}/customers_crm/dt={base_date}/customers.json"
        ),
        # Bronze (Delta)
        "sales_bronze": f"s3://{S3_BUCKET_BRONZE}/sales_erp/dt={base_date}/stg_sales_bronze",
        "customers_bronze": (
            f"s3://{S3_BUCKET_BRONZE}/customers_crm/dt={base_date}/stg_customers_bronze"
        ),
        # Silver (Delta)
        "sales_silver": f"s3://{S3_BUCKET_SILVER}/sales/dt={base_date}",
        "customers_silver": f"s3://{S3_BUCKET_SILVER}/customers/dt={base_date}",
        # Gold (Delta)
        "sales_summary": f"s3://{S3_BUCKET_GOLD}/sales_summary/dt={base_date}",
        # Quarantine (Failed validation)
        "sales_quarantine": (f"s3://{S3_BUCKET_QUARANTINE}/sales_erp/dt={base_date}/sales.parquet"),
        "customers_quarantine": (
            f"s3://{S3_BUCKET_QUARANTINE}/customers_crm/dt={base_date}/customers.parquet"
        ),
    }
