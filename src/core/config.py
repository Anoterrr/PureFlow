"""Shared configuration: partition dates, bucket names and S3 connection settings."""

import os
from datetime import datetime
from zoneinfo import ZoneInfo

from core.logger import logger

# Host and containers must agree on the partition date, so it is never naive local time.
PIPELINE_TIMEZONE = ZoneInfo(os.getenv("PIPELINE_TIMEZONE", "UTC"))

S3_PORT = 18900


def today() -> str:
    return datetime.now(PIPELINE_TIMEZONE).strftime("%Y-%m-%d")


def get_base_date() -> str:
    """BASE_DATE if set, otherwise today. Resolved per call, not at import."""
    return os.getenv("BASE_DATE") or today()


S3_BUCKET_LANDING = os.getenv("S3_BUCKET_LANDING", "landing-zone")
S3_BUCKET_BRONZE = os.getenv("S3_BUCKET_BRONZE", "bronze")
S3_BUCKET_SILVER = os.getenv("S3_BUCKET_SILVER", "silver")
S3_BUCKET_GOLD = os.getenv("S3_BUCKET_GOLD", "gold")


def get_s3_connection_config():
    """Resolves the S3 endpoint for the current environment and exports the AWS_* variables."""
    is_docker_env = os.getenv("IS_DOCKER", "false").lower() == "true"
    has_dockerenv = os.path.exists("/.dockerenv")
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
        s3_endpoint = f"http://storage:{S3_PORT}" if is_docker else f"http://127.0.0.1:{S3_PORT}"

    # DuckDB ignores path-style addressing for 'localhost', so use an IP or the service name.
    if is_docker:
        if any(local in s3_endpoint for local in ["localhost", "127.0.0.1"]):
            logger.info("🐳 [Config] Docker detected. Forcing 'storage:%s' endpoint.", S3_PORT)
            s3_endpoint = f"http://storage:{S3_PORT}"
    else:
        if "localhost" in s3_endpoint:
            logger.info(
                "🏠 [Config] Local environment. Replacing 'localhost' with '127.0.0.1' to force path-style."
            )
            s3_endpoint = s3_endpoint.replace("localhost", "127.0.0.1")

    logger.info("🚀 [Config] Final S3 Endpoint resolved to: %s", s3_endpoint)

    os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("STORAGE_USER", "admin")
    os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("STORAGE_PASSWORD", "strongpassword123")
    os.environ["AWS_ENDPOINT_URL"] = s3_endpoint
    os.environ["AWS_REGION"] = "us-east-1"

    clean_endpoint = s3_endpoint.replace("http://", "").replace("https://", "")

    os.environ["S3_ENDPOINT"] = clean_endpoint

    return {
        "s3_endpoint": clean_endpoint,
        "s3_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
        "s3_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
        "s3_use_ssl": "false",
        "s3_url_style": "path",
        "s3_region": "us-east-1",
    }


def get_delta_storage_options() -> dict[str, str]:
    """Storage options for delta-rs reads and writes."""
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
        # S3 has no atomic rename; safe here because each table has a single writer.
        "s3_allow_unsafe_rename": "true",
    }


def get_s3_paths(base_date: str):
    """S3 URIs for every layer. Single source of truth for the storage layout."""
    return {
        "sales_landing": (f"s3://{S3_BUCKET_LANDING}/sales_erp/dt={base_date}/sales.csv"),
        "customers_landing": (
            f"s3://{S3_BUCKET_LANDING}/customers_crm/dt={base_date}/customers.json"
        ),
        "sales_bronze": f"s3://{S3_BUCKET_BRONZE}/sales_erp/dt={base_date}/stg_sales_bronze",
        "customers_bronze": (
            f"s3://{S3_BUCKET_BRONZE}/customers_crm/dt={base_date}/stg_customers_bronze"
        ),
        "sales_silver": f"s3://{S3_BUCKET_SILVER}/sales/dt={base_date}",
        "customers_silver": f"s3://{S3_BUCKET_SILVER}/customers/dt={base_date}",
        "sales_summary": f"s3://{S3_BUCKET_GOLD}/sales_summary/dt={base_date}",
    }
