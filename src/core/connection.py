"""Module to manage connections to Storage (MinIO) and the Processing Engine (DuckDB)."""

import os

import duckdb
from dotenv import load_dotenv

from core.config import get_s3_connection_config
from core.logger import logger

# Only load .env if variables are not already set (prevents overriding Docker env with localhost)
load_dotenv(override=False)


class ConnectionFactory:
    """Manages connections to Storage (MinIO) and the Processing Engine (DuckDB)."""

    @staticmethod
    def get_duckdb_conn(db_path: str = None):
        """Returns a DuckDB connection. Use :memory: for non-persistent tasks to avoid locks."""
        if db_path is None:
            db_path = os.getenv("DUCKDB_PATH", "data/datagate_local.db")

        if db_path != ":memory:":
            db_dir = os.path.dirname(db_path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)

        conn = duckdb.connect(db_path)

        # Configurable resource limits (defaults are conservative to fit lower-RAM
        # environments, e.g. Docker Desktop on Mac). Override via .env if needed.
        memory_limit = os.getenv("DUCKDB_MEMORY_LIMIT", "4GB")
        threads = os.getenv("DUCKDB_THREADS", "2")
        conn.execute(f"SET memory_limit = '{memory_limit}'")
        conn.execute(f"SET threads = {threads}")

        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        conn.execute("INSTALL delta;")
        conn.execute("LOAD delta;")
        conn.execute("INSTALL json;")
        conn.execute("LOAD json;")

        return conn

    @staticmethod
    def setup_s3_auth(conn):
        """Configures credentials for DuckDB to see MinIO using the official Secrets Manager (Hyper-Redundant)."""
        s3_cfg = get_s3_connection_config()

        logger.info(
            "🔌 [Conn] Configuring S3 access with endpoint: %s (Style: %s)",
            s3_cfg["s3_endpoint"],
            s3_cfg["s3_url_style"],
        )

        # Enforce path style and endpoint (Session + Global)
        conn.execute("SET s3_url_style = 'path'")
        conn.execute("SET GLOBAL s3_url_style = 'path'")
        conn.execute(f"SET s3_endpoint = '{s3_cfg['s3_endpoint']}'")
        conn.execute(f"SET GLOBAL s3_endpoint = '{s3_cfg['s3_endpoint']}'")
        conn.execute("SET s3_use_ssl = false")
        conn.execute("SET GLOBAL s3_use_ssl = false")

        conn.execute(f"""
            CREATE OR REPLACE SECRET (
                TYPE S3,
                PROVIDER CREDENTIAL_CHAIN,
                ENDPOINT '{s3_cfg["s3_endpoint"]}',
                URL_STYLE 'path',
                USE_SSL false
            );
        """)
        logger.debug("✅ [Conn] S3 Secrets and Session parameters applied.")
