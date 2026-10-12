"""DuckDB connections configured for S3 access."""

import os

import duckdb
from dotenv import load_dotenv

from core.config import get_s3_connection_config
from core.logger import logger

load_dotenv(override=False)


class ConnectionFactory:
    @staticmethod
    def get_duckdb_conn(db_path: str = None):
        """Returns a DuckDB connection with the httpfs, delta and json extensions loaded."""
        if db_path is None:
            db_path = os.getenv("DUCKDB_PATH", "data/datagate_local.db")

        if db_path != ":memory:":
            db_dir = os.path.dirname(db_path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)

        conn = duckdb.connect(db_path)

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
        """Creates the S3 secret; delta_scan only authenticates through a secret, not SET."""
        s3_cfg = get_s3_connection_config()

        logger.info(
            "🔌 [Conn] Configuring S3 access with endpoint: %s (Style: %s)",
            s3_cfg["s3_endpoint"],
            s3_cfg["s3_url_style"],
        )

        conn.execute(f"""
            CREATE OR REPLACE SECRET (
                TYPE S3,
                PROVIDER CREDENTIAL_CHAIN,
                ENDPOINT '{s3_cfg["s3_endpoint"]}',
                URL_STYLE 'path',
                USE_SSL false
            );
        """)
        logger.debug("✅ [Conn] S3 secret applied.")
