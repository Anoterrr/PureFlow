"""dbt-duckdb plugin that reads raw landing files (CSV/JSON) from S3.

dbt does not render Jinja in sources.yml `meta`, so the partition date comes
from EXECUTION_DATE, which orchestration.py sets before each dbt run.
"""

import os

from dbt.adapters.duckdb.plugins import BasePlugin
from dbt.adapters.duckdb.utils import SourceConfig

from core.config import S3_BUCKET_LANDING, get_base_date
from core.connection import ConnectionFactory


class Plugin(BasePlugin):
    def load(self, source_config: SourceConfig):
        execution_date = os.environ.get("EXECUTION_DATE") or get_base_date()
        path = source_config["path_template"].format(
            landing_bucket=S3_BUCKET_LANDING, execution_date=execution_date
        )

        conn = ConnectionFactory.get_duckdb_conn(":memory:")
        ConnectionFactory.setup_s3_auth(conn)
        try:
            return conn.execute(f"SELECT * FROM '{path}'").arrow()  # nosec B608
        finally:
            conn.close()

    def default_materialization(self):
        return "view"
