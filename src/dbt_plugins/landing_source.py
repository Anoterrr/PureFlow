"""dbt-duckdb read plugin for the landing zone (raw CSV/JSON, the true external
boundary — not part of the Delta lakehouse).

dbt only Jinja-renders .sql files, not `meta:` values in sources.yml, so
`{{ var('execution_date') }}` inside `meta.external_location` never resolves.
This plugin resolves the date in Python instead, from the EXECUTION_DATE env
var (set by pureflow_dbt_assets in orchestration.py before invoking dbt, to
stay in sync with the --vars execution_date the .sql models use).
"""

import os
from datetime import datetime

from dbt.adapters.duckdb.plugins import BasePlugin
from dbt.adapters.duckdb.utils import SourceConfig

from core.connection import ConnectionFactory


class Plugin(BasePlugin):
    def load(self, source_config: SourceConfig):
        execution_date = os.environ.get("EXECUTION_DATE") or datetime.now().strftime("%Y-%m-%d")
        path = source_config["path_template"].format(execution_date=execution_date)

        conn = ConnectionFactory.get_duckdb_conn(":memory:")
        ConnectionFactory.setup_s3_auth(conn)
        try:
            return conn.execute(f"SELECT * FROM '{path}'").arrow()  # nosec B608
        finally:
            conn.close()

    def default_materialization(self):
        return "view"
