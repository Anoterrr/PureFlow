"""dbt-duckdb read plugin for the landing zone (raw CSV/JSON — the true external
boundary, not part of the Delta lakehouse).

dbt only Jinja-renders .sql files, not arbitrary `meta:` values in sources.yml
— so `{{ var('execution_date') }}` inside `meta.external_location` never
resolves (it reaches this code as a literal string). This plugin resolves the
date in Python instead, from the EXECUTION_DATE env var (which
pureflow_dbt_assets in orchestration.py sets before invoking dbt, so it always
agrees with the --vars execution_date used by the .sql models), falling back
to today — the same default dbt_project.yml's `execution_date` var uses.
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
