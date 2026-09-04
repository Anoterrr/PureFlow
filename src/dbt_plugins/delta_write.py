"""dbt-duckdb write plugin: materializes 'external' models as Delta tables.

dbt-duckdb's bundled 'delta' plugin only reads (no store()) — Delta writes
aren't native. This hooks into the external materialization's store_relation()
call to convert the staged output into a real Delta table via delta-rs,
reading it back through DuckDB rather than pandas+s3fs (not a dependency here).
"""

from dbt.adapters.duckdb.plugins import BasePlugin
from dbt.adapters.duckdb.utils import TargetConfig
from deltalake import write_deltalake

from core.config import get_delta_storage_options
from core.connection import ConnectionFactory


class Plugin(BasePlugin):
    def store(self, target_config: TargetConfig):
        location = target_config.location
        if location is None:
            raise ValueError("delta_write plugin requires a staged 'location' to read back")

        delta_table_path = target_config.config.get("delta_table_path")
        if not delta_table_path:
            raise ValueError("delta_write plugin requires 'delta_table_path' in the model config")

        conn = ConnectionFactory.get_duckdb_conn(":memory:")
        ConnectionFactory.setup_s3_auth(conn)
        try:
            table = conn.execute(
                f"SELECT * FROM read_parquet('{location.path}')"  # nosec B608
            ).fetch_arrow_table()
        finally:
            conn.close()

        mode = target_config.config.get("delta_mode", "overwrite")
        write_deltalake(
            delta_table_path,
            table,
            mode=mode,
            # A full overwrite should take the model's current schema rather
            # than hard-failing if a column was added/removed since last run.
            schema_mode="overwrite" if mode == "overwrite" else None,
            storage_options=get_delta_storage_options(),
        )

    def default_materialization(self):
        return "external"
