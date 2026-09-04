"""dbt-duckdb write plugin: materializes 'external' models as Delta Lake tables.

dbt-duckdb ships a read-only 'delta' plugin (load() only, no store()) — writing
Delta isn't natively supported. This plugin hooks into the external
materialization's store_relation() call (see dbt-duckdb's external.sql: after
writing the model's result to `location` in a native format, it hands off to
the configured plugin's store()) to convert that staged file into a real Delta
table via delta-rs, the same library core/engine.py already uses.

Reads the staged output back via DuckDB (not pandas+s3fs, which isn't a
dependency here) since DuckDB's S3/MinIO wiring is already established
throughout this codebase.
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
            # A model's SQL can legitimately change its output schema between
            # runs (dev iteration, new columns) — a full overwrite should just
            # take the new schema rather than hard-failing on mismatch.
            schema_mode="overwrite" if mode == "overwrite" else None,
            storage_options=get_delta_storage_options(),
        )

    def default_materialization(self):
        return "external"
