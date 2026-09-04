"""Core Engine for executing data engineering tasks using DuckDB."""

from typing import Any

from deltalake import write_deltalake

from core.config import BASE_DATE, get_s3_connection_config
from core.connection import ConnectionFactory
from core.logger import logger


class PureFlowEngine:
    """Technically executes data movement, transformation and validation tasks."""

    def __init__(self, execution_date: str = None):
        self.execution_date = execution_date or BASE_DATE
        self.factory = ConnectionFactory()

    def render_path(self, path: str, context: dict[str, str] | None = None) -> str:
        """
        Renders dynamic variables in paths.
        Context can include: name, group, format.
        """
        rendered = path.replace("{{ execution_date }}", self.execution_date)

        if context:
            for key, value in context.items():
                rendered = rendered.replace(f"{{{{ {key} }}}}", str(value))

            # Handle automatic extensions based on format
            fmt = context.get("format", "").lower()
            ext = ""
            if fmt == "parquet":
                ext = ".parquet"
            elif fmt == "csv":
                ext = ".csv"
            elif fmt == "json":
                ext = ".json"
            # delta has no extension (directory)

            rendered = rendered.replace("{{ extension }}", ext)

        return rendered

    def quarantine_data(self, source_path: str, reason: str, source_format: str = "parquet") -> str:
        """
        Moves failing data to a quarantine prefix in S3.
        Returns the new quarantine path.
        """
        source_path = self.render_path(source_path)

        # Build quarantine path: s3://bucket/quarantine/dt=YYYY-MM-DD/reason=.../filename
        path_parts = source_path.replace("s3://", "").split("/")
        bucket = path_parts[0]
        filename = (
            path_parts[-1] if path_parts[-1] else path_parts[-2]
        )  # Handle trailing slash for Delta

        quarantine_prefix = f"quarantine/dt={self.execution_date}/reason={reason.replace(' ', '_')}"
        target_quarantine_path = f"s3://{bucket}/{quarantine_prefix}/{filename}"

        conn = self.factory.get_duckdb_conn(db_path=":memory:")
        self.factory.setup_s3_auth(conn)

        try:
            logger.warning(
                "🛡️ [Engine] Quarantining data: %s -> %s",
                source_path,
                target_quarantine_path,
            )

            # Detect format for reading during quarantine
            fmt = source_format.lower()
            if fmt == "delta":
                read_func = "delta_scan"
            elif fmt in ["csv", "json"]:
                read_func = f"read_{fmt}_auto"
            else:
                read_func = "read_parquet"

            # Use DuckDB's internal S3 copy capabilities
            # This is a 'move' simulated by COPY
            # read_func comes from the fixed whitelist above, not external input
            conn.execute(
                f"COPY (SELECT * FROM {read_func}(?)) TO ? (FORMAT 'PARQUET')",  # nosec B608
                [source_path, target_quarantine_path],
            )

            return target_quarantine_path
        except Exception as e:
            # Broad exception caught to prevent engine crash during quarantine attempt
            logger.error("❌ [Engine] Failed to quarantine: %s", str(e))
            return source_path
        finally:
            conn.close()

    def execute_move_and_transform(
        self,
        source_path: str,
        source_format: str,
        target_path: str,
        target_format: str,
        sql_transform: str | None = None,
    ) -> dict[str, Any]:
        """
        Executes a move from source to target with an optional SQL transformation.
        Supports DELTA format using delta-rs for proper transaction logs.
        """
        source_path = self.render_path(source_path)
        target_path = self.render_path(target_path)

        conn = self.factory.get_duckdb_conn(db_path=":memory:")
        self.factory.setup_s3_auth(conn)

        try:
            logger.info(
                "🚀 [Engine] Moving: %s (%s) -> %s (%s)",
                source_path,
                source_format,
                target_path,
                target_format,
            )

            # 1. Define the source read function
            fmt = source_format.lower()
            if fmt == "delta":
                read_func = "delta_scan"
            else:
                read_func = f"read_{fmt}_auto" if fmt in ["csv", "json"] else "read_parquet"

            # 2. Build the query
            conn.execute(
                f"CREATE OR REPLACE VIEW source_data AS SELECT * FROM {read_func}('{source_path}')"  # nosec B608
            )

            final_query = sql_transform if sql_transform else "SELECT * FROM source_data"

            # 3. Execute and Write
            if target_format.upper() == "DELTA":
                logger.info("📦 [Engine] Writing to Delta Lake via delta-rs...")
                # Fetch as Arrow for high-performance zero-copy transfer to delta-rs
                result_arrow = conn.execute(final_query).fetch_arrow_table()
                row_count = len(result_arrow)

                s3_cfg = get_s3_connection_config()
                # Use the resolved endpoint (which we now force to IP or 'minio')
                endpoint = s3_cfg["s3_endpoint"]
                if not endpoint.startswith("http"):
                    endpoint = f"http://{endpoint}"

                storage_options = {
                    "endpoint_url": endpoint,
                    "access_key_id": s3_cfg["s3_access_key_id"],
                    "secret_access_key": s3_cfg["s3_secret_access_key"],
                    "region": s3_cfg["s3_region"],
                    "allow_http": "true",
                    "s3_allow_unsafe_rename": "true",  # Needed for MinIO/S3 non-atomic renames
                }

                write_deltalake(
                    target_path, result_arrow, mode="overwrite", storage_options=storage_options
                )
            else:
                # Standard DuckDB COPY for other formats
                copy_query = f"""
                    COPY ({final_query})
                    TO '{target_path}' (FORMAT '{target_format.upper()}')
                """
                conn.execute(copy_query)
                row_count = conn.execute("SELECT count(*) FROM source_data").fetchone()[0]

            logger.info(
                "✅ [Engine] Success! Processed %d rows using %s.", row_count, target_format
            )

            return {
                "status": "success",
                "row_count": row_count,
                "target_path": target_path,
                "format": target_format,
            }

        except Exception as e:
            # Broad exception re-raised after logging for context
            logger.error("❌ [Engine] Failed execution: %s", str(e))
            raise e
        finally:
            conn.close()
