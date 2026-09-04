"""Core Engine: path templating and quarantine handling shared across pipeline gates.

Bronze/silver/gold transformation itself lives in dbt models (see dbt/models/);
this engine only backs the quality-gate assets in orchestration.py.
"""

from core.config import BASE_DATE
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
            # read_func comes from the fixed whitelist above, not external input.
            # Both paths are inlined (not bound as `?`) — a COPY with a
            # placeholder both inside the SELECT and in the TO clause silently
            # mis-binds on this DuckDB version; source_path/target_quarantine_path
            # are built internally, never raw user input, so inlining is safe.
            conn.execute(
                f"COPY (SELECT * FROM {read_func}('{source_path}')) "  # nosec B608
                f"TO '{target_quarantine_path}' (FORMAT 'PARQUET')"
            )

            return target_quarantine_path
        except Exception as e:
            # Broad exception caught to prevent engine crash during quarantine attempt
            logger.error("❌ [Engine] Failed to quarantine: %s", str(e))
            return source_path
        finally:
            conn.close()
