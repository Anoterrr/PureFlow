"""Quarantine handling for the quality gates."""

from core.config import get_base_date
from core.connection import ConnectionFactory
from core.logger import logger


class PureFlowEngine:
    def __init__(self, execution_date: str = None):
        self.execution_date = execution_date or get_base_date()
        self.factory = ConnectionFactory()

    def quarantine_path_for(self, source_path: str, reason: str) -> str:
        """Quarantine lives as a prefix inside the source bucket."""
        path_parts = source_path.replace("s3://", "").split("/")
        bucket = path_parts[0]
        # A Delta table path may end with "/", leaving the last segment empty.
        filename = path_parts[-1] or path_parts[-2]
        prefix = f"quarantine/dt={self.execution_date}/reason={reason.replace(' ', '_')}"
        return f"s3://{bucket}/{prefix}/{filename}"

    def quarantine_data(
        self, source_path: str, reason: str, source_format: str = "parquet"
    ) -> str | None:
        """Copies (never moves) failing data to quarantine. Returns the path, or None on failure."""
        target_quarantine_path = self.quarantine_path_for(source_path, reason)

        conn = self.factory.get_duckdb_conn(db_path=":memory:")
        self.factory.setup_s3_auth(conn)

        try:
            logger.warning(
                "🛡️ [Engine] Quarantining data: %s -> %s",
                source_path,
                target_quarantine_path,
            )

            fmt = source_format.lower()
            if fmt == "delta":
                read_func = "delta_scan"
            elif fmt in ["csv", "json"]:
                read_func = f"read_{fmt}_auto"
            else:
                read_func = "read_parquet"

            # Paths are inlined: DuckDB mis-binds `?` used in both the SELECT and TO clauses.
            conn.execute(
                f"COPY (SELECT * FROM {read_func}('{source_path}')) "  # nosec B608
                f"TO '{target_quarantine_path}' (FORMAT 'PARQUET')"
            )

            return target_quarantine_path
        except Exception as e:
            logger.error("❌ [Engine] Failed to quarantine %s: %s", source_path, str(e))
            return None
        finally:
            conn.close()
