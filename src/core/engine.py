"""Core Engine: quarantine handling shared across pipeline gates.

Bronze/silver/gold transformation itself lives in dbt models (see dbt/models/);
this engine only backs the quality-gate assets in orchestration.py.
"""

from core.config import get_base_date
from core.connection import ConnectionFactory
from core.logger import logger


class PureFlowEngine:
    """Technically executes data movement, transformation and validation tasks."""

    def __init__(self, execution_date: str = None):
        self.execution_date = execution_date or get_base_date()
        self.factory = ConnectionFactory()

    def quarantine_path_for(self, source_path: str, reason: str) -> str:
        """Builds the quarantine URI for a source path. Pure string work, no I/O.

        Quarantine is a prefix inside the *source* bucket, not a bucket of its
        own, so the bad data stays next to where it landed.
        """
        path_parts = source_path.replace("s3://", "").split("/")
        bucket = path_parts[0]
        # Trailing slash: a Delta table is a directory, so the last segment is empty
        filename = path_parts[-1] or path_parts[-2]
        prefix = f"quarantine/dt={self.execution_date}/reason={reason.replace(' ', '_')}"
        return f"s3://{bucket}/{prefix}/{filename}"

    def quarantine_data(
        self, source_path: str, reason: str, source_format: str = "parquet"
    ) -> str | None:
        """Copies failing data to a quarantine prefix in S3.

        Returns the quarantine path, or None if the copy failed. It is a copy,
        not a move: the source is left in place on purpose so the bad data can
        still be inspected where it landed.

        Returning None rather than the source path matters. Handing the original
        path back made a failed quarantine indistinguishable from a successful
        one, so callers logged "Quarantined to <the file that never moved>".
        """
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

            # read_func is from the fixed whitelist above, and both paths are
            # inlined rather than bound as `?`, because a COPY with a placeholder both
            # inside the SELECT and in the TO clause silently mis-binds on this
            # DuckDB version. Neither path is ever raw user input.
            conn.execute(
                f"COPY (SELECT * FROM {read_func}('{source_path}')) "  # nosec B608
                f"TO '{target_quarantine_path}' (FORMAT 'PARQUET')"
            )

            return target_quarantine_path
        except Exception as e:
            # Broad exception caught to prevent engine crash during quarantine
            # attempt; the None return is what tells the caller it did not happen.
            logger.error("❌ [Engine] Failed to quarantine %s: %s", source_path, str(e))
            return None
        finally:
            conn.close()
