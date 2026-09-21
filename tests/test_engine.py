"""Unit tests for PureFlowEngine's pure quarantine-path logic (no S3/DuckDB I/O).

Replaces the old render_path tests. render_path was removed because nothing
ever reached it with a template: every caller passes an already-concrete S3 URI
built by get_s3_paths(), so the substitution was a no-op and four of the five
tests here were exercising a code path production never took.
"""

from src.core.engine import PureFlowEngine


def test_quarantine_path_keeps_the_source_bucket():
    """Quarantine is a prefix inside the source bucket, not a bucket of its own."""
    engine = PureFlowEngine(execution_date="2026-01-15")
    result = engine.quarantine_path_for(
        "s3://landing-zone/sales_erp/dt=2026-01-15/sales.csv", reason="source_fail_sales"
    )
    assert result == (
        "s3://landing-zone/quarantine/dt=2026-01-15/reason=source_fail_sales/sales.csv"
    )


def test_quarantine_path_uses_the_engines_execution_date():
    """The dt= partition comes from the run, not from the source path."""
    engine = PureFlowEngine(execution_date="2026-03-01")
    result = engine.quarantine_path_for("s3://bronze/sales_erp/dt=2026-01-15/x.parquet", reason="r")
    assert "/dt=2026-03-01/" in result


def test_quarantine_path_handles_a_delta_directory_with_trailing_slash():
    """A Delta table is a directory; a trailing slash must not yield an empty name."""
    engine = PureFlowEngine(execution_date="2026-01-15")
    result = engine.quarantine_path_for("s3://silver/sales/dt=2026-01-15/", reason="target_fail")
    assert result.endswith("/dt=2026-01-15")
    assert not result.endswith("//")


def test_quarantine_reason_is_slugified():
    """Spaces would produce an unusable S3 prefix."""
    engine = PureFlowEngine(execution_date="2026-01-15")
    result = engine.quarantine_path_for("s3://bucket/file.parquet", reason="two words")
    assert "reason=two_words" in result


def test_execution_date_defaults_to_base_date_when_unset():
    engine = PureFlowEngine()
    assert engine.execution_date  # falls back to core.config.BASE_DATE, never empty
