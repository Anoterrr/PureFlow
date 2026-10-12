"""Quarantine path logic (no I/O)."""

from src.core.engine import PureFlowEngine


def test_quarantine_path_keeps_the_source_bucket():
    engine = PureFlowEngine(execution_date="2026-01-15")
    result = engine.quarantine_path_for(
        "s3://landing-zone/sales_erp/dt=2026-01-15/sales.csv", reason="source_fail_sales"
    )
    assert result == (
        "s3://landing-zone/quarantine/dt=2026-01-15/reason=source_fail_sales/sales.csv"
    )


def test_quarantine_path_uses_the_engines_execution_date():
    engine = PureFlowEngine(execution_date="2026-03-01")
    result = engine.quarantine_path_for("s3://bronze/sales_erp/dt=2026-01-15/x.parquet", reason="r")
    assert "/dt=2026-03-01/" in result


def test_quarantine_path_handles_a_delta_directory_with_trailing_slash():
    engine = PureFlowEngine(execution_date="2026-01-15")
    result = engine.quarantine_path_for("s3://silver/sales/dt=2026-01-15/", reason="target_fail")
    assert result.endswith("/dt=2026-01-15")
    assert not result.endswith("//")


def test_quarantine_reason_is_slugified():
    engine = PureFlowEngine(execution_date="2026-01-15")
    result = engine.quarantine_path_for("s3://bucket/file.parquet", reason="two words")
    assert "reason=two_words" in result


def test_execution_date_defaults_to_base_date_when_unset():
    engine = PureFlowEngine()
    assert engine.execution_date
