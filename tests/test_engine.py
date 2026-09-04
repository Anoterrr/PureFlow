"""Unit tests for PureFlowEngine's pure path-rendering logic (no S3/DuckDB I/O)."""

from src.core.engine import PureFlowEngine


def test_render_path_substitutes_execution_date():
    engine = PureFlowEngine(execution_date="2026-01-15")
    result = engine.render_path("s3://bucket/dt={{ execution_date }}/file.csv")
    assert result == "s3://bucket/dt=2026-01-15/file.csv"


def test_render_path_substitutes_context_fields():
    engine = PureFlowEngine(execution_date="2026-01-15")
    context = {"name": "stg_sales_bronze", "group": "bronze", "format": "parquet"}
    result = engine.render_path(
        "s3://{{ group }}/dt={{ execution_date }}/{{ name }}{{ extension }}",
        context=context,
    )
    assert result == "s3://bronze/dt=2026-01-15/stg_sales_bronze.parquet"


def test_render_path_extension_mapping():
    engine = PureFlowEngine(execution_date="2026-01-15")
    for fmt, expected_ext in [("csv", ".csv"), ("json", ".json"), ("parquet", ".parquet")]:
        result = engine.render_path("s3://bucket/file{{ extension }}", context={"format": fmt})
        assert result == f"s3://bucket/file{expected_ext}"


def test_render_path_delta_has_no_extension():
    engine = PureFlowEngine(execution_date="2026-01-15")
    result = engine.render_path("s3://bucket/dir{{ extension }}", context={"format": "delta"})
    assert result == "s3://bucket/dir"


def test_render_path_defaults_to_base_date_when_unset():
    engine = PureFlowEngine()
    assert engine.execution_date  # falls back to core.config.BASE_DATE, never empty
