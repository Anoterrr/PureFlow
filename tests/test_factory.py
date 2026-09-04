"""Unit tests for the DataPipelineFactory contract (asset wiring, no execution)."""

from dagster import AssetKey
from src.core.factory import DataPipelineFactory


def _dummy_source_target():
    return (
        {"path": "s3://landing/dt={{ execution_date }}/file.csv", "format": "csv"},
        {"path": "s3://bronze/{{ name }}{{ extension }}", "format": "parquet"},
    )


def test_create_asset_returns_correctly_keyed_asset():
    source, target = _dummy_source_target()
    assets = DataPipelineFactory.create_asset(
        name="stg_test_bronze", source=source, target=target, group_name="bronze"
    )

    assert len(assets) == 1
    (asset_def,) = assets
    assert asset_def.key == AssetKey("stg_test_bronze")
    assert asset_def.group_names_by_key[AssetKey("stg_test_bronze")] == "bronze"


def test_create_asset_wires_dependencies():
    source, target = _dummy_source_target()
    assets = DataPipelineFactory.create_asset(
        name="silver_test",
        source=source,
        target=target,
        group_name="silver",
        depends_on=["stg_test_bronze"],
    )

    (asset_def,) = assets
    upstream = asset_def.asset_deps[AssetKey("silver_test")]
    assert AssetKey("stg_test_bronze") in upstream


def test_load_sql_reads_file_contents(tmp_path):
    sql_file = tmp_path / "transform.sql"
    sql_file.write_text("SELECT * FROM source_data", encoding="utf-8")

    content = DataPipelineFactory.load_sql(str(sql_file))

    assert content == "SELECT * FROM source_data"
