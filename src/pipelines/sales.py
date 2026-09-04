"""Sales data pipelines defined using the DataPipelineFactory."""

import os

from core.factory import DataPipelineFactory

# Paths to SQL files
SQL_DIR = os.path.join("src", "sql", "sales")

# --- 1. Bronze Layer (Landing -> Bronze) ---
sales_bronze_assets = DataPipelineFactory.create_asset(
    name="stg_sales_bronze",
    group_name="bronze",
    source={
        "path": "s3://landing-zone/sales_erp/dt={{ execution_date }}/sales.csv",
        "format": "csv",
    },
    target={
        "path": "s3://{{ group }}/sales_erp/dt={{ execution_date }}/{{ name }}{{ extension }}",
        "format": "parquet",
    },
    sql_transform=DataPipelineFactory.load_sql(os.path.join(SQL_DIR, "stg_sales_bronze.sql")),
    source_expectations=[
        {
            "expectation": "ExpectTableRowCountToBeBetween",
            "kwargs": {"min_value": 1, "max_value": 2000000},
        },
        {
            "expectation": "ExpectColumnValuesToNotBeNull",
            "kwargs": {"column": "customer_id"},
        },
    ],
    target_expectations=[
        {"expectation": "ExpectColumnValuesToNotBeNull", "kwargs": {"column": "id"}},
    ],
)

# --- 2. Silver Layer (Bronze -> Silver) ---
sales_silver_assets = DataPipelineFactory.create_asset(
    name="sales_silver",
    group_name="silver",
    depends_on=["stg_sales_bronze"],  # Point directly to bronze asset
    source={
        "path": "s3://bronze/sales_erp/dt={{ execution_date }}/stg_sales_bronze.parquet",
        "format": "parquet",
    },
    target={
        "path": "s3://{{ group }}/sales/dt={{ execution_date }}",
        "format": "delta",
    },
    sql_transform=DataPipelineFactory.load_sql(os.path.join(SQL_DIR, "sales_silver.sql")),
    target_expectations=[
        {
            "expectation": "ExpectColumnValuesToBeBetween",
            "kwargs": {"column": "price", "min_value": 0, "max_value": 10000},
        }
    ],
)
