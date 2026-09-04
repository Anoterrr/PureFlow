"""Customers data pipelines defined using the DataPipelineFactory."""

import os

from core.factory import DataPipelineFactory

# Paths to SQL files
SQL_DIR = os.path.join("src", "sql", "customers")

# --- 1. Bronze Layer (Landing -> Bronze) ---
customers_bronze_assets = DataPipelineFactory.create_asset(
    name="stg_customers_bronze",
    group_name="bronze",
    source={
        "path": "s3://landing-zone/customers_crm/dt={{ execution_date }}/customers.json",
        "format": "json",
    },
    target={
        "path": "s3://{{ group }}/customers_crm/dt={{ execution_date }}/{{ name }}{{ extension }}",
        "format": "parquet",
    },
    sql_transform=DataPipelineFactory.load_sql(os.path.join(SQL_DIR, "stg_customers_bronze.sql")),
    source_expectations=[
        {
            "expectation": "ExpectColumnValuesToNotBeNull",
            "kwargs": {"column": "id"},
        },
    ],
    target_expectations=[
        {
            "expectation": "ExpectColumnValuesToMatchRegex",
            "kwargs": {"column": "email", "regex": r"[^@]+@[^@]+\.[^@]+"},
        },
    ],
)

# --- 2. Silver Layer (Bronze -> Silver) ---
customers_silver_assets = DataPipelineFactory.create_asset(
    name="customers_silver",
    group_name="silver",
    depends_on=["stg_customers_bronze"],  # Point directly to bronze asset
    source={
        "path": "s3://bronze/customers_crm/dt={{ execution_date }}/stg_customers_bronze.parquet",
        "format": "parquet",
    },
    target={
        "path": "s3://{{ group }}/customers/dt={{ execution_date }}",
        "format": "delta",
    },
    sql_transform=DataPipelineFactory.load_sql(os.path.join(SQL_DIR, "customers_silver.sql")),
    target_expectations=[
        {
            "expectation": "ExpectColumnValuesToNotBeNull",
            "kwargs": {"column": "email"},
        },
    ],
)
