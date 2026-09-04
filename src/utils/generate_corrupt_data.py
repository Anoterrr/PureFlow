"""Module for generating intentionally corrupt data at different layers in S3/MinIO."""

from core.config import BASE_DATE, get_s3_paths
from core.connection import ConnectionFactory
from core.logger import logger


def corrupt_landing_zone(execution_date=None):
    """Corrupts data in the Landing Zone (CSV/JSON)."""
    factory = ConnectionFactory()
    conn = factory.get_duckdb_conn()
    factory.setup_s3_auth(conn)

    base_date = execution_date or BASE_DATE
    s3_paths = get_s3_paths(base_date=base_date)

    logger.warning("🧨 [Corruptor] Corrupting Landing Zone data for date %s...", base_date)

    # 1. Corrupt Sales (CSV) - Inject Null IDs and Negative Prices
    # Column names must match what stg_sales_bronze.sql expects:
    # id, customer_id, product, price, date
    conn.execute(
        """
        COPY (
            SELECT
                CASE WHEN range % 10 = 0 THEN NULL ELSE range END as id,
                CASE WHEN range % 15 = 0 THEN -100.0 ELSE 50.0 END as price,
                '2024-01-01' as date,
                1 as customer_id,
                'Produto Corrompido' as product
            FROM range(1, 101)
        ) TO ? (FORMAT 'CSV', HEADER TRUE)
        """,
        [s3_paths["sales_landing"]],
    )

    # 2. Corrupt Customers (JSON) - Invalid Emails
    # Column names must match what stg_customers_bronze.sql expects: id, name, email, city, state
    conn.execute(
        """
        COPY (
            SELECT
                range as id,
                'invalid-email-' || range as email,
                'User ' || range as name,
                'São Paulo' as city,
                'SP' as state,
                ? as created_at
            FROM range(1, 51)
        ) TO ? (FORMAT 'JSON', ARRAY TRUE)
        """,
        [base_date, s3_paths["customers_landing"]],
    )

    conn.close()
    logger.info("✅ Landing Zone corrupted.")


def corrupt_bronze_layer(execution_date=None):
    """Injects bad data directly into Bronze Parquet files."""
    factory = ConnectionFactory()
    conn = factory.get_duckdb_conn()
    factory.setup_s3_auth(conn)

    base_date = execution_date or BASE_DATE
    s3_paths = get_s3_paths(base_date=base_date)

    logger.warning("🧨 [Corruptor] Corrupting Bronze Layer for date %s...", base_date)

    # Inject sales with NULL product names in Bronze
    # Bronze format: id, customer_id, product, price, sale_date (casted from date)
    conn.execute(
        """
        COPY (
            SELECT
                range as id,
                101 as customer_id,
                CASE WHEN range % 5 = 0 THEN NULL ELSE 'Fake Product' END as product,
                -10.5 as price,
                CAST('1900-01-01' AS DATE) as sale_date,
                now() as _ingested_at,
                'sales' as _domain,
                'corrupted_ingest.csv' as _source_file
            FROM range(1000, 1050)
        ) TO ? (FORMAT 'PARQUET')
        """,
        [s3_paths["sales_bronze"]],
    )

    # Inject customers with null customer_id in Bronze
    # Bronze format: customer_id, name, email, city, state
    conn.execute(
        """
        COPY (
            SELECT
                CASE WHEN range % 3 = 0 THEN NULL ELSE range END as customer_id,
                'Corrupted User' as name,
                'corrupted@mail.com' as email,
                'Rio de Janeiro' as city,
                'RJ' as state,
                now() as _ingested_at,
                'customers' as _domain,
                'corrupted_ingest.json' as _source_file
            FROM range(2000, 2050)
        ) TO ? (FORMAT 'PARQUET')
        """,
        [s3_paths["customers_bronze"]],
    )

    conn.close()
    logger.info("✅ Bronze Layer corrupted.")


def corrupt_silver_layer(execution_date=None):
    """Injects bad data directly into Silver Delta tables to test Gold gates."""
    factory = ConnectionFactory()
    conn = factory.get_duckdb_conn()
    factory.setup_s3_auth(conn)

    base_date = execution_date or BASE_DATE
    s3_paths = get_s3_paths(base_date=base_date)

    logger.warning("🧨 [Corruptor] Corrupting Silver Layer (DELTA) for date %s...", base_date)

    # Corrupt Sales Silver (Delta) - Extreme prices
    # Silver format: id, customer_id, product, price, sale_date
    conn.execute(
        """
        COPY (
            SELECT
                range as id,
                1 as customer_id,
                'Silver Corruption' as product,
                999999.99 as price,
                CAST('2026-04-20' AS DATE) as sale_date,
                now() as _processed_at
            FROM range(5000, 5010)
        ) TO ? (FORMAT 'DELTA')
        """,
        [s3_paths["sales_silver"]],
    )

    conn.close()
    logger.info("✅ Silver Layer corrupted.")


if __name__ == "__main__":
    corrupt_landing_zone()
    corrupt_bronze_layer()
    corrupt_silver_layer()
