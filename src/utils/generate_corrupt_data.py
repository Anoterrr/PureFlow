"""Module for generating intentionally corrupt data at different layers in S3/MinIO."""

from deltalake import write_deltalake

from core.config import BASE_DATE, get_delta_storage_options, get_s3_paths
from core.connection import ConnectionFactory
from core.logger import logger


def corrupt_landing_zone(execution_date=None):
    """Corrupts data in the Landing Zone (CSV/JSON)."""
    factory = ConnectionFactory()
    conn = factory.get_duckdb_conn(db_path=":memory:")
    factory.setup_s3_auth(conn)

    base_date = execution_date or BASE_DATE
    s3_paths = get_s3_paths(base_date=base_date)

    logger.warning("🧨 [Corruptor] Corrupting Landing Zone data for date %s...", base_date)

    # 1. Corrupt Sales (CSV) - Inject Null IDs and Negative Prices
    # Column names must match what stg_sales_bronze expects:
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
    # Column names must match what stg_customers_bronze expects: id, name, email, city, state
    # NOTE: base_date is inlined (not bound as `?`) — a COPY with a placeholder
    # both inside the SELECT and in the TO clause silently writes nothing on
    # this DuckDB version (no error raised); base_date is an internal value,
    # never user input, so inlining it here is safe.
    conn.execute(
        f"""
        COPY (
            SELECT
                range as id,
                'invalid-email-' || range as email,
                'User ' || range as name,
                'São Paulo' as city,
                'SP' as state,
                '{base_date}' as created_at
            FROM range(1, 51)
        ) TO ? (FORMAT 'JSON', ARRAY TRUE)
        """,  # nosec B608
        [s3_paths["customers_landing"]],
    )

    conn.close()
    logger.info("✅ Landing Zone corrupted.")


def _write_corrupt_delta(conn, query: str, delta_table_path: str) -> None:
    """Runs `query` and overwrites a Delta table with the result (bronze/silver are Delta now)."""
    table = conn.execute(query).fetch_arrow_table()
    write_deltalake(
        delta_table_path,
        table,
        mode="overwrite",
        schema_mode="overwrite",
        storage_options=get_delta_storage_options(),
    )


def corrupt_bronze_layer(execution_date=None):
    """Overwrites the Bronze Delta tables directly with bad data."""
    factory = ConnectionFactory()
    conn = factory.get_duckdb_conn(db_path=":memory:")
    factory.setup_s3_auth(conn)

    base_date = execution_date or BASE_DATE
    s3_paths = get_s3_paths(base_date=base_date)

    logger.warning("🧨 [Corruptor] Corrupting Bronze Layer for date %s...", base_date)

    try:
        # Inject sales with NULL product names in Bronze
        # Bronze format: id, customer_id, product, price, sale_date (casted from date)
        _write_corrupt_delta(
            conn,
            """
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
            """,
            s3_paths["sales_bronze"],
        )

        # Inject customers with null customer_id in Bronze
        # Bronze format: customer_id, name, email, city, state
        _write_corrupt_delta(
            conn,
            """
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
            """,
            s3_paths["customers_bronze"],
        )
    finally:
        conn.close()

    logger.info("✅ Bronze Layer corrupted.")


def corrupt_silver_layer(execution_date=None):
    """Overwrites the Sales Silver Delta table directly with bad data, to test Gold gates."""
    factory = ConnectionFactory()
    conn = factory.get_duckdb_conn(db_path=":memory:")
    factory.setup_s3_auth(conn)

    base_date = execution_date or BASE_DATE
    s3_paths = get_s3_paths(base_date=base_date)

    logger.warning("🧨 [Corruptor] Corrupting Silver Layer (DELTA) for date %s...", base_date)

    try:
        # Corrupt Sales Silver (Delta) - Extreme prices
        # Silver format: id, customer_id, product, price, sale_date
        _write_corrupt_delta(
            conn,
            """
            SELECT
                range as id,
                1 as customer_id,
                'Silver Corruption' as product,
                999999.99 as price,
                CAST('2026-04-20' AS DATE) as sale_date,
                now() as _processed_at
            FROM range(5000, 5010)
            """,
            s3_paths["sales_silver"],
        )
    finally:
        conn.close()

    logger.info("✅ Silver Layer corrupted.")


if __name__ == "__main__":
    corrupt_landing_zone()
    corrupt_bronze_layer()
    corrupt_silver_layer()
