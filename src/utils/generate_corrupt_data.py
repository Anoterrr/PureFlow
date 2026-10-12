"""Corruptors used by the quality demo jobs to prove the gates fire."""

from deltalake import write_deltalake

from core.config import get_base_date, get_delta_storage_options, get_s3_paths
from core.connection import ConnectionFactory
from core.logger import logger


def corrupt_landing_zone(execution_date=None):
    """Null ids and negative prices in sales, invalid e-mails in customers."""
    factory = ConnectionFactory()
    conn = factory.get_duckdb_conn(db_path=":memory:")
    factory.setup_s3_auth(conn)

    base_date = execution_date or get_base_date()
    s3_paths = get_s3_paths(base_date=base_date)

    logger.warning("🧨 [Corruptor] Corrupting Landing Zone data for date %s...", base_date)

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

    # base_date is inlined: DuckDB silently writes nothing when `?` is used in both SELECT and TO.
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
    """Overwrites a Delta table with the result of `query`."""
    table = conn.execute(query).fetch_arrow_table()
    write_deltalake(
        delta_table_path,
        table,
        mode="overwrite",
        schema_mode="overwrite",
        storage_options=get_delta_storage_options(),
    )


def corrupt_bronze_layer(execution_date=None):
    """Null products and negative prices in sales, null customer_id in customers."""
    factory = ConnectionFactory()
    conn = factory.get_duckdb_conn(db_path=":memory:")
    factory.setup_s3_auth(conn)

    base_date = execution_date or get_base_date()
    s3_paths = get_s3_paths(base_date=base_date)

    logger.warning("🧨 [Corruptor] Corrupting Bronze Layer for date %s...", base_date)

    try:
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


if __name__ == "__main__":
    corrupt_landing_zone()
    corrupt_bronze_layer()
