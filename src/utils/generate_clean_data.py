"""Writes a clean synthetic landing zone (1M sales, 100k customers) to S3."""

import pandas as pd

from core.config import get_base_date, get_s3_paths
from core.connection import ConnectionFactory
from core.logger import logger
from utils.generators import generate_base_customers, generate_base_sales


def generate_clean_big_data(execution_date=None):
    factory = ConnectionFactory()
    conn = factory.get_duckdb_conn(db_path=":memory:")
    factory.setup_s3_auth(conn)

    base_date = execution_date or get_base_date()
    s3_paths = get_s3_paths(base_date=base_date)

    n_customers = 100_000
    logger.info("🚀 Generating %d customers (CLEAN) for date %s...", n_customers, base_date)
    customers = generate_base_customers(n_customers)
    customers["created_at"] = [base_date] * n_customers

    # Referenced by name in the COPY below (DuckDB replacement scan).
    df_customers = pd.DataFrame(customers)  # noqa: F841

    logger.info(
        "📤 Writing customers directly to Landing Zone: %s",
        s3_paths["customers_landing"],
    )
    conn.execute(
        f"COPY df_customers TO '{s3_paths['customers_landing']}' (FORMAT 'JSON', ARRAY TRUE)"
    )

    n_sales = 1_000_000
    logger.info("🚀 Generating %d sales records (CLEAN)...", n_sales)
    sales = generate_base_sales(
        n_sales=n_sales,
        n_customers=n_customers,
        amount_range=(10.0, 5000.0),
        base_date=base_date,
    )
    df_sales = pd.DataFrame(sales)  # noqa: F841

    logger.info("📤 Writing sales directly to Landing Zone: %s", s3_paths["sales_landing"])
    conn.execute(f"COPY df_sales TO '{s3_paths['sales_landing']}' (FORMAT 'CSV', HEADER TRUE)")

    conn.close()
    logger.info("✅ Clean Big Data generated and written to S3 successfully!")


if __name__ == "__main__":
    generate_clean_big_data()
