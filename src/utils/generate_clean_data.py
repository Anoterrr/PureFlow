"""Module for generating synthetic clean big data in S3/MinIO."""

import pandas as pd

from core.config import BASE_DATE, get_s3_paths
from core.connection import ConnectionFactory
from core.logger import logger
from utils.generators import generate_base_customers, generate_base_sales


def generate_clean_big_data(execution_date=None):
    """Generates clean sales and customer data and writes directly to MinIO."""
    # 1. Initialize Connection
    factory = ConnectionFactory()
    conn = factory.get_duckdb_conn(db_path=":memory:")
    factory.setup_s3_auth(conn)

    # Use provided execution_date or fallback to global/env
    base_date = execution_date or BASE_DATE
    s3_paths = get_s3_paths(base_date=base_date)

    # 2. Generate clean customers_crm (Big Data Scale)
    n_customers = 100_000
    logger.info("🚀 Generating %d customers (CLEAN) for date %s...", n_customers, base_date)
    customers = generate_base_customers(n_customers)
    # created_at is an extra technical metadata field
    customers["created_at"] = [base_date] * n_customers

    # DuckDB looks this up by variable name from the local scope in the COPY below
    df_customers = pd.DataFrame(customers)  # noqa: F841

    logger.info(
        "📤 Writing customers directly to Landing Zone: %s",
        s3_paths["customers_landing"],
    )
    conn.execute(
        f"COPY df_customers TO '{s3_paths['customers_landing']}' (FORMAT 'JSON', ARRAY TRUE)"
    )

    # 3. Generate clean sales_erp (Big Data Scale)
    n_sales = 1_000_000
    logger.info("🚀 Generating %d sales records (CLEAN)...", n_sales)
    sales = generate_base_sales(
        n_sales=n_sales,
        n_customers=n_customers,
        amount_range=(10.0, 5000.0),
        base_date=base_date,
    )
    # DuckDB looks this up by variable name from the local scope in the COPY below
    df_sales = pd.DataFrame(sales)  # noqa: F841

    logger.info("📤 Writing sales directly to Landing Zone: %s", s3_paths["sales_landing"])
    conn.execute(f"COPY df_sales TO '{s3_paths['sales_landing']}' (FORMAT 'CSV', HEADER TRUE)")

    conn.close()
    logger.info("✅ Clean Big Data generated and written to MinIO successfully!")


if __name__ == "__main__":
    generate_clean_big_data()
