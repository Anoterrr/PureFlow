"""Module for generating synthetic dirty data in S3/MinIO to test validation gates."""

import pandas as pd

from core.config import BASE_DATE, get_s3_paths
from core.connection import ConnectionFactory
from core.logger import logger
from utils.generators import generate_base_customers, generate_base_sales


def generate_dirty_big_data(execution_date=None):
    """Generates dirty sales and customer data with intentional errors."""
    factory = ConnectionFactory()
    conn = factory.get_duckdb_conn()
    factory.setup_s3_auth(conn)

    base_date = execution_date or BASE_DATE
    s3_paths = get_s3_paths(base_date=base_date)

    # 1. Generate Dirty Customers (Null IDs)
    n_customers = 1000
    logger.info("🚀 Generating %d customers (DIRTY) for date %s...", n_customers, base_date)
    customers = generate_base_customers(n_customers)
    # created_at is an extra technical metadata field
    customers["created_at"] = [base_date] * n_customers
    df_customers = pd.DataFrame(customers)

    # Introduce Nulls in id
    df_customers.loc[0:10, "id"] = None

    logger.info(
        "📤 Writing dirty customers directly to Landing Zone: %s", s3_paths["customers_landing"]
    )
    conn.execute(
        f"COPY df_customers TO '{s3_paths['customers_landing']}' (FORMAT 'JSON', ARRAY TRUE)"
    )

    # 2. Generate Dirty Sales (Negative values, invalid dates)
    n_sales = 5000
    logger.info("🚀 Generating %d sales records (DIRTY)...", n_sales)
    sales = generate_base_sales(
        n_sales=n_sales,
        n_customers=n_customers,
        amount_range=(-500.0, 5000.0),  # Intentional negative values
        base_date=base_date,
    )
    df_sales = pd.DataFrame(sales)

    # Introduce Nulls in IDs
    df_sales.loc[0:50, "id"] = None

    logger.info("📤 Writing dirty sales directly to Landing Zone: %s", s3_paths["sales_landing"])
    conn.execute(f"COPY df_sales TO '{s3_paths['sales_landing']}' (FORMAT 'CSV', HEADER TRUE)")

    conn.close()
    logger.info("⚠️ Dirty Data generated successfully! Validation gates should catch this.")


if __name__ == "__main__":
    generate_dirty_big_data()
