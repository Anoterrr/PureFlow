"""Simple Business Dashboard using Streamlit and DuckDB (Reading from Gold S3)."""

import os

import duckdb
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from core.config import get_s3_connection_config

# Only load .env if variables are not already set (prevents overriding Docker env with localhost)
load_dotenv(override=False)

# --- Page Config ---
st.set_page_config(page_title="PureFlow BI Dashboard", layout="wide")
st.title("🌊 PureFlow-Arch: Gold Business Insights")
st.markdown("This dashboard reads directly from the **Gold Layer** (S3/MinIO) using DuckDB.")


# --- Database Setup ---
@st.cache_resource
def get_duckdb_conn():
    """Initializes and returns a DuckDB connection configured for S3/MinIO access."""
    conn = duckdb.connect(database=":memory:")

    # Configure S3/MinIO
    s3_cfg = get_s3_connection_config()

    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute(f"SET s3_endpoint = '{s3_cfg['s3_endpoint']}';")
    conn.execute(f"SET s3_access_key_id = '{s3_cfg['s3_access_key_id']}';")
    conn.execute(f"SET s3_secret_access_key = '{s3_cfg['s3_secret_access_key']}';")
    conn.execute(f"SET s3_use_ssl = {s3_cfg['s3_use_ssl']};")
    conn.execute(f"SET s3_url_style = '{s3_cfg['s3_url_style']}';")
    conn.execute(f"SET s3_region = '{s3_cfg['s3_region']}';")

    return conn


# --- Data Loading ---
def load_gold_data():
    """Loads gold sales summary data from S3 into a pandas DataFrame."""
    conn = get_duckdb_conn()
    # Path to your latest Gold aggregation
    # Note: In a real scenario, we would use a dynamic date
    base_date = os.getenv("EXECUTION_DATE", "2026-04-15")
    gold_path = f"s3://gold/sales_summary/dt={base_date}/sales_summary.parquet"

    try:
        return conn.execute("SELECT * FROM read_parquet(?)", [gold_path]).df()
    except Exception as e:
        st.error(f"Error loading gold data: {e}")
        return pd.DataFrame()


# --- Visualization ---
df = load_gold_data()

if not df.empty:
    # 1. Key Metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Revenue", f"$ {df['total_revenue'].sum():,.2f}")
    with col2:
        st.metric("Total Orders", f"{df['total_orders'].sum():,}")
    with col3:
        st.metric("Avg Ticket", f"$ {df['avg_ticket'].mean():,.2f}")

    st.divider()

    # 2. Charts
    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        st.subheader("Revenue by Region")
        st.bar_chart(df, x="region", y="total_revenue", color="#2E86C1")

    with chart_col2:
        st.subheader("Orders by Region")
        st.bar_chart(df, x="region", y="total_orders", color="#F39C12")

    st.subheader("Data Preview (Gold Layer)")
    st.dataframe(df, use_container_width=True)
else:
    st.warning("No data found in Gold Layer. Please run the Dagster pipeline first.")

if st.button("Refresh Data"):
    st.rerun()
