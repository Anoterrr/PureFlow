"""Simple Business Dashboard using Streamlit and DuckDB (Reading from Gold S3)."""

import re

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from core.connection import ConnectionFactory

# Only load .env if variables are not already set (prevents overriding Docker env with localhost)
load_dotenv(override=False)

st.set_page_config(page_title="PureFlow BI Dashboard", layout="wide")
st.title("🌊 PureFlow: Gold Business Insights")
st.markdown("This dashboard reads directly from the **Gold Layer** (S3/MinIO) using DuckDB.")


@st.cache_resource
def get_duckdb_conn():
    """Initializes and returns a DuckDB connection configured for S3/MinIO access.

    Reuses ConnectionFactory (core/connection.py) rather than hand-rolling S3
    setup — its CREATE SECRET-based auth is what the delta extension actually
    needs (plain SET s3_access_key_id/etc. isn't enough for delta_scan()).
    """
    factory = ConnectionFactory()
    conn = factory.get_duckdb_conn(db_path=":memory:")
    factory.setup_s3_auth(conn)
    return conn


def get_latest_partition_date(conn, bucket: str, table: str) -> str | None:
    """Finds the most recent dt=YYYY-MM-DD partition written for a Gold Delta table.

    No date is guessed or hardcoded: whatever the Dagster pipeline last wrote
    is what gets shown, regardless of when that run happened. Looks for the
    Delta transaction log rather than a bare .parquet file since Gold is Delta.
    """
    files = conn.execute(
        "SELECT file FROM glob(?)", [f"s3://{bucket}/{table}/dt=*/_delta_log/*.json"]
    ).fetchall()
    dates = {m.group(1) for (f,) in files if (m := re.search(r"dt=(\d{4}-\d{2}-\d{2})", f))}
    return max(dates) if dates else None


def load_gold_data():
    """Loads the most recently written Gold sales summary Delta partition."""
    conn = get_duckdb_conn()

    latest_date = get_latest_partition_date(conn, "gold", "sales_summary")
    if latest_date is None:
        return pd.DataFrame(), None

    gold_path = f"s3://gold/sales_summary/dt={latest_date}/"
    try:
        return conn.execute("SELECT * FROM delta_scan(?)", [gold_path]).df(), latest_date
    except Exception as e:
        st.error(f"Error loading gold data: {e}")
        return pd.DataFrame(), latest_date


df, latest_run_date = load_gold_data()

if not df.empty:
    st.caption(f"📅 Showing latest available run: **{latest_run_date}**")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Revenue", f"$ {df['total_revenue'].sum():,.2f}")
    with col2:
        st.metric("Total Orders", f"{df['total_orders'].sum():,}")
    with col3:
        st.metric("Avg Ticket", f"$ {df['avg_ticket'].mean():,.2f}")

    st.divider()

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
