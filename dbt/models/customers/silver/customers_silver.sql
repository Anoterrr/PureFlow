{{ config(
    delta_table_path="s3://silver/customers/dt=" ~ var('execution_date'),
    location="s3://silver/_stage/customers_silver/dt=" ~ var('execution_date') ~ ".parquet"
) }}

-- Transformation from Bronze to Silver for Customers
SELECT
    *,
    UPPER(state) AS uf,
    CURRENT_TIMESTAMP AS silver_processed_at
FROM {{ ref('stg_customers_bronze') }}
