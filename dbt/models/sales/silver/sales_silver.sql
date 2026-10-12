{{ config(
    delta_table_path="s3://" ~ var('silver_bucket') ~ "/sales/dt=" ~ var('execution_date'),
    location="s3://" ~ var('silver_bucket') ~ "/_stage/sales_silver/dt=" ~ var('execution_date') ~ ".parquet"
) }}

SELECT
    *,
    (price * 0.9) AS price_with_discount,
    CURRENT_TIMESTAMP AS silver_processed_at
FROM {{ ref('stg_sales_bronze') }}
WHERE price > 0
