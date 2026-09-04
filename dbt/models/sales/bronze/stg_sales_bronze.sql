{{ config(
    delta_table_path="s3://bronze/sales_erp/dt=" ~ var('execution_date') ~ "/stg_sales_bronze",
    location="s3://bronze/_stage/stg_sales_bronze/dt=" ~ var('execution_date') ~ ".parquet"
) }}

-- Bronze transformation for Sales (Raw to Bronze)
SELECT
    id,
    customer_id,
    product,
    CAST(price AS DOUBLE) AS price,
    CAST(date AS DATE) AS sale_date
FROM {{ source('landing', 'sales_landing') }}
