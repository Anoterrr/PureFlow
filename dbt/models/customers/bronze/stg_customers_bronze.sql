{{ config(
    delta_table_path="s3://" ~ var('bronze_bucket') ~ "/customers_crm/dt=" ~ var('execution_date') ~ "/stg_customers_bronze",
    location="s3://" ~ var('bronze_bucket') ~ "/_stage/stg_customers_bronze/dt=" ~ var('execution_date') ~ ".parquet"
) }}

SELECT
    id AS customer_id,
    name,
    email,
    city,
    state
FROM {{ source('landing', 'customers_landing') }}
