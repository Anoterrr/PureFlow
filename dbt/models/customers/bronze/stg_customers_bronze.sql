{{ config(
    delta_table_path="s3://bronze/customers_crm/dt=" ~ var('execution_date') ~ "/stg_customers_bronze",
    location="s3://bronze/_stage/stg_customers_bronze/dt=" ~ var('execution_date') ~ ".parquet"
) }}

-- Bronze transformation for Customers (Raw to Bronze)
SELECT
    id AS customer_id,
    name,
    email,
    city,
    state
FROM {{ source('landing', 'customers_landing') }}
