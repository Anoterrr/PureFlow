{{ config(
    delta_table_path="s3://gold/sales_summary/dt=" ~ var('execution_date'),
    location="s3://gold/_stage/sales_summary/dt=" ~ var('execution_date') ~ ".parquet"
) }}

WITH silver_sales AS (
    SELECT * FROM {{ ref('sales_silver') }}
),

silver_customers AS (
    SELECT * FROM {{ ref('customers_silver') }}
),

enriched_sales AS (
    SELECT
        v.id,
        v.price AS sale_value,
        v.sale_date,
        c.name AS customer_name,
        c.uf AS region,
        date_trunc('month', v.sale_date::DATE) AS sale_month
    FROM silver_sales AS v
    LEFT JOIN silver_customers AS c ON v.customer_id = c.customer_id
)

SELECT
    region,
    sale_month,
    sum(sale_value) AS total_revenue,
    count(id) AS total_orders,
    round(avg(sale_value), 2) AS avg_ticket
FROM enriched_sales
GROUP BY region, sale_month
ORDER BY total_revenue DESC
