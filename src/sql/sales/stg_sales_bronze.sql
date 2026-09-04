-- Bronze transformation for Sales (Raw to Bronze)
SELECT
    id,
    customer_id,
    product,
    CAST(price AS DOUBLE) as price,
    CAST(date AS DATE) as sale_date
FROM source_data
