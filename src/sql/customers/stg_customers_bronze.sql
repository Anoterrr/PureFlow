-- Bronze transformation for Customers (Raw to Bronze)
SELECT
    id as customer_id,
    name,
    email,
    city,
    state
FROM source_data
