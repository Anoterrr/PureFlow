-- Transformation from Bronze to Silver for Customers
SELECT
    *,
    UPPER(state) as uf,
    CURRENT_TIMESTAMP as silver_processed_at
FROM source_data
