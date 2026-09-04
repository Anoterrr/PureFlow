-- Transformation from Bronze to Silver for Sales
SELECT
    *,
    (price * 0.9) as price_with_discount,
    CURRENT_TIMESTAMP as silver_processed_at
FROM source_data
WHERE price > 0
