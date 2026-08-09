SELECT
    order_id,
    product_id,
    seller_id,
    price,
    freight_value
FROM {{ source('silver_layer', 'silver_order_items') }}
-- Business Logic at Gold Layer: Filter out invalid negative prices
WHERE price > 0
