WITH source_data AS (
SELECT
    order_item_id,
    order_id,
    product_id,
    seller_id,
    price,
    freight_value
FROM {{ source('silver_layer', 'silver_order_items') }}
)

SELECT *
FROM source_data
-- Legacy rows predate the source order_item_id and must remain available.
-- Their declared grain is the complete five-column business composite.
WHERE order_id IS NOT NULL
  AND product_id IS NOT NULL
  AND seller_id IS NOT NULL
  AND price > 0
