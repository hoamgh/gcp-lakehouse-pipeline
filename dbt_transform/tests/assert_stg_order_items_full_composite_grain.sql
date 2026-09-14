SELECT
    order_id,
    product_id,
    seller_id,
    price,
    freight_value,
    COUNT(*) AS row_count
FROM {{ ref('stg_order_items') }}
GROUP BY order_id, product_id, seller_id, price, freight_value
HAVING COUNT(*) > 1
