SELECT
    oi.order_item_id AS order_item_sk,
    oi.order_id,
    oi.product_id,
    oi.seller_id,
    oi.price,
    oi.freight_value,
    oi.price + oi.freight_value AS gross_item_value,
    1 AS item_count
FROM {{ ref('stg_order_items') }} oi
WHERE EXISTS (
    SELECT 1 FROM {{ ref('stg_orders') }} o
    WHERE o.order_id = oi.order_id
      AND o.order_status != 'TEST_STATUS'
)
