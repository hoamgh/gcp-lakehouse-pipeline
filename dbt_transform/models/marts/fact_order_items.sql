SELECT
    oi.order_item_id,
    CONCAT(
        CAST(FARM_FINGERPRINT(CONCAT(
            COALESCE(oi.order_id, ''), '|',
            COALESCE(oi.product_id, ''), '|',
            COALESCE(oi.seller_id, ''), '|',
            COALESCE(CAST(oi.price AS STRING), ''), '|',
            COALESCE(CAST(oi.freight_value AS STRING), '')
        )) AS STRING),
        '-1'
    ) AS order_item_sk,
    oi.order_id,
    oi.product_id,
    p.product_sk,
    oi.seller_id,
    s.seller_sk,
    oi.price,
    oi.freight_value,
    oi.price + oi.freight_value AS gross_item_value,
    1 AS item_count
FROM {{ ref('stg_order_items') }} oi
INNER JOIN {{ ref('dim_products') }} p ON oi.product_id = p.product_id
INNER JOIN {{ ref('dim_sellers') }} s ON oi.seller_id = s.seller_id
WHERE EXISTS (
    SELECT 1 FROM {{ ref('stg_orders') }} o
    WHERE o.order_id = oi.order_id
      AND o.order_status != 'TEST_STATUS'
)
