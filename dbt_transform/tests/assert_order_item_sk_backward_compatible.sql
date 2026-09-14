SELECT
    order_item_sk,
    CONCAT(
        CAST(FARM_FINGERPRINT(CONCAT(
            COALESCE(order_id, ''), '|',
            COALESCE(product_id, ''), '|',
            COALESCE(seller_id, ''), '|',
            COALESCE(CAST(price AS STRING), ''), '|',
            COALESCE(CAST(freight_value AS STRING), '')
        )) AS STRING),
        '-1'
    ) AS expected_order_item_sk
FROM {{ ref('fact_order_items') }}
WHERE order_item_sk != CONCAT(
    CAST(FARM_FINGERPRINT(CONCAT(
        COALESCE(order_id, ''), '|',
        COALESCE(product_id, ''), '|',
        COALESCE(seller_id, ''), '|',
        COALESCE(CAST(price AS STRING), ''), '|',
        COALESCE(CAST(freight_value AS STRING), '')
    )) AS STRING),
    '-1'
)
