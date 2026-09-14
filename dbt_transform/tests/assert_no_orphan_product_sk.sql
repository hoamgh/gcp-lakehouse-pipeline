SELECT f.order_item_sk, f.product_sk
FROM {{ ref('fact_order_items') }} f
LEFT JOIN {{ ref('dim_products') }} d ON f.product_sk = d.product_sk
WHERE d.product_sk IS NULL
