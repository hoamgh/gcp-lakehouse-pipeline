SELECT f.order_item_sk, f.seller_sk
FROM {{ ref('fact_order_items') }} f
LEFT JOIN {{ ref('dim_sellers') }} d ON f.seller_sk = d.seller_sk
WHERE d.seller_sk IS NULL
