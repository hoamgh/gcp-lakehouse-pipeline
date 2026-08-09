WITH first_review AS (
    -- Take the earliest review per order to avoid duplicating order items if there are multiple reviews
    SELECT * EXCEPT(row_num)
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY review_timestamp ASC) as row_num
        FROM {{ ref('stg_reviews') }}
    )
    WHERE row_num = 1
)
SELECT
    -- Generate surrogate key since order_item_id is not in schema
    FARM_FINGERPRINT(CONCAT(oi.order_id, oi.product_id, oi.seller_id)) as order_item_sk,
    oi.order_id,
    oi.product_id,
    oi.seller_id,
    oi.price,
    oi.freight_value,
    r.review_id,
    r.review_score
FROM {{ ref('stg_order_items') }} oi
LEFT JOIN first_review r ON oi.order_id = r.order_id
WHERE oi.price >= 0
