SELECT
    review_id,
    order_id,
    review_score,
    comment,
    SAFE_CAST(review_timestamp AS TIMESTAMP) AS review_timestamp
FROM {{ source('silver_layer', 'silver_reviews') }}
