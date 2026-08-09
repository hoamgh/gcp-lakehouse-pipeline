SELECT
    review_id,
    order_id,
    review_score,
    comment,
    review_timestamp
FROM {{ source('silver_layer', 'silver_reviews') }}
