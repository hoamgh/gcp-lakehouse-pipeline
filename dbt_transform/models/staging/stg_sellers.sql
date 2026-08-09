SELECT
    seller_id,
    seller_name,
    city,
    state
FROM {{ source('silver_layer', 'silver_sellers') }}
