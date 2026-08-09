SELECT
    seller_id,
    seller_name,
    city,
    state,
    CURRENT_TIMESTAMP() as valid_from
FROM {{ ref('stg_sellers') }}
