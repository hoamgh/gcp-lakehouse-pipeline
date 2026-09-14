{{
    config(
        materialized='incremental',
        unique_key='seller_id',
        incremental_strategy='merge'
    )
}}

WITH actual_sellers AS (
SELECT
    FARM_FINGERPRINT(CONCAT('seller|', seller_id)) AS seller_sk,
    seller_id,
    seller_name,
    city,
    state,
    CURRENT_TIMESTAMP() as updated_at,
    FALSE AS is_inferred
FROM {{ ref('stg_sellers') }}
),
inferred_sellers AS (
SELECT DISTINCT
    FARM_FINGERPRINT(CONCAT('seller|', i.seller_id)) AS seller_sk,
    i.seller_id,
    'Unknown Seller' AS seller_name,
    'UNKNOWN' AS city,
    'UNKNOWN' AS state,
    CURRENT_TIMESTAMP() AS updated_at,
    TRUE AS is_inferred
FROM {{ ref('stg_order_items') }} i
LEFT JOIN actual_sellers s ON i.seller_id = s.seller_id
WHERE i.seller_id IS NOT NULL
  AND s.seller_id IS NULL
)

SELECT * FROM actual_sellers
UNION ALL
SELECT * FROM inferred_sellers
