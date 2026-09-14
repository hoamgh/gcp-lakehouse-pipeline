SELECT 
    order_id,
    customer_id,
    order_status,
    SAFE_CAST(purchase_timestamp AS TIMESTAMP) AS purchase_timestamp,
    SAFE_CAST(approved_timestamp AS TIMESTAMP) AS approved_timestamp,
    SAFE_CAST(delivered_timestamp AS TIMESTAMP) AS delivered_timestamp,
    SAFE_CAST(_ingested_at AS TIMESTAMP) AS _ingested_at
FROM {{ source('silver_layer', 'silver_orders') }}
