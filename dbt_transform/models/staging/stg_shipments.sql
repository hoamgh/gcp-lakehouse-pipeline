SELECT
    shipment_id,
    order_id,
    carrier,
    tracking_number,
    shipping_status,
    SAFE_CAST(shipped_date AS TIMESTAMP) AS shipped_date,
    SAFE_CAST(estimated_delivery_date AS TIMESTAMP) AS estimated_delivery_date,
    SAFE_CAST(actual_delivery_date AS TIMESTAMP) AS actual_delivery_date,
    SAFE_CAST(event_timestamp AS TIMESTAMP) AS event_timestamp
FROM {{ source('silver_layer', 'silver_shipments') }}
