SELECT
    shipment_id,
    order_id,
    carrier,
    tracking_number,
    shipping_status,
    shipped_date,
    estimated_delivery_date,
    actual_delivery_date,
    event_timestamp
FROM {{ source('silver_layer', 'silver_shipments') }}
