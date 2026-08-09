{{
    config(
        materialized='incremental',
        unique_key='shipment_id',
        merge_update_columns=['order_id', 'shipping_status', 'event_timestamp']
    )
}}

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
FROM {{ ref('stg_shipments') }}

{% if is_incremental() %}
  -- This filter will only be applied on an incremental run
  WHERE event_timestamp > (SELECT MAX(event_timestamp) FROM {{ this }})
{% endif %}
