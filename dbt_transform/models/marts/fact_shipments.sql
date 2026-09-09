{{
    config(
        materialized='incremental',
        unique_key='shipment_id',
        incremental_strategy='merge'
    )
}}

SELECT
    s.shipment_id,
    s.order_id,
    s.carrier,
    s.tracking_number,
    s.shipping_status,
    s.shipped_date,
    s.estimated_delivery_date,
    s.actual_delivery_date,
    s.event_timestamp
FROM {{ ref('stg_shipments') }} s
WHERE s.order_id IS NOT NULL
  AND EXISTS (
      SELECT 1 FROM {{ ref('stg_orders') }} o
      WHERE o.order_id = s.order_id
        AND o.order_status != 'TEST_STATUS'
  )
