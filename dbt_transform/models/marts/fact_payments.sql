{{ config(materialized='incremental', unique_key='payment_id', incremental_strategy='merge') }}

SELECT
    p.payment_id,
    p.order_id,
    p.payment_type,
    p.installments,
    p.payment_value,
    1 AS payment_count
FROM {{ ref('stg_payments') }} p
WHERE p.payment_id IS NOT NULL
  AND p.order_id IS NOT NULL
  AND p.payment_value >= 0
  AND EXISTS (
      SELECT 1 FROM {{ ref('stg_orders') }} o
      WHERE o.order_id = p.order_id
        AND o.order_status != 'TEST_STATUS'
  )
