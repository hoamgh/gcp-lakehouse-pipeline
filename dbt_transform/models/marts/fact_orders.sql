{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge',
        merge_update_columns=[
            'order_status', 'approved_timestamp', 'delivered_timestamp',
            'total_payment_value', 'payment_count', 'max_installments'
        ]
    )
}}

WITH order_payments AS (
    SELECT
        order_id,
        SUM(payment_value) as total_payment_value,
        COUNT(payment_id) as payment_count,
        MAX(installments) as max_installments
    FROM {{ ref('stg_payments') }}
    GROUP BY 1
)
SELECT
    o.order_id,
    o.customer_id,
    c.customer_sk,
    CAST(FORMAT_DATE('%Y%m%d', DATE(o.purchase_timestamp)) AS INT64) AS purchase_date_id,
    o.order_status,
    o.purchase_timestamp,
    o.approved_timestamp,
    o.delivered_timestamp,
    COALESCE(p.total_payment_value, 0) as total_payment_value,
    COALESCE(p.payment_count, 0) as payment_count,
    COALESCE(p.max_installments, 0) as max_installments
FROM {{ ref('stg_orders') }} o
LEFT JOIN order_payments p ON o.order_id = p.order_id
LEFT JOIN {{ ref('dim_customers') }} c
  ON o.customer_id = c.customer_id
 AND c.is_current
WHERE o.order_status != 'TEST_STATUS'
