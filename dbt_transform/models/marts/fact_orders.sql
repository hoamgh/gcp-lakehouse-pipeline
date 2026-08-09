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
    o.order_status,
    o.purchase_timestamp,
    o.approved_timestamp,
    o.delivered_timestamp,
    COALESCE(p.total_payment_value, 0) as total_payment_value,
    COALESCE(p.payment_count, 0) as payment_count,
    COALESCE(p.max_installments, 0) as max_installments
FROM {{ ref('stg_orders') }} o
LEFT JOIN order_payments p ON o.order_id = p.order_id
WHERE o.order_status != 'TEST_STATUS'
