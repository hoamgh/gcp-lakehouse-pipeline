SELECT r.*, 'order_not_found' AS rejection_reason, CURRENT_TIMESTAMP() AS rejected_at
FROM {{ ref('stg_reviews') }} r
LEFT JOIN {{ ref('stg_orders') }} o ON r.order_id = o.order_id
WHERE o.order_id IS NULL OR o.order_status = 'TEST_STATUS'
