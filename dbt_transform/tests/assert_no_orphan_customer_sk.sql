SELECT f.order_id, f.customer_sk
FROM {{ ref('fact_orders') }} f
LEFT JOIN {{ ref('dim_customers') }} d ON f.customer_sk = d.customer_sk
WHERE d.customer_sk IS NULL
