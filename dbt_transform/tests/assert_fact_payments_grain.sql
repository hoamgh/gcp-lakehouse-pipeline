SELECT payment_id, COUNT(*) AS row_count
FROM {{ ref('fact_payments') }}
GROUP BY payment_id
HAVING COUNT(*) > 1
