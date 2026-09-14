SELECT
    o.order_id,
    COUNT(c.customer_sk) AS matched_customer_versions
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('dim_customers') }} c
  ON o.customer_id = c.customer_id
 AND o.purchase_timestamp >= c.valid_from
 AND o.purchase_timestamp < c.valid_to
GROUP BY o.order_id
HAVING COUNT(c.customer_sk) > 1
