-- Replace `PROJECT_ID` before running. These queries are read-only.

-- 1. Old vs v2 row counts.
WITH old_counts AS (
  SELECT 'fact_orders' AS model, COUNT(*) AS row_count FROM `PROJECT_ID.lakehouse_gold.fact_orders`
  UNION ALL SELECT 'fact_order_items', COUNT(*) FROM `PROJECT_ID.lakehouse_gold.fact_order_items`
  UNION ALL SELECT 'dim_customers', COUNT(*) FROM `PROJECT_ID.lakehouse_gold.dim_customers`
  UNION ALL SELECT 'dim_products', COUNT(*) FROM `PROJECT_ID.lakehouse_gold.dim_products`
  UNION ALL SELECT 'dim_sellers', COUNT(*) FROM `PROJECT_ID.lakehouse_gold.dim_sellers`
),
v2_counts AS (
  SELECT 'fact_orders' AS model, COUNT(*) AS row_count FROM `PROJECT_ID.analytics_v2.fact_orders`
  UNION ALL SELECT 'fact_order_items', COUNT(*) FROM `PROJECT_ID.analytics_v2.fact_order_items`
  UNION ALL SELECT 'dim_customers', COUNT(*) FROM `PROJECT_ID.analytics_v2.dim_customers`
  UNION ALL SELECT 'dim_products', COUNT(*) FROM `PROJECT_ID.analytics_v2.dim_products`
  UNION ALL SELECT 'dim_sellers', COUNT(*) FROM `PROJECT_ID.analytics_v2.dim_sellers`
)
SELECT model, old_counts.row_count AS old_rows, v2_counts.row_count AS v2_rows,
       v2_counts.row_count - old_counts.row_count AS row_delta
FROM old_counts JOIN v2_counts USING (model)
ORDER BY model;

-- 2. Duplicate fact grains (expect zero rows).
SELECT 'fact_orders' AS model, order_id AS business_key, COUNT(*) AS row_count
FROM `PROJECT_ID.analytics_v2.fact_orders` GROUP BY order_id HAVING COUNT(*) > 1
UNION ALL
SELECT 'fact_order_items', order_item_sk, COUNT(*)
FROM `PROJECT_ID.analytics_v2.fact_order_items` GROUP BY order_item_sk HAVING COUNT(*) > 1
UNION ALL
SELECT 'fact_payments', payment_id, COUNT(*)
FROM `PROJECT_ID.analytics_v2.fact_payments` GROUP BY payment_id HAVING COUNT(*) > 1
UNION ALL
SELECT 'fact_reviews', review_id, COUNT(*)
FROM `PROJECT_ID.analytics_v2.fact_reviews` GROUP BY review_id HAVING COUNT(*) > 1
UNION ALL
SELECT 'fact_shipments', shipment_id, COUNT(*)
FROM `PROJECT_ID.analytics_v2.fact_shipments` GROUP BY shipment_id HAVING COUNT(*) > 1;

-- 3. Orphan analytical foreign keys (all counts must be zero).
SELECT
  COUNTIF(c.customer_sk IS NULL) AS orphan_customer_sk,
  (SELECT COUNT(*) FROM `PROJECT_ID.analytics_v2.fact_order_items` f
   LEFT JOIN `PROJECT_ID.analytics_v2.dim_products` d USING (product_sk)
   WHERE d.product_sk IS NULL) AS orphan_product_sk,
  (SELECT COUNT(*) FROM `PROJECT_ID.analytics_v2.fact_order_items` f
   LEFT JOIN `PROJECT_ID.analytics_v2.dim_sellers` d USING (seller_sk)
   WHERE d.seller_sk IS NULL) AS orphan_seller_sk
FROM `PROJECT_ID.analytics_v2.fact_orders` f
LEFT JOIN `PROJECT_ID.analytics_v2.dim_customers` c USING (customer_sk);

-- 4. Invalid or overlapping SCD2 intervals (expect zero rows).
WITH periods AS (
  SELECT customer_id, customer_sk, valid_from, valid_to,
         LAG(valid_to) OVER (PARTITION BY customer_id ORDER BY valid_from, valid_to) AS previous_valid_to
  FROM `PROJECT_ID.analytics_v2.dim_customers`
)
SELECT * FROM periods
WHERE valid_from >= valid_to OR previous_valid_to > valid_from;

-- 5. Customer SK changes between old and v2 for review before cutover.
SELECT o.order_id, o.customer_id,
       o.customer_sk AS old_customer_sk, v.customer_sk AS v2_customer_sk
FROM `PROJECT_ID.lakehouse_gold.fact_orders` o
JOIN `PROJECT_ID.analytics_v2.fact_orders` v USING (order_id)
WHERE o.customer_sk IS DISTINCT FROM v.customer_sk
ORDER BY order_id;

-- 6. Legacy order-item SKs must remain byte-for-byte compatible (expect zero).
SELECT COUNT(*) AS changed_legacy_order_item_sks
FROM `PROJECT_ID.lakehouse_gold.fact_order_items` old
JOIN `PROJECT_ID.analytics_v2.fact_order_items` v2
  ON old.order_id = v2.order_id
 AND old.product_id = v2.product_id
 AND old.seller_id = v2.seller_id
 AND old.price = v2.price
 AND old.freight_value = v2.freight_value
WHERE old.order_item_sk != v2.order_item_sk;

-- 7. Declared legacy five-column grain (expect zero rows).
SELECT order_id, product_id, seller_id, price, freight_value, COUNT(*) AS row_count
FROM `PROJECT_ID.analytics_v2.fact_order_items`
GROUP BY order_id, product_id, seller_id, price, freight_value
HAVING COUNT(*) > 1;

-- 8. Explain Silver order-item growth: expected 44 recovered legacy rows plus
-- independently accounted modern rows.
SELECT
  COUNTIF(old.order_id IS NULL AND v2.order_item_id IS NULL) AS recovered_legacy_rows,
  COUNTIF(old.order_id IS NULL AND v2.order_item_id IS NOT NULL) AS new_modern_rows
FROM `PROJECT_ID.lakehouse_silver_v2.silver_order_items` v2
LEFT JOIN `PROJECT_ID.lakehouse_silver.silver_order_items` old
  USING (order_id, product_id, seller_id, price, freight_value);

-- 9. Common-row financial values must not change. New/recovered rows explain
-- aggregate deltas and should be reviewed separately.
SELECT
  COUNTIF(old.gross_item_value != v2.gross_item_value) AS changed_common_items,
  SUM(v2.gross_item_value - old.gross_item_value) AS common_item_revenue_delta
FROM `PROJECT_ID.lakehouse_gold.fact_order_items` old
JOIN `PROJECT_ID.analytics_v2.fact_order_items` v2
  USING (order_id, product_id, seller_id, price, freight_value);

SELECT
  COUNTIF(old.total_payment_value != v2.total_payment_value) AS changed_common_orders,
  SUM(v2.total_payment_value - old.total_payment_value) AS common_payment_delta
FROM `PROJECT_ID.lakehouse_gold.fact_orders` old
JOIN `PROJECT_ID.analytics_v2.fact_orders` v2 USING (order_id);
