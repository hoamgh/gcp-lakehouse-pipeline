{{ config(materialized='view') }}

WITH actual_customers AS (
SELECT
    FARM_FINGERPRINT(CONCAT(customer_id, '|', CAST(dbt_valid_from AS STRING))) AS customer_sk,
    customer_id,
    customer_name,
    email,
    phone,
    zip_code,
    city,
    state,
    dbt_valid_from AS valid_from,
    COALESCE(dbt_valid_to, TIMESTAMP '9999-12-31 23:59:59 UTC') AS valid_to,
    dbt_valid_to IS NULL AS is_current,
    FALSE AS is_inferred
FROM {{ ref('customers_snapshot') }}
),
inferred_customers AS (
SELECT DISTINCT
    FARM_FINGERPRINT(CONCAT(o.customer_id, '|inferred')) AS customer_sk,
    o.customer_id,
    'Unknown Customer' AS customer_name,
    CAST(NULL AS STRING) AS email,
    CAST(NULL AS STRING) AS phone,
    CAST(NULL AS STRING) AS zip_code,
    'UNKNOWN' AS city,
    'UNKNOWN' AS state,
    TIMESTAMP '1970-01-01' AS valid_from,
    TIMESTAMP '9999-12-31 23:59:59 UTC' AS valid_to,
    TRUE AS is_current,
    TRUE AS is_inferred
FROM {{ ref('stg_orders') }} o
LEFT JOIN actual_customers c ON o.customer_id = c.customer_id
WHERE o.customer_id IS NOT NULL
  AND c.customer_id IS NULL
)

SELECT * FROM actual_customers
UNION ALL
SELECT * FROM inferred_customers
