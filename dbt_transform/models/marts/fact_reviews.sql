{{ config(materialized='incremental', unique_key='review_id', incremental_strategy='merge') }}

SELECT
    r.review_id,
    r.order_id,
    r.review_score,
    r.comment,
    r.review_timestamp,
    CAST(FORMAT_DATE('%Y%m%d', DATE(r.review_timestamp)) AS INT64) AS review_date_id,
    1 AS review_count
FROM {{ ref('stg_reviews') }} r
WHERE r.review_id IS NOT NULL
  AND r.order_id IS NOT NULL
  AND r.review_score BETWEEN 1 AND 5
  AND EXISTS (
      SELECT 1 FROM {{ ref('stg_orders') }} o
      WHERE o.order_id = r.order_id
        AND o.order_status != 'TEST_STATUS'
  )
