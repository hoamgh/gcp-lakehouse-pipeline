{{
    config(
        materialized='incremental',
        unique_key='product_id',
        merge_update_columns=['category', 'product_name', 'weight_g', 'length_cm', 'height_cm', 'width_cm', 'valid_from', 'is_inferred']
    )
}}

WITH actual_products AS (
    SELECT
        product_id,
        category,
        product_name,
        weight_g,
        length_cm,
        height_cm,
        width_cm,
        CURRENT_TIMESTAMP() as valid_from,
        FALSE as is_inferred
    FROM {{ ref('stg_products') }}
),
inferred_products AS (
    SELECT DISTINCT
        product_id,
        'UNKNOWN' as category,
        'Unknown Product' as product_name,
        NULL as weight_g,
        NULL as length_cm,
        NULL as height_cm,
        NULL as width_cm,
        CURRENT_TIMESTAMP() as valid_from,
        TRUE as is_inferred
    FROM {{ ref('stg_order_items') }}
    WHERE product_id NOT IN (SELECT product_id FROM actual_products)
),
all_products AS (
    SELECT * FROM actual_products
    UNION ALL
    SELECT * FROM inferred_products
)
SELECT * FROM all_products

{% if is_incremental() %}
  -- On incremental runs, process all products. Since unique_key is product_id, 
  -- dbt will MERGE this. If an inferred product (Unknown) is later found in actual_products,
  -- it will be overwritten with the real data (is_inferred=FALSE).
{% endif %}
