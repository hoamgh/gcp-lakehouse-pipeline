{% set source_relation = source('silver_layer', 'silver_order_items') %}
{% set source_columns = adapter.get_columns_in_relation(source_relation) if execute else [] %}
{% set source_column_names = source_columns | map(attribute='name') | map('lower') | list %}

WITH source_data AS (
SELECT
    {% if 'order_item_id' in source_column_names %}
    COALESCE(
        order_item_id,
        CONCAT(
            CAST(FARM_FINGERPRINT(CONCAT(
                COALESCE(order_id, ''), '|', COALESCE(product_id, ''), '|',
                COALESCE(seller_id, ''), '|', COALESCE(CAST(price AS STRING), ''), '|',
                COALESCE(CAST(freight_value AS STRING), '')
            )) AS STRING),
            '-',
            CAST(ROW_NUMBER() OVER (
                PARTITION BY order_id, product_id, seller_id,
                             CAST(price AS STRING), CAST(freight_value AS STRING)
                ORDER BY order_id
            ) AS STRING)
        )
    ) AS order_item_id,
    {% else %}
    CONCAT(
        CAST(FARM_FINGERPRINT(CONCAT(
            COALESCE(order_id, ''), '|', COALESCE(product_id, ''), '|',
            COALESCE(seller_id, ''), '|', COALESCE(CAST(price AS STRING), ''), '|',
            COALESCE(CAST(freight_value AS STRING), '')
        )) AS STRING),
        '-',
        CAST(ROW_NUMBER() OVER (
            PARTITION BY order_id, product_id, seller_id,
                         CAST(price AS STRING), CAST(freight_value AS STRING)
            ORDER BY order_id
        ) AS STRING)
    ) AS order_item_id,
    {% endif %}
    order_id,
    product_id,
    seller_id,
    price,
    freight_value
FROM {{ source_relation }}
)

SELECT *
FROM source_data
-- Business Logic at Gold Layer: Filter out invalid negative prices
WHERE order_item_id IS NOT NULL
  AND order_id IS NOT NULL
  AND product_id IS NOT NULL
  AND seller_id IS NOT NULL
  AND price > 0
