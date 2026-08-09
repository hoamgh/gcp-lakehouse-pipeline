WITH source_data AS (
    SELECT
        order_id,
        customer_id,
        order_status,
        purchase_timestamp,
        approved_timestamp,
        delivered_timestamp,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY purchase_timestamp DESC) as row_num
    FROM {{ source('silver_layer', 'silver_orders') }}
)
SELECT 
    order_id,
    customer_id,
    order_status,
    purchase_timestamp,
    approved_timestamp,
    delivered_timestamp
FROM source_data
WHERE row_num = 1
