SELECT
    product_id,
    category,
    product_name,
    weight_g,
    length_cm,
    height_cm,
    width_cm
FROM {{ source('silver_layer', 'silver_products') }}
