SELECT
    customer_id,
    customer_name,
    email,
    phone,
    city,
    state,
    zip_code
FROM {{ source('silver_layer', 'silver_customers') }}
