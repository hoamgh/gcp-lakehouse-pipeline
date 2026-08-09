SELECT
    customer_id,
    zip_code,
    city,
    state,
    CURRENT_TIMESTAMP() as valid_from
FROM {{ ref('stg_customers') }}
