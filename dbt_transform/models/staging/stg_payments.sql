SELECT
    payment_id,
    order_id,
    payment_type,
    installments,
    payment_value
FROM {{ source('silver_layer', 'silver_payments') }}
