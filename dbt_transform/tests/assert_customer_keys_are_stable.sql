-- The inferred member and first real version share a key; later versions do not.
WITH violations AS (
    SELECT customer_id
    FROM {{ ref('dim_customers') }}
    WHERE (
        (is_inferred OR valid_from = TIMESTAMP '1970-01-01')
        AND customer_sk != FARM_FINGERPRINT(CONCAT(customer_id, '|initial'))
    ) OR (
        NOT is_inferred
        AND valid_from != TIMESTAMP '1970-01-01'
        AND (
            customer_sk = FARM_FINGERPRINT(CONCAT(customer_id, '|initial'))
            OR customer_sk != FARM_FINGERPRINT(
                CONCAT(customer_id, '|', CAST(valid_from AS STRING))
            )
        )
    )
)
SELECT * FROM violations
