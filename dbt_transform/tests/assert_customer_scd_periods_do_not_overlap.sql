WITH ordered AS (
    SELECT
        customer_id,
        valid_from,
        valid_to,
        LAG(valid_to) OVER (PARTITION BY customer_id ORDER BY valid_from) AS previous_valid_to
    FROM {{ ref('dim_customers') }}
)
SELECT *
FROM ordered
WHERE valid_from >= valid_to
   OR previous_valid_to > valid_from
