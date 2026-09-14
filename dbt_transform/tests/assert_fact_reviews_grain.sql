SELECT review_id, COUNT(*) AS row_count
FROM {{ ref('fact_reviews') }}
GROUP BY review_id
HAVING COUNT(*) > 1
