SELECT shipment_id, COUNT(*) AS row_count
FROM {{ ref('fact_shipments') }}
GROUP BY shipment_id
HAVING COUNT(*) > 1
