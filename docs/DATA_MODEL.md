# Gold dimensional model

> Xem [BIGQUERY_MIGRATION_INCIDENT.md](BIGQUERY_MIGRATION_INCIDENT.md) để hiểu
> lỗi dữ liệu cũ, lý do phải migration và quy trình bảo toàn dữ liệu đã áp dụng.

## Modeling rules

| Model | Grain | Pattern |
| --- | --- | --- |
| `dim_customers` | One row per customer version | SCD Type 2 through `customers_snapshot` |
| `dim_products` | One row per product | SCD Type 1 merge |
| `dim_sellers` | One row per seller | SCD Type 1 merge |
| `dim_date` | One row per calendar date | Role-playing date dimension |
| `fact_orders` | One row per order | Transaction fact |
| `fact_order_items` | One row per order item | Transaction fact |
| `fact_payments` | One row per payment | Transaction fact |
| `fact_reviews` | One row per review | Event fact |
| `fact_shipments` | One row per shipment | Accumulating snapshot fact |

Customer attributes are historized because location and contact changes can be
analytically relevant. Product and seller attributes use SCD1 because the
current descriptive value is sufficient for this project. Facts keep their
natural transaction IDs as degenerate dimensions and use additive count and
amount measures.

`fact_orders.customer_sk` is assigned to the current customer version when an
order is first loaded. The incremental merge deliberately excludes that key
from updates, so later customer changes do not rewrite historical facts.

## Breaking schema migration

`order_item_id` was added to establish a stable order-item grain. Existing
Bronze, Silver, BigQuery external tables, and Gold models created with the old
schema must be rebuilt once:

```bash
dbt snapshot --profiles-dir .
dbt build --full-refresh --profiles-dir .
```

Before the dbt rebuild, clear or version the old order-item Spark checkpoints
and rebuild the order-item Bronze/Silver data from staging. Do not mix old rows
without `order_item_id` with the new schema; those rows are intentionally sent
to the DLQ.
