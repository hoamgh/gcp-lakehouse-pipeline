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
| `fact_order_items` | One row per five-column legacy composite | Transaction fact |
| `fact_payments` | One row per payment | Transaction fact |
| `fact_reviews` | One row per review | Event fact |
| `fact_shipments` | One row per shipment | Accumulating snapshot fact |

Customer attributes are historized because location and contact changes can be
analytically relevant. Product and seller attributes use SCD1 because the
current descriptive value is sufficient for this project. Facts keep their
natural transaction IDs as degenerate dimensions and use additive count and
amount measures.

`fact_orders.customer_sk` is resolved by the customer SCD2 validity interval at
the order purchase timestamp. If defensive overlapping inferred and actual
members are present, the actual member wins and the result remains one row per
order.

## Isolated v2 migration

`order_item_id` is the source business identifier. It is required for new
upstream records but remains nullable for legacy rows. `order_item_sk` is the
not-null warehouse key derived from `(order_id, product_id, seller_id, price,
freight_value)` with the production-compatible `FARM_FINGERPRINT(...)-1`
expression. Build the changed Gold schema into separate datasets:

```bash
$env:ANALYTICS_V2_DATASET='analytics_v2'
$env:SILVER_DATASET='lakehouse_silver_v2'
dbt parse --profiles-dir profiles_v2 --target v2
dbt build --profiles-dir profiles_v2 --target v2
```

Do not reset snapshots, delete existing layer data, or run a production full
refresh. Because Silver now retains `_ingested_at`, a historical Silver
backfill is safe only if complete Bronze Delta history is confirmed on GCS.
New records missing `order_item_id` remain a Bronze DLQ concern. Existing
legacy nulls are retained, and dbt never synthesizes a source identifier for
them. See [MODELING_V2_RUNBOOK.md](MODELING_V2_RUNBOOK.md) for the prepared,
non-production migration commands.
