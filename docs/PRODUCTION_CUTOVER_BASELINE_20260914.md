# Production Cutover Baseline — 2026-09-14

This baseline was captured after the final production idempotency gate passed and
before merging `fix/modeling-foundation-v2` into `main`.

## Rollback references

- Pre-merge `main` commit/tag target: `23586ad` (`pre-modeling-v2-cutover-20260914`)
- Modeling-v2 commit parent: `1372209`
- Previous production Silver object generation: `1788683609507840`
- Previous production Silver SHA-256: `c6a973489f19b3322eb241b26c0777c2e3a9b11d20ed0c688d3d7cf3118b5e61`
- The previous GCS generation is protected by the bucket's seven-day soft-delete policy.

Checkpoints must not be reset during rollback or forward recovery.

## Active production artifacts

| Object | Generation | SHA-256 |
|---|---:|---|
| `scripts/stream_bronze_to_silver.py` | `1789234293724402` | `ea44423429ae602889597d429a2d3c020718e4ce452535e8e53b9fe90d4b8d10` |
| `scripts/config.py` | `1788683608978628` | `1bef7f3ea1351941130961876bd4b910ae575dccdbedcaff62217a2722651587` |
| `scripts/logger.py` | `1788683609153572` | `5b6ee9535af0e597d926d261f47460ed57b169949293dacfe0f490d0006e98c1` |

## Logical row-count baseline

| Object | Rows |
|---|---:|
| `silver_customers` | 34,811 |
| `silver_products` | 22,766 |
| `silver_sellers` | 11,569 |
| `silver_orders` | 103,733 |
| `silver_order_items` | 153,535 |
| `silver_payments` | 104,421 |
| `silver_reviews` | 24,367 |
| `silver_shipments` | 82,046 |
| `fact_orders` | 103,733 |
| `fact_order_items` | 134,736 |
| `fact_payments` | 93,366 |
| `fact_reviews` | 21,816 |
| `fact_shipments` | 59,399 |
| `dim_customers` | 39,679 |
| `dim_products` | 26,847 |
| `dim_sellers` | 13,366 |

## Validated invariants

- All fact grains and modern/legacy order-item grains have zero duplicates.
- Customer, product, and seller analytical surrogate-key orphan counts are zero.
- Canary customer has two non-overlapping SCD2 versions and one current version.
- Old and new canary orders resolve to V1 and V2 respectively.
- Canary shipment has one latest-state row.
- Canary item and payment totals are both `375,678`; Silver-to-Gold deltas are zero.
- Production Silver and normal production dbt processing are idempotent on unchanged input.
