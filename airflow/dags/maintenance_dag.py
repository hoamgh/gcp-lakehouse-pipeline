from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)
from airflow.utils.dates import days_ago
from dag_config import (
    DEFAULT_ARGS,
    GCS_BUCKET,
    PROJECT_ID,
    REGION,
    get_batch_config,
    make_batch_id,
)

from airflow import DAG

# Override retries for maintenance (more tolerance for long-running compaction)
maintenance_args = {**DEFAULT_ARGS, "retries": 2}

# Tables to compact and vacuum
ENTITIES = (
    "customers",
    "products",
    "sellers",
    "orders",
    "order_items",
    "payments",
    "reviews",
    "shipments",
)
MAINTENANCE_TABLES = [
    *[f"gs://{GCS_BUCKET}/bronze/{entity}" for entity in ENTITIES],
    *[f"gs://{GCS_BUCKET}/silver/silver_{entity}" for entity in ENTITIES],
]

with DAG(
    "hybrid_lakehouse_weekly_maintenance",
    default_args=maintenance_args,
    description="Weekly Delta Lake Maintenance: Optimize & Vacuum",
    schedule_interval="0 3 * * 0",
    start_date=days_ago(1),
    catchup=False,
    tags=["lakehouse", "spark", "serverless", "maintenance"],
) as dag:
    run_delta_maintenance = DataprocCreateBatchOperator(
        task_id="run_delta_maintenance",
        project_id=PROJECT_ID,
        region=REGION,
        batch=get_batch_config(
            "delta_maintenance.py",
            extra_args=["--tables", ",".join(MAINTENANCE_TABLES)],
        ),
        batch_id=make_batch_id("delta-maintenance"),
    )
