from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.utils.dates import days_ago

from dag_config import (
    PROJECT_ID, REGION, GCS_BUCKET, DEFAULT_ARGS,
    get_batch_config, make_batch_id,
)

# Override retries for maintenance (more tolerance for long-running compaction)
maintenance_args = {**DEFAULT_ARGS, "retries": 2}

# Tables to compact and vacuum
MAINTENANCE_TABLES = [
    f"gs://{GCS_BUCKET}/bronze/orders",
    f"gs://{GCS_BUCKET}/silver/dim_customers",
]

with DAG(
    'hybrid_lakehouse_weekly_maintenance',
    default_args=maintenance_args,
    description='Weekly Delta Lake Maintenance: Optimize & Vacuum',
    schedule_interval='0 3 * * 0',
    start_date=days_ago(1),
    catchup=False,
    tags=['lakehouse', 'spark', 'serverless', 'maintenance'],
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

    run_delta_maintenance
