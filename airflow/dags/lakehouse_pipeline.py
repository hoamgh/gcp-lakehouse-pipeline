from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from dag_config import (
    PROJECT_ID, REGION, DEFAULT_ARGS,
    get_batch_config, make_batch_id,
)

with DAG(
    'hybrid_lakehouse_daily_pipeline',
    default_args=DEFAULT_ARGS,
    description='Daily ELT Pipeline: Ingest -> Silver -> dbt Gold',
    schedule_interval='0 2 * * *',
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['lakehouse', 'spark', 'serverless', 'dbt'],
) as dag:

    # 1. Staging -> Bronze (Schema validation + DLQ routing)
    run_raw_to_bronze = DataprocCreateBatchOperator(
        task_id="run_raw_to_bronze",
        project_id=PROJECT_ID,
        region=REGION,
        batch=get_batch_config("stream_raw_to_bronze.py"),
        batch_id=make_batch_id("raw-to-bronze"),
    )

    # 2. Bronze -> Silver (Dedup + MERGE upsert)
    run_bronze_to_silver = DataprocCreateBatchOperator(
        task_id="run_bronze_to_silver",
        project_id=PROJECT_ID,
        region=REGION,
        batch=get_batch_config("stream_bronze_to_silver.py"),
        batch_id=make_batch_id("bronze-to-silver"),
    )

    # 3. Silver -> Gold (dbt transformations in BigQuery)
    run_dbt_gold = BashOperator(
        task_id="run_dbt_gold",
        bash_command="cd /opt/airflow/dbt_transform && dbt build --profiles-dir .",
    )

    run_raw_to_bronze >> run_bronze_to_silver >> run_dbt_gold
