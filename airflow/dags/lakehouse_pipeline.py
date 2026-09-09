from datetime import timedelta

from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)
from airflow.utils.dates import days_ago
from dag_config import (
    DEFAULT_ARGS,
    PROJECT_ID,
    REGION,
    get_batch_config,
    make_batch_id,
)

from airflow import DAG

with DAG(
    "hybrid_lakehouse_daily_pipeline",
    default_args=DEFAULT_ARGS,
    description="Daily ELT Pipeline: Ingest -> Silver -> dbt Gold",
    schedule_interval="0 2 * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "spark", "serverless", "dbt"],
) as dag:
    # 1. Staging -> Bronze (Schema validation + DLQ routing)
    run_raw_to_bronze = DataprocCreateBatchOperator(
        task_id="run_raw_to_bronze",
        project_id=PROJECT_ID,
        region=REGION,
        batch=get_batch_config("stream_raw_to_bronze.py"),
        batch_id=make_batch_id("raw-to-bronze"),
        sla=timedelta(hours=1),
    )

    # 2. Bronze -> Silver (Dedup + MERGE upsert)
    run_bronze_to_silver = DataprocCreateBatchOperator(
        task_id="run_bronze_to_silver",
        project_id=PROJECT_ID,
        region=REGION,
        batch=get_batch_config("stream_bronze_to_silver.py"),
        batch_id=make_batch_id("bronze-to-silver"),
        sla=timedelta(hours=1),
    )

    # 3. Analyze DLQ
    run_dlq_analysis = DataprocCreateBatchOperator(
        task_id="run_dlq_analysis",
        project_id=PROJECT_ID,
        region=REGION,
        batch=get_batch_config("dlq_analysis.py"),
        batch_id=make_batch_id("dlq-analysis"),
    )

    # 4. Silver -> Gold (dbt transformations in BigQuery)
    run_dbt_gold = BashOperator(
        task_id="run_dbt_gold",
        bash_command="cd /opt/airflow/dbt_transform && dbt build --profiles-dir .",
        sla=timedelta(minutes=30),
    )

    run_raw_to_bronze >> [run_bronze_to_silver, run_dlq_analysis]
    run_bronze_to_silver >> run_dbt_gold

    @task
    def report_gold_funnel():
        """Reads row count from BigQuery Gold tables and logs them to Cloud Logging"""
        from google.cloud import bigquery
        from google.cloud import logging as cloud_logging

        # Initialize clients
        bq_client = bigquery.Client(project=PROJECT_ID)
        log_client = cloud_logging.Client(project=PROJECT_ID)
        logger = log_client.logger("lakehouse_pipeline")

        # Assuming dbt builds into the `lakehouse_gold` dataset
        dataset_id = f"{PROJECT_ID}.lakehouse_gold"

        query = f"""
            SELECT table_id, row_count
            FROM `{dataset_id}.__TABLES__`
            WHERE table_id IN (
                'fact_orders', 'fact_order_items', 'fact_payments',
                'fact_reviews', 'fact_shipments', 'dim_customers',
                'dim_products', 'dim_sellers', 'dim_date'
            )
        """
        results = bq_client.query(query).result()
        for row in results:
            payload = {
                "event_type": "funnel_reconciliation",
                "entity": row["table_id"].replace("fact_", "").replace("dim_", ""),
                "layer": "gold",
                "total_count": row["row_count"],
            }
            logger.log_struct(payload, severity="INFO")
            print(f"Logged Gold Funnel for {row['table_id']}: {row['row_count']}")

    run_dbt_gold >> report_gold_funnel()
