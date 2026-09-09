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
    batch_id_template,
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
    user_defined_macros={"make_batch_id": make_batch_id},
) as dag:
    # 1. Staging -> Bronze (Schema validation + DLQ routing)
    run_raw_to_bronze = DataprocCreateBatchOperator(
        task_id="run_raw_to_bronze",
        project_id=PROJECT_ID,
        region=REGION,
        batch=get_batch_config("stream_raw_to_bronze.py"),
        batch_id=batch_id_template("raw-to-bronze"),
        sla=timedelta(hours=1),
    )

    # 2. Bronze -> Silver (Dedup + MERGE upsert)
    run_bronze_to_silver = DataprocCreateBatchOperator(
        task_id="run_bronze_to_silver",
        project_id=PROJECT_ID,
        region=REGION,
        batch=get_batch_config("stream_bronze_to_silver.py"),
        batch_id=batch_id_template("bronze-to-silver"),
        sla=timedelta(hours=1),
    )

    # 3. Analyze DLQ. This is observational: rejected records are reported but
    # do not fail the task; inability to execute the analysis still fails it.
    run_dlq_analysis = DataprocCreateBatchOperator(
        task_id="run_dlq_analysis",
        project_id=PROJECT_ID,
        region=REGION,
        batch=get_batch_config("dlq_analysis.py"),
        batch_id=batch_id_template("dlq-analysis"),
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
    def reconcile_gold_layer():
        """Reconcile Silver control totals and Gold integrity; fail on hard errors."""
        from airflow.exceptions import AirflowException
        from airflow.operators.python import get_current_context
        from google.cloud import bigquery
        from google.cloud import logging as cloud_logging
        from reconciliation import raise_for_reconciliation_failures, run_reconciliation

        bq_client = bigquery.Client(project=PROJECT_ID)
        log_client = cloud_logging.Client(project=PROJECT_ID)
        logger = log_client.logger("lakehouse_pipeline")
        dag_run_id = get_current_context()["run_id"]
        try:
            results = run_reconciliation(bq_client, logger, PROJECT_ID, dag_run_id)
        except RuntimeError as exc:
            raise AirflowException(str(exc)) from exc

        raise_for_reconciliation_failures(results, AirflowException)

    run_dbt_gold >> reconcile_gold_layer()
