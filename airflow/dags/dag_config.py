"""
Shared configuration for all Airflow DAGs.

Centralizes GCP project settings, Dataproc Serverless batch config,
and common DAG defaults. All DAGs should import from here instead
of defining their own constants.
"""

import os
import re
import uuid
from datetime import timedelta

# ---------------------------------------------------------------------------
# GCP Project Settings (read from Airflow environment / docker-compose)
# ---------------------------------------------------------------------------
PROJECT_ID = os.environ["PROJECT_ID"]
REGION = os.environ.get("REGION", "asia-southeast1")
GCS_BUCKET = os.environ["GCS_BUCKET"]
SERVICE_ACCOUNT_NAME = os.environ["SERVICE_ACCOUNT_NAME"]

# Derived URIs
SUBNET_URI = f"projects/{PROJECT_ID}/regions/{REGION}/subnetworks/default"
SERVICE_ACCOUNT = f"{SERVICE_ACCOUNT_NAME}@{PROJECT_ID}.iam.gserviceaccount.com"
SCRIPTS_GCS_PREFIX = f"gs://{GCS_BUCKET}/scripts"

# ---------------------------------------------------------------------------
# Delta Lake / Spark versions
# ---------------------------------------------------------------------------
DELTA_SPARK_VERSION = "3.2.1"
DELTA_SPARK_PACKAGE = f"io.delta:delta-spark_2.13:{DELTA_SPARK_VERSION}"

SPARK_PROPERTIES = {
    "spark.jars.packages": DELTA_SPARK_PACKAGE,
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.databricks.delta.retentionDurationCheck.enabled": "false",
    "spark.dataproc.driverEnv.PIPELINE_ENV": "gcs",
    "spark.dataproc.driverEnv.GCS_BUCKET": GCS_BUCKET,
    "spark.executorEnv.PIPELINE_ENV": "gcs",
    "spark.executorEnv.GCS_BUCKET": GCS_BUCKET,
}

# ---------------------------------------------------------------------------
# Common DAG defaults
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ---------------------------------------------------------------------------
# Batch config builder for Dataproc Serverless
# ---------------------------------------------------------------------------
def get_batch_config(script_name: str, extra_args: list | None = None):
    """
    Build a Dataproc Serverless batch config dict for the given script.

    Args:
        script_name: Filename of the PySpark script (e.g. 'stream_raw_to_bronze.py').
        extra_args:  Optional CLI arguments to pass to the script.
    """
    config = {
        "pyspark_batch": {
            "main_python_file_uri": f"{SCRIPTS_GCS_PREFIX}/{script_name}",
            "python_file_uris": [
                f"{SCRIPTS_GCS_PREFIX}/config.py",
                f"{SCRIPTS_GCS_PREFIX}/logger.py",
            ],
        },
        "environment_config": {
            "execution_config": {
                "subnetwork_uri": SUBNET_URI,
                "service_account": SERVICE_ACCOUNT,
            }
        },
        "runtime_config": {
            "properties": SPARK_PROPERTIES,
        },
    }
    if extra_args:
        config["pyspark_batch"]["args"] = extra_args
    return config


def make_batch_id(prefix: str, dag_id: str, run_id: str) -> str:
    """Build a deterministic, GCP-safe Dataproc batch ID for a DAG run."""
    safe_prefix = re.sub(r"[^a-z0-9-]", "-", prefix.lower()).strip("-")
    if not safe_prefix or not safe_prefix[0].isalpha():
        safe_prefix = f"batch-{safe_prefix}"
    run_hash = uuid.uuid5(uuid.NAMESPACE_URL, f"{dag_id}:{run_id}:{prefix}").hex[:16]
    return f"{safe_prefix[:46].rstrip('-')}-{run_hash}"


def batch_id_template(prefix: str) -> str:
    """Render per-run IDs; provider 10.19 attaches to an existing ID on retry."""
    return "{{ make_batch_id('" + prefix + "', dag.dag_id, run_id) }}"
