"""
Shared configuration for all Airflow DAGs.

Centralizes GCP project settings, Dataproc Serverless batch config,
and common DAG defaults. All DAGs should import from here instead
of defining their own constants.
"""

import os
import uuid
from datetime import timedelta

# ---------------------------------------------------------------------------
# GCP Project Settings (read from Airflow environment / docker-compose)
# ---------------------------------------------------------------------------
PROJECT_ID = os.environ.get("PROJECT_ID", "hybrid-elt-lakehouse-pipeline")
REGION = os.environ.get("REGION", "asia-southeast1")
GCS_BUCKET = os.environ.get("GCS_BUCKET", f"{PROJECT_ID}-lakehouse")
SERVICE_ACCOUNT_NAME = os.environ.get("SERVICE_ACCOUNT_NAME", "elt-pipeline-sa")

# Derived URIs
SUBNET_URI = f"projects/{PROJECT_ID}/regions/{REGION}/subnetworks/default"
SERVICE_ACCOUNT = f"{SERVICE_ACCOUNT_NAME}@{PROJECT_ID}.iam.gserviceaccount.com"
SCRIPTS_GCS_PREFIX = f"gs://{GCS_BUCKET}/scripts"

# ---------------------------------------------------------------------------
# Delta Lake / Spark versions
# ---------------------------------------------------------------------------
DELTA_SPARK_VERSION = "3.2.0"
DELTA_SPARK_PACKAGE = f"io.delta:delta-spark_2.13:{DELTA_SPARK_VERSION}"

SPARK_PROPERTIES = {
    "spark.jars.packages": DELTA_SPARK_PACKAGE,
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.databricks.delta.retentionDurationCheck.enabled": "false",
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
def get_batch_config(script_name: str, extra_args: list = None):
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


def make_batch_id(prefix: str) -> str:
    """Generate a unique batch ID for Dataproc Serverless."""
    return f"{prefix}-{str(uuid.uuid4())[:8]}"
