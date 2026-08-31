import os
import sys
import uuid
import logging
from google.cloud import dataproc_v1 as dataproc
from dotenv import load_dotenv

load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------------------------------------------------------------------------
# Configuration — read from environment, fallback to defaults
# ---------------------------------------------------------------------------
PROJECT_ID = os.environ.get("PROJECT_ID", "hybrid-elt-lakehouse-pipeline")
REGION = os.environ.get("REGION", "asia-southeast1")
GCS_BUCKET = os.environ.get("GCS_BUCKET", f"{PROJECT_ID}-lakehouse")
SERVICE_ACCOUNT_NAME = os.environ.get("SERVICE_ACCOUNT_NAME", "elt-pipeline-sa")

SUBNET_URI = f"projects/{PROJECT_ID}/regions/{REGION}/subnetworks/default"
SERVICE_ACCOUNT = f"{SERVICE_ACCOUNT_NAME}@{PROJECT_ID}.iam.gserviceaccount.com"
SCRIPTS_GCS_PREFIX = f"gs://{GCS_BUCKET}/scripts"

DELTA_SPARK_VERSION = "3.2.0"
DELTA_SPARK_PACKAGE = f"io.delta:delta-spark_2.13:{DELTA_SPARK_VERSION}"


def submit_pyspark_batch(job_name: str, script_name: str):
    """Submit a PySpark batch to Dataproc Serverless."""
    client = dataproc.BatchControllerClient(
        client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com:443"}
    )

    batch = dataproc.Batch()
    batch.pyspark_batch = dataproc.PySparkBatch(
        main_python_file_uri=f"{SCRIPTS_GCS_PREFIX}/{script_name}",
        python_file_uris=[
            f"{SCRIPTS_GCS_PREFIX}/config.py",
            f"{SCRIPTS_GCS_PREFIX}/logger.py",
        ],
    )

    batch.environment_config = dataproc.EnvironmentConfig(
        execution_config=dataproc.ExecutionConfig(
            subnetwork_uri=SUBNET_URI,
            service_account=SERVICE_ACCOUNT,
        )
    )

    batch.runtime_config = dataproc.RuntimeConfig(
        properties={
            "spark.jars.packages": DELTA_SPARK_PACKAGE,
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        }
    )

    batch_id = f"{job_name}-{str(uuid.uuid4())[:8]}"
    parent = f"projects/{PROJECT_ID}/locations/{REGION}"

    logging.info(f"Submitting {job_name} to Dataproc Serverless (Batch ID: {batch_id})...")

    operation = client.create_batch(
        request={"parent": parent, "batch": batch, "batch_id": batch_id}
    )

    logging.info("Batch submitted! Waiting for operation to complete...")
    print(f"To view the job: https://console.cloud.google.com/dataproc/batches/{REGION}/{batch_id}?project={PROJECT_ID}")

    result = operation.result()
    logging.info(f"Batch {batch_id} completed successfully.")


if __name__ == "__main__":
    submit_pyspark_batch("stream-raw-to-bronze", "stream_raw_to_bronze.py")
    submit_pyspark_batch("stream-bronze-to-silver", "stream_bronze_to_silver.py")
