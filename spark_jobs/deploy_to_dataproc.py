import os
import sys
import logging
from google.cloud import dataproc_v1 as dataproc
from dotenv import load_dotenv

load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

PROJECT_ID = os.environ.get("PROJECT_ID", "hybrid-elt-lakehouse-pipeline")
REGION = os.environ.get("REGION", "asia-southeast1")
GCS_BUCKET = os.environ.get("GCS_BUCKET", f"{PROJECT_ID}-lakehouse")
SUBNET_URI = f"projects/{PROJECT_ID}/regions/{REGION}/subnetworks/default" 

def submit_pyspark_batch(job_name, script_path_on_gcs):
    # Create a batch client
    client = dataproc.BatchControllerClient(
        client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com:443"}
    )
    
    # Define the batch job
    batch = dataproc.Batch()
    batch.pyspark_batch = dataproc.PySparkBatch(
        main_python_file_uri=script_path_on_gcs,
        python_file_uris=[
            f"gs://{GCS_BUCKET}/scripts/config.py",
            f"gs://{GCS_BUCKET}/scripts/logger.py"
        ],
    )
    
    # Environment variables & Delta Lake configs
    batch.environment_config = dataproc.EnvironmentConfig(
        execution_config=dataproc.ExecutionConfig(
            subnetwork_uri=SUBNET_URI,
            service_account=f"elt-pipeline-sa@{PROJECT_ID}.iam.gserviceaccount.com"
        )
    )
    
    # Add Delta spark packages to properties
    batch.runtime_config = dataproc.RuntimeConfig(
        properties={
            "spark.jars.packages": "io.delta:delta-spark_2.13:3.2.0",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        }
    )

    import uuid
    batch_id = f"{job_name}-{str(uuid.uuid4())[:8]}"
    parent = f"projects/{PROJECT_ID}/locations/{REGION}"
    
    logging.info(f"Submitting {job_name} to Dataproc Serverless (Batch ID: {batch_id})...")
    
    # Submit the batch
    operation = client.create_batch(
        request={"parent": parent, "batch": batch, "batch_id": batch_id}
    )
    
    logging.info(f"Batch submitted! Waiting for operation to complete...")
    print(f"To view the job: https://console.cloud.google.com/dataproc/batches/{REGION}/{batch_id}?project={PROJECT_ID}")
    
    # Wait for the job to complete
    result = operation.result()
    logging.info(f"Batch {batch_id} completed successfully.")

if __name__ == "__main__":
    submit_pyspark_batch("stream-raw-to-bronze", f"gs://{GCS_BUCKET}/scripts/stream_raw_to_bronze.py")
    submit_pyspark_batch("stream-bronze-to-silver", f"gs://{GCS_BUCKET}/scripts/stream_bronze_to_silver.py")
