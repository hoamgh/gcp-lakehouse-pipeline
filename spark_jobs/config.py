"""
Spark jobs configuration — paths, schemas, and SparkSession builder.

Centralizes all path definitions (local vs GCS) and provides a reusable
SparkSession factory with Delta Lake support.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_PROJECT_ROOT / ".env")

# ---------------------------------------------------------------------------
# Windows Hadoop setup — must be set BEFORE any Spark/Java interaction
# ---------------------------------------------------------------------------
if sys.platform == "win32" and "HADOOP_HOME" not in os.environ:
    _HADOOP_HOME = r"d:\hadoop"
    os.environ["HADOOP_HOME"] = _HADOOP_HOME
    # Also add hadoop\bin to PATH so hadoop.dll is found
    _hadoop_bin = os.path.join(_HADOOP_HOME, "bin")
    if _hadoop_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = _hadoop_bin + os.pathsep + os.environ.get("PATH", "")



from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType,
)


# ---------------------------------------------------------------------------
# Path configuration — switch between local and GCS
# ---------------------------------------------------------------------------

# Set PIPELINE_ENV=gcs to use GCS paths, otherwise use local paths.
_ENV = os.environ.get("PIPELINE_ENV", "gcs")

# Local development — relative to project root
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
STAGING_DIR = str(_PROJECT_ROOT / "output" / "staging")

if _ENV == "gcs":
    GCS_BUCKET = os.environ.get("GCS_BUCKET", "hybrid-elt-lakehouse-pipeline-lakehouse")
    STAGING_DIR = f"gs://{GCS_BUCKET}/staging"
    BRONZE_DIR = f"gs://{GCS_BUCKET}/bronze"
    SILVER_DIR = f"gs://{GCS_BUCKET}/silver"
    DLQ_DIR = f"gs://{GCS_BUCKET}/dlq"
else:
    # Local development — relative to project root
    _PROJECT_ROOT = Path(__file__).resolve().parent.parent
    RAW_DIR = str(_PROJECT_ROOT / "output")
    STAGING_DIR = str(_PROJECT_ROOT / "output" / "staging")
    BRONZE_DIR = str(_PROJECT_ROOT / "output" / "bronze")
    SILVER_DIR = str(_PROJECT_ROOT / "output" / "silver")
    DLQ_DIR = str(_PROJECT_ROOT / "output" / "dlq")


# ---------------------------------------------------------------------------
# Entity list (in processing order)
# ---------------------------------------------------------------------------

ENTITIES = [
    "customers",
    "products",
    "sellers",
    "orders",
    "order_items",
    "payments",
    "reviews",
    "shipments",
]


# ---------------------------------------------------------------------------
# Explicit schemas — used for schema validation in Raw → Bronze
#
# We define StructType schemas that mirror the JSON output of generators.py.
# A special "_corrupt_record" column is appended for PERMISSIVE mode parsing.
# ---------------------------------------------------------------------------

CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", StringType(), nullable=False),
    StructField("customer_name", StringType(), nullable=True),
    StructField("email", StringType(), nullable=True),
    StructField("phone", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("state", StringType(), nullable=True),
    StructField("zip_code", StringType(), nullable=True),
])

PRODUCT_SCHEMA = StructType([
    StructField("product_id", StringType(), nullable=False),
    StructField("category", StringType(), nullable=True),
    StructField("product_name", StringType(), nullable=True),
    StructField("weight_g", DoubleType(), nullable=True),
    StructField("length_cm", DoubleType(), nullable=True),
    StructField("height_cm", DoubleType(), nullable=True),
    StructField("width_cm", DoubleType(), nullable=True),
])

SELLER_SCHEMA = StructType([
    StructField("seller_id", StringType(), nullable=False),
    StructField("seller_name", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("state", StringType(), nullable=True),
])

ORDER_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=True),
    StructField("order_status", StringType(), nullable=True),
    StructField("purchase_timestamp", StringType(), nullable=True),
    StructField("approved_timestamp", StringType(), nullable=True),
    StructField("delivered_timestamp", StringType(), nullable=True),
])

ORDER_ITEM_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=True),
    StructField("product_id", StringType(), nullable=True),
    StructField("seller_id", StringType(), nullable=True),
    StructField("price", DoubleType(), nullable=True),
    StructField("freight_value", DoubleType(), nullable=True),
])

PAYMENT_SCHEMA = StructType([
    StructField("payment_id", StringType(), nullable=False),
    StructField("order_id", StringType(), nullable=True),
    StructField("payment_type", StringType(), nullable=True),
    StructField("installments", IntegerType(), nullable=True),
    StructField("payment_value", DoubleType(), nullable=True),
])

REVIEW_SCHEMA = StructType([
    StructField("review_id", StringType(), nullable=False),
    StructField("order_id", StringType(), nullable=True),
    StructField("review_score", IntegerType(), nullable=True),
    StructField("comment", StringType(), nullable=True),
    StructField("review_timestamp", StringType(), nullable=True),
])

SHIPMENT_SCHEMA = StructType([
    StructField("shipment_id", StringType(), nullable=False),
    StructField("order_id", StringType(), nullable=True),
    StructField("carrier", StringType(), nullable=True),
    StructField("tracking_number", StringType(), nullable=True),
    StructField("shipping_status", StringType(), nullable=True),
    StructField("shipped_date", StringType(), nullable=True),
    StructField("estimated_delivery_date", StringType(), nullable=True),
    StructField("actual_delivery_date", StringType(), nullable=True),
    StructField("event_timestamp", StringType(), nullable=True),
])

# Registry: entity name -> (schema, primary key column)
ENTITY_SCHEMAS = {
    "customers":   (CUSTOMER_SCHEMA, "customer_id"),
    "products":    (PRODUCT_SCHEMA, "product_id"),
    "sellers":     (SELLER_SCHEMA, "seller_id"),
    "orders":      (ORDER_SCHEMA, "order_id"),
    "order_items": (ORDER_ITEM_SCHEMA, None),  # no single PK
    "payments":    (PAYMENT_SCHEMA, "payment_id"),
    "reviews":     (REVIEW_SCHEMA, "review_id"),
    "shipments":   (SHIPMENT_SCHEMA, "shipment_id"),
}


# ---------------------------------------------------------------------------
# Timestamp columns per entity — used by Silver layer for casting
# ---------------------------------------------------------------------------

TIMESTAMP_COLUMNS = {
    "customers":   [],
    "products":    [],
    "sellers":     [],
    "orders":      ["purchase_timestamp", "approved_timestamp", "delivered_timestamp"],
    "order_items": [],
    "payments":    [],
    "reviews":     ["review_timestamp"],
    "shipments":   ["shipped_date", "estimated_delivery_date", "actual_delivery_date", "event_timestamp"],
}


# ---------------------------------------------------------------------------
# Dedup keys per entity — used by Silver layer for dropDuplicates
# ---------------------------------------------------------------------------

DEDUP_KEYS = {
    "customers":   ["customer_id"],
    "products":    ["product_id"],
    "sellers":     ["seller_id"],
    "orders":      ["order_id"],
    "order_items": ["order_id", "product_id", "seller_id"],
    "payments":    ["payment_id"],
    "reviews":     ["review_id"],
    "shipments":   ["shipment_id"],
}


# ---------------------------------------------------------------------------
# SparkSession builder
# ---------------------------------------------------------------------------

def get_spark_session(app_name: str = "ELT-Pipeline") -> SparkSession:
    """
    Build a SparkSession with Delta Lake support.

    On Dataproc, Delta jars are pre-installed via init actions.
    Locally, delta-spark pip package provides the JARs via
    configure_spark_with_delta_pip().
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Enable auto compaction and optimized writes for Delta Lake
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        # Performance tuning for local dev
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
    )

    if _ENV == "local" or _ENV == "gcs":
        builder = builder.master("local[*]")

        # Set Google credentials for GCS connector if environment variable exists
        credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if credentials_path:
            # Fix backslashes for Windows Java path
            credentials_path_java = credentials_path.replace("\\", "/")
            builder = builder.config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
            builder = builder.config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_path_java)
            
            # Explicitly set the GCS file system implementation
            builder = builder.config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
            builder = builder.config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

    # Use delta-spark pip utilities to add Delta JARs to classpath.
    # On Dataproc this is a no-op since JARs are pre-installed.
    from delta.pip_utils import configure_spark_with_delta_pip
    
    # Add GCS connector for reading/writing to gs://
    extra_packages = ["com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.14"]
    builder = configure_spark_with_delta_pip(builder, extra_packages=extra_packages)

    return builder.getOrCreate()
