import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark_jobs"))

from stream_raw_to_bronze import (
    bronze_transaction_app_id,
    classify_records,
    counts_from_classification_rows,
    dlq_epoch_path,
)

# Avoid leaking Spark's top-level `config` module into generator tests.
sys.path.pop(0)
sys.modules.pop("config", None)


def test_classification_counts_reconcile_95_valid_5_rejected():
    rows = [
        {"_is_rejected": False, "count": 95},
        {"_is_rejected": True, "count": 5},
    ]
    counts = counts_from_classification_rows(rows)
    assert counts == {
        "source_count": 100,
        "valid_count": 95,
        "rejected_count": 5,
        "error_rate": 0.05,
    }
    assert counts["source_count"] == counts["valid_count"] + counts["rejected_count"]


def test_100_classified_source_records_produce_95_valid_and_5_rejected():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.master("local[1]").appName("bronze-metrics-test").getOrCreate()
    try:
        source = spark.range(100).selectExpr(
            "concat('order-', id) AS order_id",
            "IF(id < 5, CAST(NULL AS STRING), concat('customer-', id)) AS customer_id",
            "CAST(NULL AS STRING) AS _corrupt_record",
        )
        classified = classify_records(source, ["order_id", "customer_id"])
        counts = counts_from_classification_rows(
            classified.groupBy("_is_rejected").count().collect()
        )
    finally:
        spark.stop()

    assert counts["source_count"] == 100
    assert counts["valid_count"] == 95
    assert counts["rejected_count"] == 5
    assert counts["error_rate"] == 0.05


def test_retry_identifiers_are_stable_per_entity_and_epoch():
    assert bronze_transaction_app_id("orders") == bronze_transaction_app_id("orders")
    assert bronze_transaction_app_id("orders") != bronze_transaction_app_id("payments")
    assert dlq_epoch_path("orders", 7) == dlq_epoch_path("orders", 7)
    assert dlq_epoch_path("orders", 7) != dlq_epoch_path("orders", 8)
