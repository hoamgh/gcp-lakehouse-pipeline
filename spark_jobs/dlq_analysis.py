import logging
import os
import sys

sys.path.append(os.path.dirname(__file__))
from config import DLQ_DIR, ENTITIES, get_spark_session


def path_exists(spark, path):
    jvm = spark._jvm
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    filesystem = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
    return filesystem.exists(hadoop_path)


def analyze_dlq():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logging.info("Starting DLQ Analysis Job")

    spark = get_spark_session("DLQ-Analyzer")
    spark.sparkContext.setLogLevel("WARN")

    try:
        from logger import log_event
    except ImportError:

        def log_event(*args, **kwargs):
            pass

        logging.warning("No logger found, ignoring cloud logging")

    dlq_metrics = {}

    for entity in ENTITIES:
        entity_dlq_path = f"{DLQ_DIR}/{entity}"
        if not path_exists(spark, entity_dlq_path):
            continue

        # Findings are observational and never fail the job. Read/query errors
        # are operational failures and intentionally propagate to Airflow.
        # recursiveFileLookup supports both legacy root files and new
        # idempotent epoch_id=<n> subdirectories during migration.
        df = (
            spark.read.format("json")
            .option("recursiveFileLookup", "true")
            .load(entity_dlq_path)
        )

        total_corr = df.count()
        if "_error_reason" in df.columns:
            reason_rows = df.groupBy("_error_reason").count().collect()
            reasons = {row["_error_reason"] or "unknown": row["count"] for row in reason_rows}
        else:
            reasons = {"legacy_unclassified": total_corr}
        dlq_metrics[entity] = {
            "total_corrupted": total_corr,
            "reasons": reasons,
        }

        if total_corr > 0:
            log_event(
                "dlq_analysis",
                {
                    "entity": entity,
                    "total_corrupted": total_corr,
                    "breakdown": dlq_metrics[entity]["reasons"],
                    "quality_gate": False,
                },
                severity="WARNING",
            )
            print(f"[{entity}] DLQ count: {total_corr}")

    print("DLQ Analysis Complete. Metrics emitted.")


if __name__ == "__main__":
    analyze_dlq()
