import logging
import os
import sys

sys.path.append(os.path.dirname(__file__))
from config import DLQ_DIR, ENTITIES, get_spark_session


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
        if not os.path.exists(entity_dlq_path) and not entity_dlq_path.startswith("gs://"):
            continue

        try:
            # DLQ is stored as JSON containing _corrupt_record
            df = spark.read.format("json").load(entity_dlq_path)

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

            # Emit metric
            if total_corr > 0:
                log_event(
                    "dlq_analysis",
                    {
                        "entity": entity,
                        "total_corrupted": total_corr,
                        "breakdown": dlq_metrics[entity]["reasons"],
                    },
                    severity="WARNING",
                )
                print(f"[{entity}] DLQ count: {total_corr}")

        except Exception as e:
            logging.warning(f"Could not analyze DLQ for {entity}: {e}")

    print("DLQ Analysis Complete. Metrics emitted.")


if __name__ == "__main__":
    analyze_dlq()
