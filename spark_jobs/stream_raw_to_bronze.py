import logging
import os
import sys
from datetime import datetime, timezone
from functools import reduce

from pyspark import StorageLevel
from pyspark.sql.functions import col, current_timestamp, lit, when
from pyspark.sql.types import StringType, StructField

sys.path.append(os.path.dirname(__file__))
from config import (
    BRONZE_DIR,
    DLQ_DIR,
    ENTITY_SCHEMAS,
    REQUIRED_COLUMNS,
    STAGING_DIR,
    get_spark_session,
)


def counts_from_classification_rows(rows):
    """Convert one classification aggregation into reconciled counters."""
    counts = {True: 0, False: 0}
    for row in rows:
        counts[bool(row["_is_rejected"])] = int(row["count"])

    valid_count = counts[False]
    rejected_count = counts[True]
    source_count = valid_count + rejected_count
    return {
        "source_count": source_count,
        "valid_count": valid_count,
        "rejected_count": rejected_count,
        "error_rate": rejected_count / source_count if source_count else 0.0,
    }


def classify_records(annotated_df, required_columns):
    """Classify every source row exactly once as valid or rejected."""
    missing_required = reduce(
        lambda left, right: left | right,
        [col(name).isNull() for name in required_columns],
    )
    invalid_record = col("_corrupt_record").isNotNull() | missing_required
    return annotated_df.withColumn("_is_rejected", invalid_record).withColumn(
        "_error_reason",
        when(
            col("_corrupt_record").isNotNull(),
            lit("malformed_json_or_type_mismatch"),
        )
        .when(missing_required, lit("missing_required_field"))
        .otherwise(lit(None).cast("string")),
    )


def bronze_transaction_app_id(entity):
    """Stable Delta transaction identity for one entity's checkpointed stream."""
    return f"raw-to-bronze-{entity}"


def dlq_epoch_path(entity, epoch_id):
    """Stable overwrite target that makes one DLQ epoch replay idempotent."""
    return f"{DLQ_DIR}/{entity}/epoch_id={int(epoch_id)}"


def process_classified_batch(classified_df, batch_id, entity):
    """Idempotently write both outcomes and emit counts from one cached batch."""
    from logger import log_quality_check

    processing_timestamp = datetime.now(timezone.utc).isoformat()
    cached = classified_df.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        # This single aggregation action materializes the cache and derives all counters.
        metrics = counts_from_classification_rows(
            cached.groupBy("_is_rejected").count().collect()
        )
        if metrics["source_count"] != metrics["valid_count"] + metrics["rejected_count"]:
            raise RuntimeError(
                f"Classification counts do not reconcile for {entity} batch {batch_id}: {metrics}"
            )

        (
            cached.filter(~col("_is_rejected"))
            .drop("_corrupt_record", "_error_reason", "_is_rejected")
            .write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .option("txnAppId", bronze_transaction_app_id(entity))
            .option("txnVersion", int(batch_id))
            .save(f"{BRONZE_DIR}/{entity}")
        )
        if metrics["rejected_count"]:
            (
                cached.filter(col("_is_rejected"))
                .drop("_is_rejected")
                .withColumn("_batch_id", lit(int(batch_id)))
                .withColumn("_processed_at", lit(processing_timestamp))
                .write.format("json")
                .mode("overwrite")
                .save(dlq_epoch_path(entity, batch_id))
            )

        log_quality_check(
            entity=entity,
            corrupt_records_count=metrics["rejected_count"],
            total_records=metrics["source_count"],
            valid_records_count=metrics["valid_count"],
            batch_id=batch_id,
            processing_timestamp=processing_timestamp,
        )
        logging.info(
            "classification_metrics entity=%s batch_id=%s processing_timestamp=%s "
            "source_count=%s valid_count=%s rejected_count=%s",
            entity,
            batch_id,
            processing_timestamp,
            metrics["source_count"],
            metrics["valid_count"],
            metrics["rejected_count"],
        )
    finally:
        cached.unpersist()


def stream_raw_to_bronze():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logging.info("Starting Batch Execution: Raw -> Bronze")
    spark = get_spark_session("StreamRawToBronze")
    spark.sparkContext.setLogLevel("WARN")
    queries = []

    for entity, (schema, _) in ENTITY_SCHEMAS.items():
        source_dir = f"{STAGING_DIR}/{entity}"
        if not source_dir.startswith("gs://"):
            os.makedirs(source_dir, exist_ok=True)

        schema_with_corrupt = schema.add(StructField("_corrupt_record", StringType(), True))
        raw_stream = (
            spark.readStream.format("json")
            .schema(schema_with_corrupt)
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .load(source_dir)
        )
        annotated = raw_stream.withColumn("_ingested_at", current_timestamp()).withColumn(
            "source", lit("streaming")
        )
        classified = classify_records(annotated, REQUIRED_COLUMNS[entity])

        query = (
            classified.writeStream.foreachBatch(
                lambda df, epoch_id, e=entity: process_classified_batch(df, epoch_id, e)
            )
            .option("checkpointLocation", f"{BRONZE_DIR}/_checkpoints/{entity}")
            .trigger(availableNow=True)
            .start()
        )
        queries.append(query)
        logging.info("Started classified batch processing for entity=%s", entity)

    for query in queries:
        query.awaitTermination()

    logging.info("Batch execution completed")


if __name__ == "__main__":
    stream_raw_to_bronze()
