import logging
import os
import sys
from functools import reduce

from pyspark.sql.functions import col, current_timestamp, lit, when
from pyspark.sql.types import StringType

# Import schemas from config
sys.path.append(os.path.dirname(__file__))
from config import (
    BRONZE_DIR,
    DLQ_DIR,
    ENTITY_SCHEMAS,
    REQUIRED_COLUMNS,
    STAGING_DIR,
    get_spark_session,
)


def stream_raw_to_bronze():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logging.info("Starting Batch Execution: Raw -> Bronze")

    spark = get_spark_session("StreamRawToBronze")
    spark.sparkContext.setLogLevel("WARN")

    print(f"Starting AvailableNow ingestion from: {STAGING_DIR}")

    from logger import log_funnel_count, log_quality_check

    queries = []

    for entity in ENTITY_SCHEMAS:
        print(f"Setting up stream for entity: {entity}")

        schema, _ = ENTITY_SCHEMAS[entity]

        # Ensure directory exists before starting the stream (only for local paths)
        source_dir = f"{STAGING_DIR}/{entity}"
        if not source_dir.startswith("gs://"):
            os.makedirs(source_dir, exist_ok=True)

        # Add _corrupt_record to the schema to catch schema errors
        from pyspark.sql.types import StructField

        schema_with_corrupt = schema.add(StructField("_corrupt_record", StringType(), True))

        # Read from raw JSON files as a stream
        raw_stream = (
            spark.readStream.format("json")
            .schema(schema_with_corrupt)
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .load(source_dir)
        )

        # Add metadata columns
        df_annotated = raw_stream.withColumn("_ingested_at", current_timestamp()).withColumn(
            "source", lit("streaming")
        )

        missing_required = reduce(
            lambda left, right: left | right,
            [col(name).isNull() for name in REQUIRED_COLUMNS[entity]],
        )
        invalid_record = col("_corrupt_record").isNotNull() | missing_required

        df_bronze = df_annotated.filter(~invalid_record).drop("_corrupt_record")
        df_dlq = df_annotated.filter(invalid_record).withColumn(
            "_error_reason",
            when(
                col("_corrupt_record").isNotNull(), lit("malformed_json_or_type_mismatch")
            ).otherwise(lit("missing_required_field")),
        )

        # Write to Bronze Delta using AvailableNow trigger
        query_bronze = (
            df_bronze.writeStream.format("delta")
            .outputMode("append")
            .option("mergeSchema", "true")
            .option("checkpointLocation", f"{BRONZE_DIR}/_checkpoints/{entity}")
            .trigger(availableNow=True)
            .start(f"{BRONZE_DIR}/{entity}")
        )

        queries.append({"query": query_bronze, "type": "bronze", "entity": entity})
        logging.info(f"Started batch processing for entity: {entity}")

        # Write to DLQ JSON using AvailableNow trigger
        query_dlq = (
            df_dlq.writeStream.format("json")
            .option("checkpointLocation", f"{DLQ_DIR}/_checkpoints/{entity}")
            .trigger(availableNow=True)
            .start(f"{DLQ_DIR}/{entity}")
        )

        queries.append({"query": query_dlq, "type": "dlq", "entity": entity})

    logging.info("Waiting for all entities to finish processing this batch...")

    # Wait for queries and collect metrics
    for q_dict in queries:
        q = q_dict["query"]
        q.awaitTermination()

    # After all queries finish for this AvailableNow batch, process metrics
    # We group by entity to sum up metrics
    metrics = {}
    for q_dict in queries:
        entity = q_dict["entity"]
        q_type = q_dict["type"]
        q = q_dict["query"]

        # Sum numInputRows across all recent micro-batches for this query
        total_input = 0
        if q.recentProgress:
            total_input = sum([rp.get("numInputRows", 0) for rp in q.recentProgress])

        if entity not in metrics:
            metrics[entity] = {"bronze": 0, "dlq": 0}

        metrics[entity][q_type] += total_input

    # Log metrics per entity
    for entity, counts in metrics.items():
        total_dlq = counts["dlq"]
        total_bronze = counts["bronze"]
        total = total_dlq + total_bronze
        if total > 0:
            log_quality_check(entity=entity, corrupt_records_count=total_dlq, total_records=total)

        # Funnel Reconciliation: Read actual count from Delta table
        bronze_table_path = f"{BRONZE_DIR}/{entity}"
        if os.path.exists(bronze_table_path) or bronze_table_path.startswith("gs://"):
            try:
                actual_bronze_count = spark.read.format("delta").load(bronze_table_path).count()
                log_funnel_count(entity=entity, layer="bronze", count=actual_bronze_count)
            except Exception as e:
                logging.warning(f"Failed to count Bronze table for {entity}: {e}")

    logging.info("Batch execution completed! Shutting down Spark.")
    print("All streaming queries completed and metrics logged.")


if __name__ == "__main__":
    stream_raw_to_bronze()
