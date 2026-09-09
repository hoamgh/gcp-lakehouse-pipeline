import logging
import os

from config import BRONZE_DIR, DEDUP_KEYS, ENTITIES, SILVER_DIR, get_spark_session
from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, row_number, when
from pyspark.sql.window import Window


def upsert_to_delta(microBatchOutputDF, batchId, entity, pks, silver_path):
    """
    foreachBatch function to perform MERGE (UPSERT) into Silver Delta tables.
    """
    spark = microBatchOutputDF.sparkSession

    # 1. Deduplicate the micro-batch itself
    if entity == "shipments":
        # For CDC, keep the latest event based on event_timestamp
        window_spec = Window.partitionBy(pks[0]).orderBy(col("event_timestamp").desc())
        deduped_df = (
            microBatchOutputDF.withColumn("rn", row_number().over(window_spec))
            .filter(col("rn") == 1)
            .drop("rn")
        )
    else:
        # For append-only entities, standard dedup is fine
        deduped_df = microBatchOutputDF.dropDuplicates(pks)

    # 2. Check if Silver table exists
    if DeltaTable.isDeltaTable(spark, silver_path):
        delta_table = DeltaTable.forPath(spark, silver_path)

        # Add newly introduced nullable columns before MERGE. Delta's mergeSchema
        # does not evolve columns referenced by the merge condition itself.
        target_columns = set(delta_table.toDF().columns)
        missing_fields = [
            field for field in deduped_df.schema.fields if field.name not in target_columns
        ]
        if missing_fields:
            escaped_path = silver_path.replace("`", "``")
            column_definitions = ", ".join(
                f"`{field.name.replace('`', '``')}` {field.dataType.simpleString()}"
                for field in missing_fields
            )
            logging.info(
                "Evolving Silver schema for %s with columns: %s",
                entity,
                [field.name for field in missing_fields],
            )
            spark.sql(
                f"ALTER TABLE delta.`{escaped_path}` ADD COLUMNS ({column_definitions})"
            )
            delta_table = DeltaTable.forPath(spark, silver_path)

        # Build merge condition
        merge_cond = " AND ".join([f"target.{k} = source.{k}" for k in pks])

        # 3. Perform MERGE
        merge = delta_table.alias("target").merge(deduped_df.alias("source"), merge_cond)
        if entity == "shipments":
            update_values = {
                column: f"coalesce(source.{column}, target.{column})"
                for column in deduped_df.columns
            }
            merge = merge.whenMatchedUpdate(
                condition="source.event_timestamp >= target.event_timestamp",
                set=update_values,
            )
        else:
            merge = merge.whenMatchedUpdateAll()
        merge.whenNotMatchedInsertAll().execute()
    else:
        # Table doesn't exist yet, write the first batch directly
        deduped_df.write.format("delta").mode("append").save(silver_path)


def stream_bronze_to_silver():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logging.info("Starting Batch Execution: Bronze -> Silver")

    spark = get_spark_session("StreamBronzeToSilver")
    spark.sparkContext.setLogLevel("WARN")

    queries = []

    for entity in ENTITIES:
        pks = DEDUP_KEYS.get(entity)
        if not pks:
            continue
        print(f"Starting Stream Bronze -> Silver for {entity} (PKs: {pks})")

        bronze_path = f"{BRONZE_DIR}/{entity}"
        silver_path = f"{SILVER_DIR}/silver_{entity}"
        checkpoint_path = f"{SILVER_DIR}/_checkpoints/{entity}"

        # Check if Bronze exists before streaming (local only)
        if not bronze_path.startswith("gs://") and not os.path.exists(bronze_path):
            print(f"  [Skip] Bronze path not found: {bronze_path}")
            continue

        bronze_stream = spark.readStream.format("delta").load(bronze_path)

        # We don't need _ingested_at from Bronze, we clean it up
        clean_stream = bronze_stream.drop("_ingested_at", "source")

        # --- Data Quality Cleaning at Silver Layer ---
        if entity == "customers":
            # Fill missing emails with a default placeholder
            clean_stream = clean_stream.withColumn(
                "email",
                when(col("email").isNull(), lit("no-email@example.com")).otherwise(col("email")),
            )

        # Write to Silver using foreachBatch and AvailableNow trigger
        query = (
            clean_stream.writeStream.format("delta")
            .foreachBatch(
                lambda df, epoch_id, e=entity, p=pks, sp=silver_path: upsert_to_delta(
                    df, epoch_id, e, p, sp
                )
            )
            .option("checkpointLocation", checkpoint_path)
            .trigger(availableNow=True)
            .start()
        )

        queries.append({"query": query, "entity": entity})
        logging.info(f"Started batch upsert for entity: {entity}")

    logging.info("Waiting for all entities to finish upserting this batch...")

    from logger import log_funnel_count, log_performance

    for q_dict in queries:
        q = q_dict["query"]
        entity = q_dict["entity"]
        q.awaitTermination()

        # Calculate performance metrics from recent micro-batches
        total_input = 0
        total_duration_ms = 0
        if q.recentProgress:
            total_input = sum([rp.get("numInputRows", 0) for rp in q.recentProgress])
            # sum up the total execution time of this batch across micro-batches
            for rp in q.recentProgress:
                durations = rp.get("durationMs", {})
                # addBatch is the time spent in foreachBatch
                total_duration_ms += (
                    durations.get("addBatch", 0)
                    + durations.get("getBatch", 0)
                    + durations.get("queryPlanning", 0)
                )

        if total_input > 0:
            duration_seconds = total_duration_ms / 1000.0
            log_performance(
                entity=entity,
                duration_seconds=duration_seconds,
                output_rows=total_input,
                layer="silver",
            )

        # Funnel Reconciliation: Read actual count from Silver Delta table
        silver_table_path = f"{SILVER_DIR}/silver_{entity}"
        if os.path.exists(silver_table_path) or silver_table_path.startswith("gs://"):
            try:
                actual_silver_count = spark.read.format("delta").load(silver_table_path).count()
                log_funnel_count(entity=entity, layer="silver", count=actual_silver_count)
            except Exception as e:
                logging.warning(f"Failed to count Silver table for {entity}: {e}")

    logging.info("Batch execution completed! Shutting down Spark.")
    print("All Silver streaming queries completed and metrics logged.")


if __name__ == "__main__":
    stream_bronze_to_silver()
