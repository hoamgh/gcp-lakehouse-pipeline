import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, schema_of_json, lit
from pyspark.sql.types import StringType

# Import schemas from config
sys.path.append(os.path.dirname(__file__))
from config import (
    STAGING_DIR, BRONZE_DIR, DLQ_DIR, ENTITY_SCHEMAS, get_spark_session
)

PROJECT_ID = os.environ.get("PROJECT_ID")
SUBSCRIPTION_ID = os.environ.get("SUBSCRIPTION_ID")

def stream_raw_to_bronze():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("Starting Batch Execution: Raw -> Bronze")
    
    spark = get_spark_session("StreamRawToBronze")
    spark.sparkContext.setLogLevel("WARN")
    
    subscription_path = f"projects/{PROJECT_ID}/subscriptions/{SUBSCRIPTION_ID}"
    
    print(f"Starting Structured Streaming from Pub/Sub: {subscription_path}")
    
    from logger import log_quality_check

    queries = []
    
    for entity in ENTITY_SCHEMAS.keys():
        print(f"Setting up stream for entity: {entity}")
        
        schema, _ = ENTITY_SCHEMAS[entity]
        
        # Ensure directory exists before starting the stream (only for local paths)
        source_dir = f"{STAGING_DIR}/{entity}"
        if not source_dir.startswith("gs://"):
            os.makedirs(source_dir, exist_ok=True)
        
        # Add _corrupt_record to the schema to catch schema errors
        from pyspark.sql.types import StructField, StringType
        schema_with_corrupt = schema.add(StructField("_corrupt_record", StringType(), True))
        
        # Read from raw JSON files as a stream
        raw_stream = spark.readStream \
            .format("json") \
            .schema(schema_with_corrupt) \
            .option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt_record") \
            .load(source_dir)
            
        # Add metadata columns
        df_annotated = raw_stream \
            .withColumn("_ingested_at", current_timestamp()) \
            .withColumn("source", lit("streaming"))
            
        # Split logic: Schema check (CHK node in architecture)
        # Valid records go to Bronze, corrupted records go to DLQ
        df_bronze = df_annotated.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
        df_dlq = df_annotated.filter(col("_corrupt_record").isNotNull())
        
        # Write to Bronze Delta using AvailableNow trigger
        query_bronze = df_bronze.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", f"{BRONZE_DIR}/_checkpoints/{entity}") \
            .trigger(availableNow=True) \
            .start(f"{BRONZE_DIR}/{entity}")
            
        queries.append({"query": query_bronze, "type": "bronze", "entity": entity})
        logging.info(f"Started batch processing for entity: {entity}")
            
        # Write to DLQ JSON using AvailableNow trigger
        query_dlq = df_dlq.writeStream \
            .format("json") \
            .option("checkpointLocation", f"{DLQ_DIR}/_checkpoints/{entity}") \
            .trigger(availableNow=True) \
            .start(f"{DLQ_DIR}/{entity}")
            
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
        
    logging.info("Batch execution completed! Shutting down Spark.")
    print("All streaming queries completed and metrics logged.")

if __name__ == "__main__":
    stream_raw_to_bronze()
