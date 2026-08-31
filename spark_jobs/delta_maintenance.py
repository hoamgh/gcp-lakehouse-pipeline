from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import argparse
import logging
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_maintenance(spark: SparkSession, table_path: str, retention_hours: int = 168):
    """
    Runs OPTIMIZE and VACUUM on a Delta Table.
    Retention is strictly enforced at 168 hours (7 days) for safety and time-travel.
    """
    logger.info(f"Starting maintenance for Delta table at: {table_path}")
    
    if not DeltaTable.isDeltaTable(spark, table_path):
        logger.error(f"Path is not a valid Delta table: {table_path}")
        return

    delta_table = DeltaTable.forPath(spark, table_path)
    
    # 1. OPTIMIZE: Coalesce small files into larger optimal sizes
    logger.info("Executing OPTIMIZE...")
    try:
        # Use executeCompaction if open-source Delta Spark supports it, or simply optimize()
        delta_table.optimize().executeCompaction()
        logger.info("OPTIMIZE completed.")
    except Exception as e:
        logger.warning(f"OPTIMIZE failed or not fully supported in this version: {e}")

    # 2. VACUUM: Remove tombstones older than retention_hours
    logger.info(f"Executing VACUUM with retention {retention_hours} hours...")
    try:
        delta_table.vacuum(retention_hours)
        logger.info("VACUUM completed.")
    except Exception as e:
        logger.error(f"VACUUM failed: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tables", required=True, help="Comma-separated list of GCS paths to Delta tables")
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("Delta_Lake_Maintenance_Job") \
        .getOrCreate()

    tables = [t.strip() for t in args.tables.split(",")]
    
    for tbl in tables:
        run_maintenance(spark, tbl)
        
    spark.stop()
    logger.info("All maintenance tasks finished.")
