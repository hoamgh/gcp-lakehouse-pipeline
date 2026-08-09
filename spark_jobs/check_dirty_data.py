import sys
import os
sys.path.append(os.path.dirname(__file__))
import config

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("CheckDirtyData") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\n--- CHECKING SILVER ORDERS FOR 'TEST_STATUS' ---")
df_orders = spark.read.format("delta").load("../output/silver/silver_orders")
dirty_orders = df_orders.filter(col("order_status") == "TEST_STATUS")
dirty_orders.select("order_id", "order_status").show(truncate=False)

print("\n--- CHECKING SILVER ORDER_ITEMS FOR NEGATIVE PRICE ---")
df_items = spark.read.format("delta").load("../output/silver/silver_order_items")
dirty_items = df_items.filter(col("price") < 0)
dirty_items.select("order_id", "product_id", "price").show(truncate=False)
