# Databricks notebook source
"""Bronze ingestion pipeline for RetailPulse orders using Databricks Auto Loader."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

spark = SparkSession.builder.getOrCreate()

SOURCE_PATH = "/Volumes/retailpulse/bronze/orders_files/orders/"
SCHEMA_LOCATION = "/Volumes/retailpulse/bronze/orders_files/checkpoints/orders_schema"
CHECKPOINT_LOCATION = "/Volumes/retailpulse/bronze/orders_files/checkpoints/bronze_orders"
TARGET_TABLE = "retailpulse.bronze.orders"

spark.sql("CREATE CATALOG IF NOT EXISTS retailpulse")
spark.sql("CREATE SCHEMA IF NOT EXISTS retailpulse.bronze")
spark.sql("CREATE VOLUME IF NOT EXISTS retailpulse.bronze.orders_files")

orders_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.rescuedDataColumn", "_rescued_data")
    .option("header", "true")
    .load(SOURCE_PATH)
    .select(
        col("order_id"),
        col("customer_id"),
        col("product_id"),
        col("quantity"),
        col("price"),
        col("_rescued_data"),
        col("_metadata.file_path").alias("source_file_name"),
        current_timestamp().alias("ingest_ts"),
    )
)

query = (
    orders_stream.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
)

query.awaitTermination()

print(f"Completed bronze ingestion for {SOURCE_PATH} -> {TARGET_TABLE}")
print(f"Checkpoint location: {CHECKPOINT_LOCATION}")
print(f"Schema location: {SCHEMA_LOCATION}")
print(f"Streaming query id: {query.id}")
