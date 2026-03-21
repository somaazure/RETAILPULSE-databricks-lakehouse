# Skill: Bronze Pipeline Generator

Create a Databricks Auto Loader ingestion pipeline for RetailPulse orders data using Unity Catalog volumes and an `availableNow` trigger.

## Objective

Ingest CSV files from `/Volumes/retailpulse/bronze/orders_files/orders/` into the Unity Catalog table `retailpulse.bronze.orders` using Auto Loader with schema inference, checkpointing, and bronze-level audit columns.

## Requirements

- Source format: CSV
- Source path: `/Volumes/retailpulse/bronze/orders_files/orders/`
- Target table: `retailpulse.bronze.orders`
- Use Auto Loader with schema inference
- Use a persistent schema location
- Use a persistent checkpoint location
- Use Unity Catalog volume storage instead of `FileStore`
- Read CSV headers
- Write in append mode
- Use `availableNow` trigger for cluster compatibility
- Sink format: Delta table
- Capture `_rescued_data`, source file path, and ingest timestamp

## Recommended PySpark Pattern

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

spark = SparkSession.builder.getOrCreate()

source_path = "/Volumes/retailpulse/bronze/orders_files/orders/"
schema_location = "/Volumes/retailpulse/bronze/orders_files/checkpoints/orders_schema"
checkpoint_location = "/Volumes/retailpulse/bronze/orders_files/checkpoints/bronze_orders"
target_table = "retailpulse.bronze.orders"

spark.sql("CREATE CATALOG IF NOT EXISTS retailpulse")
spark.sql("CREATE SCHEMA IF NOT EXISTS retailpulse.bronze")
spark.sql("CREATE VOLUME IF NOT EXISTS retailpulse.bronze.orders_files")

orders_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.rescuedDataColumn", "_rescued_data")
    .option("header", "true")
    .load(source_path)
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
    orders_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_location)
    .trigger(availableNow=True)
    .toTable(target_table)
)

query.awaitTermination()
```

## Notes

- Use Unity Catalog volumes when public DBFS root is disabled.
- Auto Loader tracks discovered files incrementally and uses the checkpoint to avoid reprocessing.
- `cloudFiles.schemaLocation` persists inferred schema metadata across runs.
- `availableNow=True` is a good fit for cluster types that do not support infinite `processingTime` triggers.
- For near-continuous ingestion, schedule the notebook as a Databricks Job to run every few minutes.
- In Unity Catalog-enabled environments, use `_metadata.file_path` instead of `input_file_name()`.

## Optional Setup

```python
spark.sql("CREATE CATALOG IF NOT EXISTS retailpulse")
spark.sql("CREATE SCHEMA IF NOT EXISTS retailpulse.bronze")
spark.sql("CREATE VOLUME IF NOT EXISTS retailpulse.bronze.orders_files")
```

## Expected Columns

- `order_id`
- `customer_id`
- `product_id`
- `quantity`
- `price`
- `_rescued_data`
- `source_file_name`
- `ingest_ts`

## Validation Checks

- Confirm new CSV files arrive in `/Volumes/retailpulse/bronze/orders_files/orders/`
- Confirm checkpoint files are created under `/Volumes/retailpulse/bronze/orders_files/checkpoints/bronze_orders`
- Confirm inferred schema metadata is created under `/Volumes/retailpulse/bronze/orders_files/checkpoints/orders_schema`
- Query `retailpulse.bronze.orders` to verify new rows are appended after each run
- Check `_rescued_data` for malformed or schema-drifted input rows
