# Skill: Bronze Pipeline Generator

Create a Databricks Bronze ingestion pipeline for RetailPulse orders data using Unity Catalog volumes, Auto Loader, and either classic streaming notebooks or Delta Live Tables (DLT).

## Objective

Ingest CSV files from `/Volumes/retailpulse/bronze/orders_files/orders/` into the Unity Catalog table `retailpulse.bronze.orders` using Auto Loader with schema inference, schema evolution, checkpointing or equivalent source-state tracking, and bronze-level audit columns.

## Requirements

- Source format: CSV
- Source path: `/Volumes/retailpulse/bronze/orders_files/orders/`
- Target table: `retailpulse.bronze.orders`
- Use Auto Loader with schema inference
- Use a persistent schema location
- Use persistent checkpointing or DLT-managed source tracking
- Use Unity Catalog volume storage instead of `FileStore`
- Read CSV headers
- Capture `_rescued_data`, source file path, and ingest timestamp
- Keep backward compatibility for non-DLT pipelines

## Preferred Production Pattern

Where applicable, implement Bronze ingestion with DLT.

Use DLT when you want:

- managed streaming ingestion
- built-in observability
- schema evolution handling
- cleaner integration with Silver-layer data quality processing

Use a classic notebook stream when DLT is not part of the requested architecture.

## Recommended DLT Pattern

```python
import dlt
from pyspark.sql import functions as F

source_path = "/Volumes/retailpulse/bronze/orders_files/orders/"
schema_location = "/Volumes/retailpulse/bronze/orders_files/checkpoints/orders_schema"

@dlt.table(name="retailpulse.bronze.orders")
def bronze_orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .option("header", "true")
        .load(source_path)
        .select(
            F.col("order_id"),
            F.col("customer_id"),
            F.col("product_id"),
            F.col("quantity"),
            F.col("price"),
            F.col("_rescued_data"),
            F.col("_metadata.file_path").alias("source_file_name"),
            F.current_timestamp().alias("ingest_ts"),
        )
    )
```

## Recommended Non-DLT Pattern

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

spark = SparkSession.builder.getOrCreate()

source_path = "/Volumes/retailpulse/bronze/orders_files/orders/"
schema_location = "/Volumes/retailpulse/bronze/orders_files/checkpoints/orders_schema"
checkpoint_location = "/Volumes/retailpulse/bronze/orders_files/checkpoints/bronze_orders"
target_table = "retailpulse.bronze.orders"

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
- Auto Loader tracks discovered files incrementally and uses checkpointing or DLT-managed state to avoid reprocessing.
- `cloudFiles.schemaLocation` persists inferred schema metadata across runs.
- Include `_rescued_data`, file metadata, and ingest timestamp for downstream Silver validation.
- In Unity Catalog-enabled environments, use `_metadata.file_path` instead of `input_file_name()`.

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
- Confirm schema metadata is created under `/Volumes/retailpulse/bronze/orders_files/checkpoints/orders_schema`
- Confirm new rows arrive in `retailpulse.bronze.orders`
- Check `_rescued_data` for malformed or schema-drifted input rows
- Confirm ingestion metadata is populated for traceability
