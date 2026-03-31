# Bronze Pipeline Creation Prompt

Create a Databricks Bronze ingestion pipeline for RetailPulse data using Auto Loader and Unity Catalog.

## Objective

Ingest raw data files into Bronze layer tables with minimal transformation, preserving source fidelity and capturing metadata.

## Configuration

* **Source Path**: `/Volumes/retailpulse/bronze/<entity>_files/`
* **Target Tables**: `retailpulse.bronze.<entity>`
* **File Formats**: CSV, JSON, Parquet
* **Ingestion Method**: Auto Loader with cloudFiles

## Auto Loader Pattern

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="bronze_orders",
    comment="Raw orders data from source files"
)
def bronze_orders():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaLocation", "/Volumes/retailpulse/bronze/orders_files/checkpoints/schema")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.rescuedDataColumn", "_rescued_data")
            .option("header", "true")
            .load("/Volumes/retailpulse/bronze/orders_files/orders/")
            .withColumn("ingest_ts", F.current_timestamp())
            .withColumn("source_file_name", F.col("_metadata.file_path"))
    )
```

## Required Columns

* All source columns (schema inferred or explicit)
* `_rescued_data` - malformed/unexpected data capture
* `ingest_ts` - ingestion timestamp for audit
* `source_file_name` - source file path for lineage

## Schema Management

* Use `cloudFiles.schemaLocation` to persist inferred schema
* Enable `cloudFiles.schemaEvolutionMode` for automatic schema changes
* Store schema metadata under `/Volumes/retailpulse/bronze/<entity>_files/checkpoints/`
