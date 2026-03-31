# DLT Pipeline Creation Prompt

Create a Databricks Delta Live Tables (DLT) pipeline following medallion architecture (Bronze → Silver → Gold) for RetailPulse.

## Pipeline Configuration

* **Pipeline Name**: `retailpulse_bronze_silver_pipeline`
* **Target Catalog**: `retailpulse`
* **Target Schemas**: `bronze`, `silver`, `gold`
* **Source**: Unity Catalog Volumes (`/Volumes/retailpulse/bronze/`)
* **Storage Location**: Unity Catalog Managed Storage

## Architecture Layers

**Bronze Layer** (Raw Ingestion):
* Use Auto Loader with `cloudFiles` format
* Ingest from `/Volumes/retailpulse/bronze/<entity>_files/`
* Schema inference with `cloudFiles.inferColumnTypes`
* Schema evolution with `cloudFiles.schemaEvolutionMode: addNewColumns`
* Capture `_rescued_data`, `_metadata.file_path`, `ingest_ts`
* Create streaming tables with `@dp.table()` or `@dlt.table()`

**Silver Layer** (Cleaned & Validated):
* Read from bronze streaming tables with `spark.readStream.table()`
* Apply data quality expectations (`@dp.expect_or_drop()`, `@dp.expect()`)
* Implement SCD Type 2 with `dp.create_auto_cdc_flow()` where needed
* Deduplicate, cleanse, and standardize data
* Add business logic columns (date parts, derived fields)

**Gold Layer** (Business Aggregates):
* Use materialized views with `@dp.materialized_view()`
* Batch read from silver tables with `spark.read.table()`
* Create aggregated metrics, KPIs, and denormalized views
* Optimize with liquid clustering or partitioning

## Directory Structure

```
RetailPulse/
├── transformations/
│   ├── bronze/
│   │   ├── orders.py
│   │   ├── customers.py
│   │   └── products.py
│   ├── silver/
│   │   ├── orders.py
│   │   ├── customers_scd2.py
│   │   └── products.py
│   └── gold/
│       ├── sales_summary.py
│       └── customer_metrics.py
```

## Implementation Guidelines

* Prefer `from pyspark import pipelines as dp` over `import dlt`
* Use Unity Catalog volumes instead of DBFS FileStore
* Use schema locations under `/Volumes/retailpulse/bronze/<entity>_files/checkpoints/`
* Follow naming convention: `bronze_<entity>`, `silver_<entity>`, `gold_<aggregate>`
* Add table properties: `pipelines.autoOptimize.optimizeWrite: true`
* Include comments on all datasets describing purpose and source
