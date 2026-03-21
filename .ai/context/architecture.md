# Architecture

## Overview

RetailPulse is a Databricks Lakehouse project built on Unity Catalog with a medallion-style design.

- Platform: Databricks Lakehouse
- Catalog: `retailpulse`
- Storage pattern: Unity Catalog volumes for landed files and Delta tables for curated layers
- Processing pattern: Auto Loader ingestion plus batch transformations, coordinated through DLT and Databricks Jobs

## Layers

- Bronze: raw orders ingestion from a Unity Catalog volume into Delta
- Silver: validated and deduplicated business-ready tables
- Gold: dimensional and fact tables for analytics
- Ops: quarantine tables for rejected or unresolved records

## Source And Ingestion

Orders are generated as CSV files and written to the Unity Catalog volume path:

`/Volumes/retailpulse/bronze/orders_files/orders/`

Auto Loader ingests those files into:

`retailpulse.bronze.orders`

The bronze ingestion process also stores schema inference and checkpoint state under:

`/Volumes/retailpulse/bronze/orders_files/checkpoints/`

## Table Relationships

```text
/Volumes/retailpulse/bronze/orders_files/orders/
                  |
                  v
+----------------------------------+
| retailpulse.bronze.orders        |
|----------------------------------|
| order_id                         |
| customer_id                      |
| product_id                       |
| quantity                         |
| price                            |
| _rescued_data                    |
| source_file_name                 |
| ingest_ts                        |
+----------------+-----------------+
                 |
                 | clean, validate, deduplicate
                 v
+----------------------------------+
| retailpulse.silver.orders        |
|----------------------------------|
| order_id                         |
| customer_id                      |
| product_id                       |
| quantity                         |
| price                            |
| source_file_name                 |
| ingest_ts                        |
+----------------+-----------------+
                 |                        \
                 |                         \
                 |                          \
                 v                           v
+----------------------------------+   +----------------------------------+
| retailpulse.gold.dim_customer    |   | retailpulse.gold.dim_date        |
|----------------------------------|   |----------------------------------|
| customer_id                      |   | date_id                          |
| customer_name                    |   | full_date                        |
| customer_segment                 |   | day                              |
| customer_status                  |   | month                            |
+----------------------------------+   | year                             |
                                       | week_of_year                     |
                                       +----------------------------------+

+----------------------------------+
| retailpulse.silver.products      |
|----------------------------------|
| product_id                       |
| product_name                     |
| category_id                      |
| price                            |
+----------------+-----------------+
                 |
                 | SCD Type 2 source
                 v
+----------------------------------+
| retailpulse.gold.dim_product     |
|----------------------------------|
| product_sk                       |
| product_id                       |
| product_name                     |
| category_id                      |
| current_price                    |
| start_date                       |
| end_date                         |
| is_current                       |
+----------------+-----------------+
                 |
                 | joins into fact_sales
                 v
+----------------------------------+
| retailpulse.gold.fact_sales      |
|----------------------------------|
| order_id                         |
| product_sk                       |
| customer_id                      |
| date_id                          |
| quantity                         |
| price                            |
| sales_amount                     |
| order_ts                         |
+----------------------------------+
```

## Business Meaning

- `retailpulse.bronze.orders` stores raw landed order events with minimal transformation.
- `retailpulse.silver.orders` stores trusted order records after type casting, validation, and deduplication.
- `retailpulse.silver.products` stores the current product master data used to seed product dimensions.
- `retailpulse.gold.dim_product` stores product history using SCD Type 2 logic.
- `retailpulse.gold.dim_customer` stores one row per customer derived from silver orders.
- `retailpulse.gold.dim_date` stores one row per calendar date derived from silver order timestamps.
- `retailpulse.gold.fact_sales` stores sales transactions joined to the gold dimensions.
- `retailpulse.ops.silver_orders_quarantine` stores rejected Silver-order rows with `dq_reason`.
- `retailpulse.ops.fact_sales_quarantine` stores unresolved fact rows, such as missing product/date/customer matches, with `dq_reason`.

## Relationship Summary

- One product can appear in many order records.
- One customer can place many orders.
- One calendar date can have many orders.
- `retailpulse.gold.dim_product` keeps multiple historical rows for one `product_id`, but only one row should be current.
- `retailpulse.gold.fact_sales` joins:
  - `product_id` to the current `dim_product` row to get `product_sk`
  - `customer_id` to `dim_customer`
  - `to_date(order_ts)` to `dim_date.full_date`

## SCD Join Strategy (Current Implementation)

- Fact tables currently join `dim_product` using `product_id` and `is_current = true`.
- This approach is used because the synthetic sample data has a timing mismatch between order timestamps and product effective dates.
- In production, this will be replaced with a historical date-range join:
  `fact_timestamp BETWEEN effective_start_ts AND effective_end_ts`

## Technology Choices

- PySpark for data engineering logic
- Delta Lake for ACID tables and merge support
- Unity Catalog for table governance and volumes
- Auto Loader for file-based incremental ingestion
- Lakeflow / Delta Live Tables for data quality enforcement, observability, and quarantine-style exception handling
- Databricks Jobs for Gold-layer orchestration and batch processing
- SCD Type 2 for dimensional history tracking

## DLT Quality Pattern

- DLT notebook: `notebooks/08_dlt_e2e_main_refresh.py`
- DLT config: `config/dlt_bronze_silver_pipeline.json`
- Pipeline type: triggered serverless DLT pipeline for Bronze and Silver
- Sync helper: `sync_to_workspace.ps1`

Current DLT design choices:

- DLT handles ingestion, schema evolution, and data quality for Bronze and Silver.
- `retailpulse.bronze.orders` is ingested from the Unity Catalog volume using Auto Loader.
- `retailpulse.silver.orders` applies validation, casting, and deduplication.
- `retailpulse.ops.silver_orders_quarantine` stores invalid Silver rows with `dq_reason`.
- Gold dimensional modeling and fact creation are handled outside DLT in batch jobs.

## Pipeline Orchestration Strategy

A lightweight orchestrator job coordinates the full pipeline execution.

- Task 1: trigger the DLT pipeline in triggered mode to process Bronze and Silver for streaming ingestion and data quality handling.
- Task 2: trigger the Gold-layer Databricks job to build `dim_product` with SCD Type 2, `dim_customer`, `dim_date`, and `fact_sales`.

Execution behavior:

- DLT handles ingestion, schema evolution, and data quality for Bronze and Silver.
- The Gold job handles dimensional modeling and fact table creation using batch processing.
- Tasks run sequentially with dependency, so Gold starts only after the DLT task completes successfully.

Benefits:

- modular pipeline design
- independent scalability
- better failure isolation
- flexible scheduling

## Final Execution Steps

Run the pipeline in this order in Databricks:

1. Orchestrator job
   - Triggers the Bronze/Silver DLT pipeline first.
   - Triggers the Gold batch job only after DLT succeeds.

2. DLT pipeline task
   - Ingests files into `retailpulse.bronze.orders`
   - Cleans and deduplicates into `retailpulse.silver.orders`
   - Captures invalid Silver rows in `retailpulse.ops.silver_orders_quarantine`

3. Gold batch job
   - Builds `retailpulse.silver.products`
   - Maintains `retailpulse.gold.dim_product`
   - Builds `retailpulse.gold.dim_customer`
   - Builds `retailpulse.gold.dim_date`
   - Builds `retailpulse.gold.fact_sales`
   - Captures unresolved facts in `retailpulse.ops.fact_sales_quarantine`

## Validation Queries

Use these checks after the pipeline runs:

```sql
SELECT COUNT(*) FROM retailpulse.bronze.orders;
SELECT COUNT(*) FROM retailpulse.silver.orders;
SELECT COUNT(*) FROM retailpulse.silver.products;
SELECT COUNT(*) FROM retailpulse.gold.dim_product;
SELECT COUNT(*) FROM retailpulse.gold.dim_customer;
SELECT COUNT(*) FROM retailpulse.gold.dim_date;
SELECT COUNT(*) FROM retailpulse.gold.fact_sales;
SELECT COUNT(*) FROM retailpulse.ops.silver_orders_quarantine;
SELECT COUNT(*) FROM retailpulse.ops.fact_sales_quarantine;
```

## Current Scope

Implemented:

- order file generation
- bronze orders ingestion
- silver orders transformation
- silver products seed table
- SCD2 `dim_product`
- `dim_customer`
- `dim_date`
- `fact_sales`
- DLT Bronze/Silver pipeline with quarantine handling
- orchestrator-based enterprise workflow

Potential next enhancements:

- add a true business `order_timestamp` instead of relying on `ingest_ts`
- add `customer_sk` surrogate key to `dim_customer`
- add fact table audit columns and load timestamps
- optimize Delta tables with `OPTIMIZE` and `ZORDER`
- extend quarantine flows into a formal reprocessing framework with remediation status and replay logic
