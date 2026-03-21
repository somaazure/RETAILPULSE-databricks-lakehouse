# Architecture

## Overview

RetailPulse is a Databricks Lakehouse project built on Unity Catalog with a medallion-style design.

- Platform: Databricks Lakehouse
- Catalog: `retailpulse`
- Storage pattern: Unity Catalog volumes for landed files and Delta tables for curated layers
- Processing pattern: Auto Loader ingestion plus batch transformations, with a DLT quality pipeline for curated validation and quarantine handling

## Layers

- Bronze: raw orders ingestion from a Unity Catalog volume into Delta
- Silver: validated and deduplicated business-ready tables
- Gold: dimensional and fact tables for analytics
- DLT target schema: DLT-managed quality tables in `retailpulse.dlt`

## Source And Ingestion

Orders are generated as CSV files and written to the Unity Catalog volume path:

`/Volumes/retailpulse/bronze/orders_files/orders/`

Auto Loader ingests those files into:

`retailpulse.bronze.orders`

The bronze ingestion process also stores:

- schema inference state in `/Volumes/retailpulse/bronze/orders_files/checkpoints/orders_schema`
- checkpoint state in `/Volumes/retailpulse/bronze/orders_files/checkpoints/bronze_orders`

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
- `retailpulse.dlt.silver_orders_dlt` stores DLT-curated silver orders that passed enforced silver-quality filters.
- `retailpulse.dlt.silver_orders_quarantine` stores rejected silver-order rows with `dq_reason`.
- `retailpulse.dlt.dim_product_current_dlt`, `retailpulse.dlt.dim_customer_dlt`, `retailpulse.dlt.dim_date_dlt`, and `retailpulse.dlt.silver_products_dlt` store DLT-managed validated views of the core dimensions and product source.
- `retailpulse.dlt.fact_sales_dlt` stores only fully resolved fact rows that successfully join to all required dimensions.
- `retailpulse.dlt.fact_sales_quarantine` stores unresolved fact rows, such as missing product/date/customer matches, with `dq_reason`.

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
- SCD Type 2 for dimensional history tracking

## DLT Quality Pattern

- DLT notebook: `notebooks/06_dlt_pipeline.py`
- DLT config: `config/dlt_pipeline_config.json`
- Pipeline type: triggered serverless DLT pipeline targeting `retailpulse.dlt`
- Sync helper: `sync_to_workspace.ps1`

Current DLT design choices:

- `silver_orders_dlt` reads the persisted bronze Delta table in batch mode, not stream mode.
- This was chosen because the current silver dedup pattern uses an aggregate plus join-back-to-source, which is simpler and more reliable in triggered batch DLT than in streaming mode.
- `silver_orders_quarantine` is the operational bucket for invalid silver rows.
- `fact_sales_dlt` uses inner joins to dimensions so only fully resolved facts land in the curated DLT fact table.
- `fact_sales_quarantine` uses left joins and captures unresolved rows for remediation instead of failing the entire pipeline.
- Soft `@dlt.expect(...)` checks record quality metrics without rejecting rows.
- Enforced filtering is handled either through explicit transformation filters or quarantine routing.

## Final Execution Steps

Run the notebooks in this order in Databricks:

1. `01_data_generator.py`
   - Continuously creates synthetic order CSV files in the Unity Catalog volume.

2. `02_bronze_ingestion.py`
   - Ingests available order files from the volume into `retailpulse.bronze.orders` using Auto Loader and `availableNow`.

3. `03_silver_transform.py`
   - Cleans, validates, casts, and deduplicates bronze orders into `retailpulse.silver.orders`.

4. `04_dim_tables.py`
   - Builds `retailpulse.silver.products`
   - Maintains `retailpulse.gold.dim_product`
   - Builds `retailpulse.gold.dim_customer`
   - Builds `retailpulse.gold.dim_date`

5. `05_fact_tables.py`
   - Joins `retailpulse.silver.orders` to the gold dimensions and writes `retailpulse.gold.fact_sales`.

6. `06_dlt_pipeline.py`
   - Builds DLT-managed curated and quarantine tables in `retailpulse.dlt`
   - Applies data quality expectations and captures rejects for remediation

## Validation Queries

Use these checks after the pipeline runs:

```sql
SELECT COUNT(*) FROM retailpulse.bronze.orders;
SELECT COUNT(*) FROM retailpulse.silver.orders;
SELECT COUNT(*) FROM retailpulse.gold.dim_product;
SELECT COUNT(*) FROM retailpulse.gold.dim_customer;
SELECT COUNT(*) FROM retailpulse.gold.dim_date;
SELECT COUNT(*) FROM retailpulse.gold.fact_sales;
SELECT COUNT(*) FROM retailpulse.dlt.silver_orders_dlt;
SELECT COUNT(*) FROM retailpulse.dlt.silver_orders_quarantine;
SELECT COUNT(*) FROM retailpulse.dlt.fact_sales_dlt;
SELECT COUNT(*) FROM retailpulse.dlt.fact_sales_quarantine;
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
- DLT quality pipeline with curated and quarantine outputs

Potential next enhancements:

- add a true business `order_timestamp` instead of relying on `ingest_ts`
- add `customer_sk` surrogate key to `dim_customer`
- add fact table audit columns and load timestamps
- optimize Delta tables with `OPTIMIZE` and `ZORDER`
- extend quarantine flows into a formal reprocessing framework with remediation status and replay logic
