# Project Skills Guide

This folder contains project-specific skills for building RetailPulse pipelines on Databricks.

## Available Skills

### `bronze_pipeline.md`

Use for:

- Auto Loader ingestion
- raw Bronze landing tables
- schema inference and schema evolution
- checkpoint and schemaLocation setup
- rescued data capture

Example prompt:

```text
Use the bronze_pipeline skill and build a Databricks Auto Loader notebook for retail orders CSV files into retailpulse.bronze.orders with schemaLocation, checkpointLocation, rescued data column, and availableNow trigger.
```

### `silver_transform.md`

Use for:

- casting and standardization
- null handling
- duplicate removal
- row-level validation
- DLT silver curation and quarantine logic

Example prompt:

```text
Use the silver_transform skill and create a DLT version of the silver orders pipeline with expectations, a curated silver_orders_dlt table, and a silver_orders_quarantine table with dq_reason.
```

### `scd2_merge.md`

Use for:

- SCD Type 2 dimension maintenance
- merge logic for current and expired rows
- business-key tracking
- start and end date handling

Example prompt:

```text
Use the scd2_merge skill and build an SCD Type 2 dim_product pipeline from retailpulse.silver.products into retailpulse.gold.dim_product using product_id as business key and tracking category_id and price changes.
```

### `fact_table.md`

Use for:

- fact table construction
- current-mode and historical-mode dimension joins
- surrogate key resolution
- DLT fact quarantine design for unresolved joins

Example prompt:

```text
Use the fact_table skill and create a DLT fact_sales_dlt table plus fact_sales_quarantine so unresolved dimension joins are preserved instead of failing the whole pipeline.
```

### `streaming_pipeline.md`

Use for:

- true streaming pipeline design
- continuous or near-real-time processing patterns

Use only when the solution really needs streaming semantics end to end.

Example prompt:

```text
Use the streaming_pipeline skill to design a real-time inventory event pipeline with streaming bronze ingestion, streaming silver enrichment, and low-latency gold aggregates.
```

## Supporting Context

These project context files help the skills stay aligned with the RetailPulse implementation:

- `.ai/context/architecture.md`
- `.ai/context/data_model.md`
- `.ai/context/coding_standards.md`

## Recommended End-To-End Build Flow

1. Use `bronze_pipeline` for landing raw source data.
2. Use `silver_transform` for cleaning, casting, deduplication, and validation.
3. Use `scd2_merge` for historical dimensions such as `dim_product`.
4. Use `fact_table` for gold facts and dimension key resolution.
5. Ask explicitly for DLT expectations and quarantine tables when you want operational data quality handling.
6. Ask to sync changes to Databricks workspace and create or update the DLT pipeline if needed.

## Copy-Paste Prompt Examples

### Bronze

```text
Use the bronze_pipeline skill and build a Databricks Auto Loader notebook for retail orders CSV files into retailpulse.bronze.orders with schemaLocation, checkpointLocation, rescued data column, and availableNow trigger.
```

### Silver

```text
Use the silver_transform skill and build a silver transformation for retailpulse.bronze.orders into retailpulse.silver.orders with casting, null checks, quantity and price validation, and latest-record dedup by order_id.
```

### DLT Silver

```text
Use the silver_transform skill and create a DLT version of the silver orders pipeline with expectations, a curated silver_orders_dlt table, and a silver_orders_quarantine table with dq_reason.
```

### SCD2 Dimension

```text
Use the scd2_merge skill and build an SCD Type 2 dim_product pipeline from retailpulse.silver.products into retailpulse.gold.dim_product using product_id as business key and tracking category_id and price changes.
```

### Gold Fact

```text
Use the fact_table skill and build retailpulse.gold.fact_sales by joining silver orders to dim_product, dim_customer, and dim_date using current_mode joins and product surrogate keys.
```

### DLT Fact With Quarantine

```text
Use the fact_table skill and create a DLT fact_sales_dlt table plus fact_sales_quarantine so unresolved dimension joins are preserved instead of failing the whole pipeline.
```

### Full Project Build

```text
Using the project skills in .ai/skills, build the RetailPulse pipeline end to end: bronze ingestion, silver transform, SCD2 product dimension, customer and date dimensions, fact_sales, then add a DLT quality pipeline with curated and quarantine outputs. Sync all changes to my Databricks workspace.
```

### New Domain Using Same Pattern

```text
Use the RetailPulse project skills and context to create a new inventory pipeline end to end with bronze, silver, dimension tables, fact_inventory, and a DLT quality layer with quarantine and reprocessing-ready dq_reason columns.
```

## Practical Prompting Tips

- Name the skill explicitly in the prompt.
- Mention source table, target table, and layer.
- Say whether you want batch, streaming, or DLT.
- Say whether bad data should be dropped, failed, or quarantined.
- For facts, say whether joins should use current-mode or historical-mode SCD logic.
- If working in this repo, ask to sync to Databricks workspace when you want the workspace updated too.
