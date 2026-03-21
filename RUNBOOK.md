# RetailPulse Runbook

This runbook documents the Databricks operational assets created for RetailPulse and when to use each one.

## Workspace Path

Project workspace folder:

`/Workspace/Users/shekartelstra@gmail.com/RetailPulse`

## Primary DLT Pipeline

Name: `RetailPulse DLT Quality Pipeline`

Pipeline ID: `0099e0a1-8d57-42d5-9ea6-36984f54c13f`

Notebook:

`/Workspace/Users/shekartelstra@gmail.com/RetailPulse/notebooks/archive/06_dlt_pipeline`

Purpose:

- current working DLT pipeline for quality validation
- writes curated and quarantine outputs into `retailpulse.dlt`
- safe for experimentation because it does not replace the main curated tables

Use when:

- you want DLT-based DQ checks
- you want quarantine outputs without changing the existing main silver and gold table owners
- you want to inspect quality metrics and rejected records safely

Key outputs:

- `retailpulse.dlt.silver_orders_dlt`
- `retailpulse.dlt.silver_orders_quarantine`
- `retailpulse.dlt.fact_sales_dlt`
- `retailpulse.dlt.fact_sales_quarantine`

## Future Prod Cutover DLT Pipeline

Name: `RetailPulse Main Curated DLT Future Cutover`

Pipeline ID: `20d2e45a-5d08-4066-a3e9-4f268f5017da`

Notebook:

`/Workspace/Users/shekartelstra@gmail.com/RetailPulse/notebooks/archive/07_dlt_main_tables`

Purpose:

- future production cutover template
- designed for DLT to own main curated tables directly
- writes curated outputs to the main schemas and quarantine outputs to operational schemas

Target outputs:

- `retailpulse.silver.orders`
- `retailpulse.gold.fact_sales`
- `retailpulse.ops.silver_orders_quarantine`
- `retailpulse.ops.fact_sales_quarantine`

Use when:

- you are ready to retire notebook-based writers for `retailpulse.silver.orders` and `retailpulse.gold.fact_sales`
- you want DLT to become the single writer for main curated production tables

Important:

- do not run this cutover pipeline while the notebook-based flow is still writing the same main curated tables
- one production table should have only one active writer

## End-To-End Job

Name: `RetailPulse End-to-End Workflow`

Job ID: `454806619496303`

Purpose:

- runs the notebook flow end to end and then triggers the current DLT quality pipeline
- manual execution only for now; no schedule configured

Task order:

1. `01_data_generator`
2. `02_bronze_ingestion`
3. `03_silver_transform`
4. `04_dim_tables`
5. `05_fact_tables`
6. `06_dlt_pipeline` through the DLT pipeline task

Run manually from CLI:

```powershell
databricks jobs run-now 454806619496303 --profile shekartelstra
```

## Config Files

Local config files in this repo:

- `config/dlt_pipeline_config.json`
- `config/dlt_main_tables_pipeline_config.json`
- `config/job_retailpulse_e2e.json`

## Recommended Operating Model

Current recommended model:

- use the notebook flow plus the current DLT quality pipeline
- keep `retailpulse.dlt` as the DLT validation and quarantine schema
- use quarantine tables for troubleshooting and replay planning

Future production model:

- cut over to the main curated DLT pipeline only after retiring the notebook-based writers for the same tables
- keep quarantine tables in `retailpulse.ops`
- use DLT as the single owner of main curated tables

## Sync Command

Sync latest repo changes to the Databricks workspace:

```powershell
powershell -ExecutionPolicy Bypass -File .\sync_to_workspace.ps1
```


Superseded older template pipeline ID: ec8f9260-6537-4d58-81f1-04e5cc4d1723 (do not use).


