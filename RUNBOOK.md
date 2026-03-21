# RetailPulse Runbook

This runbook documents the active enterprise operating model on this branch.

## Workspace Path

`/Workspace/Users/shekartelstra@gmail.com/RetailPulse`

## Enterprise Bronze/Silver DLT

Config file:

`config/dlt_bronze_silver_pipeline.json`

Notebook:

`/Workspace/Users/shekartelstra@gmail.com/RetailPulse/notebooks/08_dlt_e2e_main_refresh`

Purpose:

- DLT owns Bronze ingestion
- DLT owns Silver curation and Silver quarantine
- this is the only active DLT layer in the enterprise model

Outputs:

- `retailpulse.bronze.orders`
- `retailpulse.silver.orders`
- `retailpulse.ops.silver_orders_quarantine`

## Gold Dims And Facts Job

Config file:

`config/job_gold_dims_facts.json`

Notebook assets:

- `/Workspace/Users/shekartelstra@gmail.com/RetailPulse/notebooks/09_product_master`
- `/Workspace/Users/shekartelstra@gmail.com/RetailPulse/notebooks/10_dim_product`
- `/Workspace/Users/shekartelstra@gmail.com/RetailPulse/notebooks/11_dim_customer`
- `/Workspace/Users/shekartelstra@gmail.com/RetailPulse/notebooks/12_dim_date`
- `/Workspace/Users/shekartelstra@gmail.com/RetailPulse/notebooks/13_fact_sales`

Purpose:

- keep Gold logic modular and independently rerunnable
- separate product mastering, SCD2 maintenance, dimensions, and fact loads
- keep fact quarantine in the Gold job layer

Gold outputs:

- `retailpulse.silver.products`
- `retailpulse.gold.dim_product`
- `retailpulse.gold.dim_customer`
- `retailpulse.gold.dim_date`
- `retailpulse.gold.fact_sales`
- `retailpulse.ops.fact_sales_quarantine`

## Enterprise Orchestrator

Config file:

`config/job_enterprise_orchestrator.json`

Purpose:

- provide one controller job for manual or scheduled execution
- run the Bronze/Silver DLT task first
- run the Gold batch job only after DLT succeeds

Execution order:

1. DLT pipeline task
2. Gold job task with dependency on DLT success

## One-Time Ownership Cutover

If the workspace currently has DLT-managed materialized views from the older one-click branch, do not point the enterprise Gold notebooks at those same table names until DLT ownership is retired.

Tables to review before first enterprise run:

- `retailpulse.silver.products`
- `retailpulse.gold.dim_product`
- `retailpulse.gold.dim_customer`
- `retailpulse.gold.dim_date`
- `retailpulse.gold.fact_sales`
- `retailpulse.ops.fact_sales_quarantine`

Recommended sequence:

1. back up current tables if needed
2. stop the older full-DLT refresh flow
3. drop the DLT-managed Gold objects
4. run the enterprise Gold notebooks so they recreate standard Delta tables

## Archive

Older notebook variants are preserved under:

`/Workspace/Users/shekartelstra@gmail.com/RetailPulse/notebooks/archive`

Legacy config and job definitions are preserved locally under:

`config/archive`

## Sync Command

```powershell
powershell -ExecutionPolicy Bypass -File .\sync_to_workspace.ps1
```
