# RetailPulse Runbook

This runbook documents the enterprise-hybrid operating model implemented on this branch.

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
- this is the only DLT layer in the enterprise model

Outputs:

- `retailpulse.bronze.orders`
- `retailpulse.silver.orders`
- `retailpulse.ops.silver_orders_quarantine`

## Modular Gold Jobs

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

## Enterprise Workflow Template

Config file:

`config/job_enterprise_hybrid_workflow.json`

Task order:

1. `00_generate_orders_once`
2. Bronze/Silver DLT pipeline task
3. `09_product_master`
4. `10_dim_product`
5. `11_dim_customer`
6. `12_dim_date`
7. `13_fact_sales`

Important:

- update the placeholder pipeline id in the job config before creating the job
- this template is intentionally modular so dim jobs can be rerun independently when needed

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
4. run the enterprise-hybrid Gold notebooks so they recreate standard Delta tables

## Archive

Older notebook variants are preserved under:

`/Workspace/Users/shekartelstra@gmail.com/RetailPulse/notebooks/archive`

These remain useful for portfolio demos, learning comparisons, and migration reference.

## Sync Command

```powershell
powershell -ExecutionPolicy Bypass -File .\sync_to_workspace.ps1
```
