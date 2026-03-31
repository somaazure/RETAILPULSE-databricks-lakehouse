# Gold Layer Job Creation Prompt

Create a Databricks Workflow Job to generate Gold layer dimensional and fact tables from Silver layer DLT outputs.

## Job Configuration

* **Job Name**: `retailpulse_gold_dims_facts`
* **Job Type**: Multi-task workflow with Python notebooks
* **Cluster**: Shared/dedicated job cluster with appropriate sizing
* **Schedule**: Triggered after Bronze/Silver DLT pipeline completion (event-based or scheduled)

## Job Tasks Structure

**Task 1: Dimension Tables**
* Read from `retailpulse.silver.customers`, `retailpulse.silver.products`
* Generate slowly changing dimensions (keep current snapshot or history)
* Write to `retailpulse.gold.dim_customer`, `retailpulse.gold.dim_product`
* Use SCD Type 1 or Type 2 based on business requirements

**Task 2: Fact Tables**
* Read from `retailpulse.silver.orders`
* Join with dimension tables for denormalization
* Calculate derived metrics (extended price, margin, etc.)
* Write to `retailpulse.gold.fact_sales`

**Task 3: Aggregate Tables**
* Read from `retailpulse.gold.fact_sales`
* Create aggregations by day, week, month, customer, product
* Write to `retailpulse.gold.sales_summary_daily`, `retailpulse.gold.customer_metrics`

## Implementation Pattern

```python
# Standard pattern for Gold notebook tasks
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# Read from Silver
silver_data = spark.read.table("retailpulse.silver.orders")

# Transform/aggregate
gold_data = (
    silver_data
    .groupBy("customer_id", "order_date")
    .agg(
        F.sum("amount").alias("total_sales"),
        F.count("order_id").alias("order_count")
    )
)

# Write to Gold (overwrite or merge)
gold_data.write.mode("overwrite").saveAsTable("retailpulse.gold.customer_daily_sales")
```

## Task Dependencies

* Ensure tasks run in order: Dimensions → Facts → Aggregates
* Use `depends_on` parameter to sequence tasks
* Configure appropriate timeout and retry policies

## Triggering Strategy

**Option 1: Event-Based Trigger**
* Use DLT pipeline completion webhook
* Trigger Gold job via API when Bronze/Silver pipeline succeeds
* Requires orchestration layer (Enterprise Hybrid Workflow or Orchestrator job)

**Option 2: Scheduled Trigger**
* Schedule Gold job to run after expected DLT completion time
* Add buffer time for DLT runtime variance
* Use cron expression for daily/hourly runs

**Option 3: Manual Trigger**
* Run Gold job on-demand via UI or API
* Useful for ad-hoc processing or testing
