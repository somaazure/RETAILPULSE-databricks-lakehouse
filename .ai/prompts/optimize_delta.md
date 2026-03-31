# Delta Table Optimization Prompt

Create maintenance jobs or notebooks for optimizing Delta tables in RetailPulse catalog.

## Optimization Operations

**OPTIMIZE (Compaction)**
* Purpose: Compact small files into larger files for better query performance
* Frequency: Daily or after large batch writes
* Target: Tables with frequent writes or high file count

**VACUUM**
* Purpose: Remove old file versions and reduce storage costs
* Frequency: Weekly or monthly
* Retention: Minimum 7 days (default), adjust based on time travel needs
* **WARNING**: Cannot undo - ensure retention period is appropriate

**Z-ORDER**
* Purpose: Co-locate related data for faster queries
* Frequency: Weekly or after significant data changes
* Columns: High-cardinality filter columns (customer_id, product_id, date)

## Implementation Pattern

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# OPTIMIZE with Z-ORDER
spark.sql("""
    OPTIMIZE retailpulse.gold.fact_sales
    ZORDER BY (customer_id, order_date)
""")

# VACUUM (remove files older than retention period)
spark.sql("""
    VACUUM retailpulse.gold.fact_sales RETAIN 168 HOURS
""")

# ANALYZE TABLE (update statistics)
spark.sql("""
    ANALYZE TABLE retailpulse.gold.fact_sales COMPUTE STATISTICS FOR ALL COLUMNS
""")
```

## Recommended Maintenance Schedule

**Bronze Tables**:
* OPTIMIZE: After each DLT pipeline run (if not using Auto Optimize)
* VACUUM: Weekly with 7-day retention
* Z-ORDER: Not typically needed (append-only, sequential access)

**Silver Tables**:
* OPTIMIZE: Daily
* VACUUM: Weekly with 7-day retention
* Z-ORDER: On frequently filtered columns (customer_id, product_id, date)

**Gold Tables**:
* OPTIMIZE: After each write operation
* VACUUM: Monthly with 30-day retention (more time travel history)
* Z-ORDER: On primary query filter columns

## Auto Optimize (Alternative)

Enable Auto Optimize at table level to automatically compact files during writes:

```python
# Set table properties
spark.sql("""
    ALTER TABLE retailpulse.silver.orders
    SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
""")
```

## Job Configuration for Maintenance

* **Job Name**: `retailpulse_delta_maintenance`
* **Schedule**: Nightly (off-peak hours)
* **Cluster**: Serverless or small dedicated cluster
* **Timeout**: 2-4 hours depending on data volume
* **Tasks**: Separate tasks for Bronze, Silver, Gold layers

## Monitoring

* Check file count before/after optimization: `DESCRIBE DETAIL <table>`
* Monitor query performance improvements
* Track storage reduction after VACUUM
* Set alerts for optimization failures
