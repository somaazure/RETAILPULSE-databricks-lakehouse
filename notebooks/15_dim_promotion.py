# Databricks notebook source
# DBTITLE 1,Build Promotion Dimension
"""Build the promotion dimension for marketing campaigns and promotional offers."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

GOLD_DIM_PROMOTION_TABLE = "retailpulse.gold.dim_promotion"

spark.sql("CREATE CATALOG IF NOT EXISTS retailpulse")
spark.sql("CREATE SCHEMA IF NOT EXISTS retailpulse.gold")

# TODO: Replace with actual source system integration
# For now, creating promotion data that would typically come from:
# - Marketing campaign management system
# - Promotional offer database
# - CRM system

promotions_data = [
    (1, 'Spring Sale 2025', 'percent_off', 20.00, '2025-03-01', '2025-03-31', 'SPRING20', 'all', 50.00),
    (2, 'Black Friday Blowout', 'percent_off', 40.00, '2024-11-25', '2024-11-29', 'BF2024', 'all', None),
    (3, 'Buy One Get One Free', 'BOGO', None, '2025-01-15', '2025-02-15', 'BOGO2025', 'retail', None),
    (4, 'Summer Clearance', 'percent_off', 30.00, '2025-06-01', '2025-08-31', 'SUMMER30', 'all', None),
    (5, 'New Customer Welcome', 'percent_off', 15.00, '2024-01-01', '2025-12-31', 'WELCOME15', 'online', 25.00),
    (6, 'Bundle Deal Electronics', 'bundle', 25.00, '2025-02-01', '2025-02-28', 'TECHBUNDLE', 'all', 200.00),
    (7, 'Loyalty Rewards Week', 'percent_off', 10.00, '2025-04-01', '2025-04-07', 'LOYAL10', 'all', None),
    (8, 'Flash Sale Wednesday', 'percent_off', 35.00, '2026-03-25', '2026-03-25', 'FLASH35', 'online', 100.00),
    (9, 'Holiday Gift Bundle', 'bundle', 20.00, '2024-12-01', '2024-12-24', 'HOLIDAY20', 'all', 75.00),
    (10, 'Back to School', 'percent_off', 25.00, '2025-08-01', '2025-09-15', 'BTS2025', 'all', 30.00),
    (11, 'VIP Early Access', 'percent_off', 15.00, '2025-05-01', '2025-05-31', 'VIPEARLY', 'online', 150.00),
    (12, 'Clearance Final Days', 'percent_off', 50.00, '2025-07-25', '2025-07-31', 'FINAL50', 'outlet', None)
]

dim_promotion_df = (
    spark.createDataFrame(
        promotions_data,
        ['promotion_id', 'promotion_name', 'promotion_type', 'discount_pct', 'start_date', 'end_date', 
         'promo_code', 'target_channel', 'min_purchase_amount']
    )
    .withColumn('start_date', F.to_date(F.col('start_date')))
    .withColumn('end_date', F.to_date(F.col('end_date')))
    .withColumn('is_active', F.lit(True))
    .withColumn('created_date', F.current_timestamp())
)

(
    dim_promotion_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_DIM_PROMOTION_TABLE)
)

print(f"Created promotion dimension table: {GOLD_DIM_PROMOTION_TABLE}")
print(f"Row count: {dim_promotion_df.count()}")
print(f"Promotion types: {dim_promotion_df.select('promotion_type').distinct().count()}")
print(f"Target channels: {dim_promotion_df.select('target_channel').distinct().count()}")

# COMMAND ----------

# DBTITLE 1,Verify Promotion Dimension
# Verify the data load and show active promotions
result = spark.sql(f"""
    SELECT 
        promotion_type,
        target_channel,
        COUNT(*) as promo_count,
        AVG(discount_pct) as avg_discount,
        MIN(start_date) as earliest_start,
        MAX(end_date) as latest_end
    FROM {GOLD_DIM_PROMOTION_TABLE}
    GROUP BY promotion_type, target_channel
    ORDER BY promotion_type, target_channel
""")

result.show(truncate=False)
print(f"\nTotal promotions loaded: {spark.table(GOLD_DIM_PROMOTION_TABLE).count()}")