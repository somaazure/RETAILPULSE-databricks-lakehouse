# Databricks notebook source
# DBTITLE 1,Build Store Dimension
"""Build the store/warehouse dimension for physical and virtual sales channels."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

GOLD_DIM_STORE_TABLE = "retailpulse.gold.dim_store"

spark.sql("CREATE CATALOG IF NOT EXISTS retailpulse")
spark.sql("CREATE SCHEMA IF NOT EXISTS retailpulse.gold")

# TODO: Replace with actual source system integration
# For now, creating store master data that would typically come from:
# - Store master database
# - Warehouse management system
# - E-commerce platform configuration

stores_data = [
    (1, 'Downtown Flagship Store', 'retail', 'Northeast', 'Manhattan', 'Sarah Johnson', '2018-03-15', 25000),
    (2, 'Mall of America Branch', 'retail', 'Midwest', 'Minneapolis', 'Mike Chen', '2019-06-01', 15000),
    (3, 'Online Store - US', 'online', 'National', 'Digital', 'Emily Rodriguez', '2017-01-10', None),
    (4, 'Outlet Center Phoenix', 'outlet', 'Southwest', 'Phoenix Metro', 'James Wilson', '2020-08-20', 12000),
    (5, 'Central Warehouse - East', 'warehouse', 'Northeast', 'New Jersey', 'Robert Taylor', '2016-11-05', 50000),
    (6, 'Central Warehouse - West', 'warehouse', 'West', 'California', 'Lisa Anderson', '2017-02-15', 60000),
    (7, 'Seattle Premium Store', 'retail', 'West', 'Seattle Metro', 'David Kim', '2019-04-12', 18000),
    (8, 'Chicago Loop Store', 'retail', 'Midwest', 'Chicago', 'Maria Garcia', '2018-09-01', 20000),
    (9, 'Miami Beach Outlet', 'outlet', 'Southeast', 'Miami-Dade', 'Carlos Martinez', '2021-01-15', 10000),
    (10, 'Fulfillment Center - South', 'warehouse', 'Southeast', 'Georgia', 'Jennifer Brown', '2019-12-01', 75000),
    (11, 'Mobile App Store', 'online', 'National', 'Digital', 'Alex Thompson', '2020-03-01', None),
    (12, 'Boston Financial District', 'retail', 'Northeast', 'Boston', 'Patricia Lee', '2017-07-20', 14000)
]

dim_store_df = (
    spark.createDataFrame(
        stores_data,
        ['store_id', 'store_name', 'store_type', 'region', 'district', 'manager', 'open_date', 'square_footage']
    )
    .withColumn('open_date', F.to_date(F.col('open_date')))
    .withColumn('square_footage', F.col('square_footage').cast(IntegerType()))
    .withColumn('is_active', F.lit(True))
    .withColumn('created_date', F.current_timestamp())
)

(
    dim_store_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_DIM_STORE_TABLE)
)

print(f"Created store dimension table: {GOLD_DIM_STORE_TABLE}")
print(f"Row count: {dim_store_df.count()}")
print(f"Store types: {dim_store_df.select('store_type').distinct().count()}")
print(f"Regions: {dim_store_df.select('region').distinct().count()}")

# COMMAND ----------

# DBTITLE 1,Verify Store Dimension
# Verify the data load
result = spark.sql(f"""
    SELECT 
        store_type,
        region,
        COUNT(*) as store_count,
        AVG(square_footage) as avg_sqft
    FROM {GOLD_DIM_STORE_TABLE}
    GROUP BY store_type, region
    ORDER BY store_type, region
""")

result.show(truncate=False)
print(f"\nTotal stores loaded: {spark.table(GOLD_DIM_STORE_TABLE).count()}")