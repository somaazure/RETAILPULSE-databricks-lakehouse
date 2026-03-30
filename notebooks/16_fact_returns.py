# Databricks notebook source
# DBTITLE 1,Build Returns Fact Table
"""Build the returns fact table from return transactions and dimension tables."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime, timedelta
import random

spark = SparkSession.builder.getOrCreate()

# Source and target tables
GOLD_FACT_RETURNS_TABLE = "retailpulse.gold.fact_returns"
GOLD_DIM_CUSTOMER_TABLE = "retailpulse.gold.dim_customer"
GOLD_DIM_PRODUCT_TABLE = "retailpulse.gold.dim_product"
GOLD_DIM_STORE_TABLE = "retailpulse.gold.dim_store"
GOLD_FACT_SALES_TABLE = "retailpulse.gold.fact_sales"

spark.sql("CREATE CATALOG IF NOT EXISTS retailpulse")
spark.sql("CREATE SCHEMA IF NOT EXISTS retailpulse.gold")

# TODO: Replace with actual source system integration
# For now, generating return data that would typically come from:
# - Order management system
# - POS return transactions
# - Customer service system
# - E-commerce returns portal

# Generate realistic return transactions
return_reasons = [
    ('defect', 'Product arrived damaged'),
    ('defect', 'Product stopped working after 2 days'),
    ('wrong_item', 'Received wrong color'),
    ('wrong_item', 'Received wrong size'),
    ('changed_mind', 'Customer changed mind'),
    ('changed_mind', 'Found better price elsewhere'),
    ('not_as_described', 'Product did not match description'),
    ('quality', 'Poor quality materials')
]

returns_data = []
for i in range(1, 51):
    return_id = i
    order_id = random.randint(1000, 9999)
    customer_id = random.randint(1, 100)
    product_id = random.randint(1, 50)
    store_id = random.randint(1, 12)
    
    # Returns from last 90 days
    days_ago = random.randint(1, 90)
    return_date = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
    
    quantity_returned = random.randint(1, 3)
    return_amount = round(random.uniform(15.99, 299.99), 2)
    
    # 20% of returns have restocking fee
    restocking_fee = round(return_amount * 0.15, 2) if random.random() < 0.2 else 0.0
    refund_amount = return_amount - restocking_fee
    
    reason_code, reason_desc = random.choice(return_reasons)
    
    returns_data.append((
        return_id, order_id, customer_id, product_id, store_id,
        return_date, quantity_returned, return_amount, refund_amount,
        restocking_fee, reason_code, reason_desc
    ))

fact_returns_df = (
    spark.createDataFrame(
        returns_data,
        ['return_id', 'order_id', 'customer_id', 'product_id', 'store_id',
         'return_date', 'quantity_returned', 'return_amount', 'refund_amount',
         'restocking_fee', 'return_reason_code', 'return_reason_desc']
    )
    .withColumn('return_date', F.to_date(F.col('return_date')))
    .withColumn('created_date', F.current_timestamp())
)

# Note: In production, you would join with dimension tables to validate foreign keys
# For example:
# .join(spark.table(GOLD_DIM_CUSTOMER_TABLE), 'customer_id', 'inner')
# .join(spark.table(GOLD_DIM_PRODUCT_TABLE), 'product_id', 'inner')
# .join(spark.table(GOLD_DIM_STORE_TABLE), 'store_id', 'inner')

(
    fact_returns_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_FACT_RETURNS_TABLE)
)

print(f"Created returns fact table: {GOLD_FACT_RETURNS_TABLE}")
print(f"Row count: {fact_returns_df.count()}")
print(f"Date range: {fact_returns_df.agg(F.min('return_date'), F.max('return_date')).first()}")
print(f"Total return amount: ${fact_returns_df.agg(F.sum('return_amount')).first()[0]:,.2f}")
print(f"Total refund amount: ${fact_returns_df.agg(F.sum('refund_amount')).first()[0]:,.2f}")

# COMMAND ----------

# DBTITLE 1,Verify Returns Fact Table
# Verify the data load and analyze return patterns
result = spark.sql(f"""
    SELECT 
        return_reason_code,
        COUNT(*) as return_count,
        SUM(quantity_returned) as total_qty_returned,
        SUM(return_amount) as total_return_amount,
        SUM(refund_amount) as total_refund_amount,
        SUM(restocking_fee) as total_restocking_fees,
        AVG(return_amount) as avg_return_amount
    FROM {GOLD_FACT_RETURNS_TABLE}
    GROUP BY return_reason_code
    ORDER BY return_count DESC
""")

result.show(truncate=False)
print(f"\nTotal returns loaded: {spark.table(GOLD_FACT_RETURNS_TABLE).count()}")

# COMMAND ----------

# DBTITLE 1,Return Rate Analysis
# Analyze return rates by store (requires joining with dim_store)
result = spark.sql(f"""
    SELECT 
        s.store_name,
        s.store_type,
        s.region,
        COUNT(r.return_id) as return_count,
        SUM(r.return_amount) as total_returns,
        AVG(r.return_amount) as avg_return_value
    FROM {GOLD_FACT_RETURNS_TABLE} r
    INNER JOIN {GOLD_DIM_STORE_TABLE} s ON r.store_id = s.store_id
    GROUP BY s.store_name, s.store_type, s.region
    ORDER BY return_count DESC
""")

result.show(truncate=False)
print("\nReturn analysis by store completed.")