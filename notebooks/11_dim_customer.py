"""Build the customer dimension from curated Silver orders."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

SILVER_ORDERS_TABLE = "retailpulse.silver.orders"
GOLD_DIM_CUSTOMER_TABLE = "retailpulse.gold.dim_customer"

spark.sql("CREATE CATALOG IF NOT EXISTS retailpulse")
spark.sql("CREATE SCHEMA IF NOT EXISTS retailpulse.gold")

dim_customer_df = (
    spark.table(SILVER_ORDERS_TABLE)
    .filter(F.col("customer_id").isNotNull())
    .select(F.col("customer_id").cast("int").alias("customer_id"))
    .dropDuplicates(["customer_id"])
    .withColumn("customer_name", F.concat(F.lit("Customer-"), F.col("customer_id")))
    .withColumn(
        "customer_segment",
        F.when((F.col("customer_id") % 3) == 0, F.lit("Premium"))
        .when((F.col("customer_id") % 3) == 1, F.lit("Standard"))
        .otherwise(F.lit("Basic")),
    )
    .withColumn("customer_status", F.lit("Active"))
)

(
    dim_customer_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_DIM_CUSTOMER_TABLE)
)

print(f"Created customer dimension table: {GOLD_DIM_CUSTOMER_TABLE}")
print(f"row count: {dim_customer_df.count()}")
