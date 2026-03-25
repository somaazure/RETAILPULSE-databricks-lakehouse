# Databricks notebook source
"""Build the date dimension from curated Silver orders."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

SILVER_ORDERS_TABLE = "retailpulse.silver.orders"
GOLD_DIM_DATE_TABLE = "retailpulse.gold.dim_date"

spark.sql("CREATE CATALOG IF NOT EXISTS retailpulse")
spark.sql("CREATE SCHEMA IF NOT EXISTS retailpulse.gold")

dim_date_df = (
    spark.table(SILVER_ORDERS_TABLE)
    .filter(F.col("order_timestamp").isNotNull())
    .select(F.to_date(F.col("order_timestamp")).alias("full_date"))
    .dropDuplicates(["full_date"])
    .select(
        F.date_format(F.col("full_date"), "yyyyMMdd").cast("int").alias("date_id"),
        F.col("full_date"),
        F.dayofmonth(F.col("full_date")).alias("day"),
        F.month(F.col("full_date")).alias("month"),
        F.year(F.col("full_date")).alias("year"),
        F.weekofyear(F.col("full_date")).alias("week_of_year"),
    )
    .orderBy("full_date")
)

(
    dim_date_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_DIM_DATE_TABLE)
)

print(f"Created date dimension table: {GOLD_DIM_DATE_TABLE}")
print(f"row count: {dim_date_df.count()}")
