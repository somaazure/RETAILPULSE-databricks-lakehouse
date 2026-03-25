# Databricks notebook source
"""Build a deterministic product master in Silver for enterprise Gold jobs."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

SILVER_PRODUCTS_TABLE = "retailpulse.silver.products"

spark.sql("CREATE CATALOG IF NOT EXISTS retailpulse")
spark.sql("CREATE SCHEMA IF NOT EXISTS retailpulse.silver")

products_df = (
    spark.range(100, 1000)
    .select(
        F.col("id").cast("int").alias("product_id"),
        F.concat(F.lit("Product-"), F.col("id")).alias("product_name"),
        ((F.col("id") % 10) + F.lit(10)).cast("int").alias("category_id"),
        (F.round((F.col("id") * F.lit(1.75)) % F.lit(495) + F.lit(5), 2)).cast("decimal(12,2)").alias("price"),
    )
)

(
    products_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_PRODUCTS_TABLE)
)

print(f"Created product master: {SILVER_PRODUCTS_TABLE}")
print(f"product count: {products_df.count()}")
