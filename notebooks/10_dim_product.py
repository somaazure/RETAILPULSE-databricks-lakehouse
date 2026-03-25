# Databricks notebook source
"""Maintain the SCD2 product dimension from the Silver product master."""

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

SILVER_PRODUCTS_TABLE = "retailpulse.silver.products"
GOLD_DIM_PRODUCT_TABLE = "retailpulse.gold.dim_product"

spark.sql("CREATE CATALOG IF NOT EXISTS retailpulse")
spark.sql("CREATE SCHEMA IF NOT EXISTS retailpulse.gold")

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {GOLD_DIM_PRODUCT_TABLE} (
      product_sk BIGINT GENERATED ALWAYS AS IDENTITY,
      product_id INT,
      product_name STRING,
      category_id INT,
      current_price DECIMAL(12,2),
      start_date TIMESTAMP,
      end_date TIMESTAMP,
      is_current BOOLEAN
    )
    USING DELTA
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW stg_dim_product AS
    SELECT
      CAST(product_id AS INT) AS product_id,
      CAST(product_name AS STRING) AS product_name,
      CAST(category_id AS INT) AS category_id,
      CAST(price AS DECIMAL(12,2)) AS current_price,
      current_timestamp() AS start_date
    FROM {SILVER_PRODUCTS_TABLE}
    WHERE product_id IS NOT NULL
    """
)

spark.sql(
    f"""
    MERGE INTO {GOLD_DIM_PRODUCT_TABLE} AS target
    USING stg_dim_product AS source
    ON target.product_id = source.product_id
    AND target.is_current = true
    WHEN MATCHED AND (
      target.category_id <> source.category_id
      OR target.current_price <> source.current_price
    ) THEN
      UPDATE SET
        target.end_date = source.start_date,
        target.is_current = false
    """
)

spark.sql(
    f"""
    INSERT INTO {GOLD_DIM_PRODUCT_TABLE} (
      product_id,
      product_name,
      category_id,
      current_price,
      start_date,
      end_date,
      is_current
    )
    SELECT
      source.product_id,
      source.product_name,
      source.category_id,
      source.current_price,
      source.start_date,
      TIMESTAMP('9999-12-31 00:00:00'),
      true
    FROM stg_dim_product AS source
    LEFT JOIN {GOLD_DIM_PRODUCT_TABLE} AS target
      ON target.product_id = source.product_id
      AND target.is_current = true
    WHERE target.product_id IS NULL
       OR target.category_id <> source.category_id
       OR target.current_price <> source.current_price
    """
)

count_df = spark.sql(f"SELECT COUNT(*) AS row_count FROM {GOLD_DIM_PRODUCT_TABLE}")
print(f"Updated SCD2 dimension table: {GOLD_DIM_PRODUCT_TABLE}")
print(f"row count: {count_df.collect()[0]['row_count']}")
