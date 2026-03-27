# Databricks notebook source
"""Silver transformation pipeline for RetailPulse orders."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, row_number, trim
from pyspark.sql.types import DecimalType, IntegerType, StringType, TimestampType
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

SOURCE_TABLE = "retailpulse.bronze.orders"
TARGET_TABLE = "retailpulse.silver.orders"


def ensure_target_schema() -> None:
    """Create the target schema if it does not exist."""
    spark.sql("CREATE CATALOG IF NOT EXISTS retailpulse")
    spark.sql("CREATE SCHEMA IF NOT EXISTS retailpulse.silver")


def read_bronze_orders() -> DataFrame:
    """Load the bronze orders table."""
    return spark.table(SOURCE_TABLE)


def standardize_orders(df: DataFrame) -> DataFrame:
    """Trim string fields and cast columns into the target silver schema."""
    return df.select(
        trim(col("order_id")).cast(StringType()).alias("order_id"),
        col("customer_id").cast(IntegerType()).alias("customer_id"),
        col("product_id").cast(IntegerType()).alias("product_id"),
        col("quantity").cast(IntegerType()).alias("quantity"),
        col("price").cast(DecimalType(12, 2)).alias("price"),
        col("source_file_name").cast(StringType()).alias("source_file_name"),
        col("ingest_ts").cast(TimestampType()).alias("ingest_ts"),
        col("_rescued_data").cast(StringType()).alias("_rescued_data"),
    )


def filter_valid_orders(df: DataFrame) -> DataFrame:
    """Keep only business-valid rows and exclude rescued malformed records."""
    return df.filter(
        col("order_id").isNotNull()
        & (col("order_id") != "")
        & col("customer_id").isNotNull()
        & col("product_id").isNotNull()
        & col("quantity").isNotNull()
        & col("price").isNotNull()
        & (col("quantity") > 0)
        & (col("price") > 0)
        & col("_rescued_data").isNull()
    )


def deduplicate_orders(df: DataFrame) -> DataFrame:
    """Keep the latest record per order_id based on ingest timestamp."""
    window_spec = Window.partitionBy("order_id").orderBy(
        col("ingest_ts").desc(), col("source_file_name").desc()
    )

    return (
        df.withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num", "_rescued_data")
    )


def write_silver_orders(df: DataFrame) -> None:
    """Persist the curated silver orders table as Delta."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )


def main() -> None:
    """Run the end-to-end bronze to silver transformation."""
    ensure_target_schema()

    bronze_df = read_bronze_orders()
    standardized_df = standardize_orders(bronze_df)
    valid_df = filter_valid_orders(standardized_df)
    silver_df = deduplicate_orders(valid_df)

    write_silver_orders(silver_df)

    print(f"Silver transformation completed: {SOURCE_TABLE} -> {TARGET_TABLE}")
    print(f"Output row count: {silver_df.count()}")


if __name__ == "__main__":
    main()
