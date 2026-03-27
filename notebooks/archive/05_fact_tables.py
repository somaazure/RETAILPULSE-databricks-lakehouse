# Databricks notebook source
"""Build the RetailPulse sales fact table."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

SILVER_ORDERS_TABLE = "retailpulse.silver.orders"
GOLD_DIM_PRODUCT_TABLE = "retailpulse.gold.dim_product"
GOLD_DIM_CUSTOMER_TABLE = "retailpulse.gold.dim_customer"
GOLD_DIM_DATE_TABLE = "retailpulse.gold.dim_date"
GOLD_FACT_SALES_TABLE = "retailpulse.gold.fact_sales"



def ensure_target_schema() -> None:
    """Create the target schema if it does not exist."""
    spark.sql("CREATE CATALOG IF NOT EXISTS retailpulse")
    spark.sql("CREATE SCHEMA IF NOT EXISTS retailpulse.gold")



def read_orders() -> DataFrame:
    """Load the curated silver orders table."""
    return spark.table(SILVER_ORDERS_TABLE).alias("orders")



def read_current_dim_product() -> DataFrame:
    """Load the current product dimension rows for product key resolution."""
    return (
        spark.table(GOLD_DIM_PRODUCT_TABLE)
        .filter(F.col("is_current") == F.lit(True))
        .select("product_sk", "product_id")
        .alias("dim_product")
    )



def read_dim_customer() -> DataFrame:
    """Load the customer dimension table."""
    return spark.table(GOLD_DIM_CUSTOMER_TABLE).alias("dim_customer")



def read_dim_date() -> DataFrame:
    """Load the date dimension table."""
    return spark.table(GOLD_DIM_DATE_TABLE).alias("dim_date")



def build_fact_sales_dataframe() -> DataFrame:
    """Join silver orders with product, customer, and date dimensions."""
    orders_df = read_orders()
    dim_product_df = read_current_dim_product()
    dim_customer_df = read_dim_customer()
    dim_date_df = read_dim_date()

    return (
        orders_df.join(
            dim_product_df,
            F.col("orders.product_id") == F.col("dim_product.product_id"),
            "inner",
        )
        .join(
            dim_customer_df,
            F.col("orders.customer_id") == F.col("dim_customer.customer_id"),
            "inner",
        )
        .join(
            dim_date_df,
            F.to_date(F.col("orders.ingest_ts")) == F.col("dim_date.full_date"),
            "inner",
        )
        .select(
            F.col("orders.order_id").alias("order_id"),
            F.col("dim_product.product_sk").alias("product_sk"),
            F.col("dim_customer.customer_id").alias("customer_id"),
            F.col("dim_date.date_id").alias("date_id"),
            F.col("orders.quantity").alias("quantity"),
            F.col("orders.price").alias("price"),
            (F.col("orders.quantity") * F.col("orders.price")).alias("sales_amount"),
            F.col("orders.ingest_ts").alias("order_ts"),
        )
        .dropDuplicates(["order_id"])
    )



def write_fact_sales_table(fact_sales_df: DataFrame) -> None:
    """Persist the sales fact table as Delta."""
    (
        fact_sales_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(GOLD_FACT_SALES_TABLE)
    )



def main() -> None:
    """Build and persist the gold sales fact table."""
    ensure_target_schema()

    fact_sales_df = build_fact_sales_dataframe()
    write_fact_sales_table(fact_sales_df)

    print(f"Created fact table: {GOLD_FACT_SALES_TABLE}")
    print(f"fact_sales row count: {fact_sales_df.count()}")


if __name__ == "__main__":
    main()
