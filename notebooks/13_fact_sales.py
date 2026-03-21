"""Build the sales fact and Gold-layer quarantine table from curated dimensions."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

SILVER_ORDERS_TABLE = "retailpulse.silver.orders"
GOLD_DIM_PRODUCT_TABLE = "retailpulse.gold.dim_product"
GOLD_DIM_CUSTOMER_TABLE = "retailpulse.gold.dim_customer"
GOLD_DIM_DATE_TABLE = "retailpulse.gold.dim_date"
GOLD_FACT_SALES_TABLE = "retailpulse.gold.fact_sales"
OPS_FACT_SALES_QUARANTINE = "retailpulse.ops.fact_sales_quarantine"

spark.sql("CREATE CATALOG IF NOT EXISTS retailpulse")
spark.sql("CREATE SCHEMA IF NOT EXISTS retailpulse.gold")
spark.sql("CREATE SCHEMA IF NOT EXISTS retailpulse.ops")

orders_df = spark.table(SILVER_ORDERS_TABLE).alias("orders")
dim_product_df = (
    spark.table(GOLD_DIM_PRODUCT_TABLE)
    .filter(F.col("is_current") == F.lit(True))
    .select("product_sk", "product_id")
    .alias("dim_product")
)
dim_customer_df = spark.table(GOLD_DIM_CUSTOMER_TABLE).select("customer_id").alias("dim_customer")
dim_date_df = spark.table(GOLD_DIM_DATE_TABLE).select("date_id", "full_date").alias("dim_date")

joined_df = (
    orders_df.join(
        dim_product_df,
        F.col("orders.product_id") == F.col("dim_product.product_id"),
        "left",
    )
    .join(
        dim_customer_df,
        F.col("orders.customer_id") == F.col("dim_customer.customer_id"),
        "left",
    )
    .join(
        dim_date_df,
        F.to_date(F.col("orders.ingest_ts")) == F.col("dim_date.full_date"),
        "left",
    )
    .select(
        F.col("orders.order_id").alias("order_id"),
        F.col("dim_product.product_sk").alias("product_sk"),
        F.col("orders.customer_id").alias("customer_id"),
        F.col("dim_date.date_id").alias("date_id"),
        F.col("orders.product_id").alias("product_id"),
        F.col("orders.quantity").alias("quantity"),
        F.col("orders.price").alias("price"),
        (F.col("orders.quantity") * F.col("orders.price")).alias("sales_amount"),
        F.col("orders.ingest_ts").alias("order_ts"),
    )
    .dropDuplicates(["order_id"])
)

fact_sales_df = joined_df.filter(
    F.col("product_sk").isNotNull()
    & F.col("customer_id").isNotNull()
    & F.col("date_id").isNotNull()
).select(
    "order_id",
    "product_sk",
    "customer_id",
    "date_id",
    "quantity",
    "price",
    "sales_amount",
    "order_ts",
)

fact_quarantine_df = joined_df.filter(
    F.col("product_sk").isNull()
    | F.col("customer_id").isNull()
    | F.col("date_id").isNull()
).select(
    F.col("order_id"),
    F.col("customer_id"),
    F.col("product_id"),
    F.col("quantity"),
    F.col("price"),
    F.col("order_ts"),
    F.concat_ws(
        "; ",
        F.when(F.col("product_sk").isNull(), F.lit("missing_product_dimension")),
        F.when(F.col("customer_id").isNull(), F.lit("missing_customer_dimension")),
        F.when(F.col("date_id").isNull(), F.lit("missing_date_dimension")),
    ).alias("dq_reason"),
)

(
    fact_sales_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_FACT_SALES_TABLE)
)

(
    fact_quarantine_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(OPS_FACT_SALES_QUARANTINE)
)

print(f"Created fact table: {GOLD_FACT_SALES_TABLE}")
print(f"fact row count: {fact_sales_df.count()}")
print(f"Created fact quarantine table: {OPS_FACT_SALES_QUARANTINE}")
print(f"quarantine row count: {fact_quarantine_df.count()}")
