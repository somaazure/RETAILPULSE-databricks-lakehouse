"""Production-cutover DLT pipeline template for RetailPulse main curated tables.

This pipeline is a safe cutover template for Option 1, where DLT owns the main curated
Silver and Gold tables directly. Keep the current notebook-driven writers disabled before
running this pipeline in production to avoid multiple writers to the same tables.
"""

import dlt
from pyspark.sql import functions as F

BRONZE_ORDERS_TABLE = "retailpulse.bronze.orders"
GOLD_DIM_PRODUCT_TABLE = "retailpulse.gold.dim_product"
GOLD_DIM_CUSTOMER_TABLE = "retailpulse.gold.dim_customer"
GOLD_DIM_DATE_TABLE = "retailpulse.gold.dim_date"
MAIN_SILVER_ORDERS_TABLE = "retailpulse.silver.orders"
MAIN_GOLD_FACT_SALES_TABLE = "retailpulse.gold.fact_sales"
OPS_SILVER_ORDERS_QUARANTINE = "retailpulse.ops.silver_orders_quarantine"
OPS_FACT_SALES_QUARANTINE = "retailpulse.ops.fact_sales_quarantine"

spark.sql("CREATE CATALOG IF NOT EXISTS retailpulse")
spark.sql("CREATE SCHEMA IF NOT EXISTS retailpulse.ops")


@dlt.view(name="silver_orders_standardized_internal")
def silver_orders_standardized_internal():
    bronze_df = spark.read.table(BRONZE_ORDERS_TABLE)

    return bronze_df.select(
        F.trim(F.col("order_id")).cast("string").alias("order_id"),
        F.col("customer_id").cast("int").alias("customer_id"),
        F.col("product_id").cast("int").alias("product_id"),
        F.col("quantity").cast("int").alias("quantity"),
        F.col("price").cast("decimal(12,2)").alias("price"),
        F.col("source_file_name").cast("string").alias("source_file_name"),
        F.col("ingest_ts").alias("ingest_ts"),
        F.col("_rescued_data").cast("string").alias("_rescued_data"),
    )


@dlt.view(name="silver_orders_curated_internal")
def silver_orders_curated_internal():
    standardized_df = dlt.read("silver_orders_standardized_internal")

    valid_orders_df = standardized_df.filter(
        F.col("order_id").isNotNull()
        & (F.col("order_id") != "")
        & F.col("customer_id").isNotNull()
        & F.col("product_id").isNotNull()
        & F.col("quantity").isNotNull()
        & (F.col("quantity") > 0)
        & F.col("price").isNotNull()
        & (F.col("price") > 0)
        & F.col("ingest_ts").isNotNull()
        & (F.col("ingest_ts") <= F.current_timestamp())
        & F.col("_rescued_data").isNull()
    )

    latest_per_order = (
        valid_orders_df.groupBy("order_id")
        .agg(F.max("ingest_ts").alias("max_ingest_ts"))
        .alias("latest")
    )

    return (
        valid_orders_df.alias("orders")
        .join(
            latest_per_order,
            (F.col("orders.order_id") == F.col("latest.order_id"))
            & (F.col("orders.ingest_ts") == F.col("latest.max_ingest_ts")),
            "inner",
        )
        .select(
            F.col("orders.order_id"),
            F.col("orders.customer_id"),
            F.col("orders.product_id"),
            F.col("orders.quantity"),
            F.col("orders.price"),
            F.col("orders.source_file_name"),
            F.col("orders.ingest_ts"),
        )
        .dropDuplicates(["order_id"])
    )


@dlt.table(
    name=MAIN_SILVER_ORDERS_TABLE,
    comment="Production-cutover curated silver orders table managed directly by DLT.",
)
@dlt.expect_all(
    {
        "valid_order_id": "order_id IS NOT NULL AND TRIM(order_id) <> ''",
        "valid_customer_id": "customer_id IS NOT NULL",
        "valid_product_id": "product_id IS NOT NULL",
        "valid_quantity": "quantity IS NOT NULL AND quantity > 0",
        "valid_price": "price IS NOT NULL AND price > 0",
        "valid_ingest_ts": "ingest_ts IS NOT NULL AND ingest_ts <= current_timestamp()",
    }
)
def silver_orders_main():
    return dlt.read("silver_orders_curated_internal")


@dlt.table(
    name=OPS_SILVER_ORDERS_QUARANTINE,
    comment="Operational quarantine table for invalid silver orders.",
)
def silver_orders_quarantine_main():
    standardized_df = dlt.read("silver_orders_standardized_internal")

    invalid_condition = (
        F.col("order_id").isNull()
        | (F.col("order_id") == "")
        | F.col("customer_id").isNull()
        | F.col("product_id").isNull()
        | F.col("quantity").isNull()
        | (F.col("quantity") <= 0)
        | F.col("price").isNull()
        | (F.col("price") <= 0)
        | F.col("ingest_ts").isNull()
        | (F.col("ingest_ts") > F.current_timestamp())
        | F.col("_rescued_data").isNotNull()
    )

    return standardized_df.filter(invalid_condition).withColumn(
        "dq_reason",
        F.concat_ws(
            "; ",
            F.when(F.col("order_id").isNull() | (F.col("order_id") == ""), F.lit("invalid_order_id")),
            F.when(F.col("customer_id").isNull(), F.lit("missing_customer_id")),
            F.when(F.col("product_id").isNull(), F.lit("missing_product_id")),
            F.when(F.col("quantity").isNull() | (F.col("quantity") <= 0), F.lit("invalid_quantity")),
            F.when(F.col("price").isNull() | (F.col("price") <= 0), F.lit("invalid_price")),
            F.when(
                F.col("ingest_ts").isNull() | (F.col("ingest_ts") > F.current_timestamp()),
                F.lit("invalid_ingest_ts"),
            ),
            F.when(F.col("_rescued_data").isNotNull(), F.lit("rescued_data_present")),
        ),
    )


@dlt.view(name="fact_sales_joined_internal")
def fact_sales_joined_internal():
    orders_df = dlt.read("silver_orders_curated_internal").alias("orders")
    dim_product_df = (
        spark.read.table(GOLD_DIM_PRODUCT_TABLE)
        .filter(F.col("is_current") == F.lit(True))
        .select("product_sk", "product_id")
        .alias("dim_product")
    )
    dim_customer_df = spark.read.table(GOLD_DIM_CUSTOMER_TABLE).select("customer_id").alias("dim_customer")
    dim_date_df = spark.read.table(GOLD_DIM_DATE_TABLE).select("date_id", "full_date").alias("dim_date")

    return (
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


@dlt.table(
    name=MAIN_GOLD_FACT_SALES_TABLE,
    comment="Production-cutover curated fact sales table managed directly by DLT.",
)
@dlt.expect("order_id_not_null", "order_id IS NOT NULL")
@dlt.expect("product_fk_present", "product_sk IS NOT NULL")
@dlt.expect("customer_fk_present", "customer_id IS NOT NULL")
@dlt.expect("date_fk_present", "date_id IS NOT NULL")
@dlt.expect("valid_quantity", "quantity > 0")
@dlt.expect("valid_price", "price > 0")
@dlt.expect("valid_sales_amount", "sales_amount = quantity * price")
def fact_sales_main():
    joined_df = dlt.read("fact_sales_joined_internal")

    return joined_df.filter(
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


@dlt.table(
    name=OPS_FACT_SALES_QUARANTINE,
    comment="Operational quarantine table for unresolved fact sales rows.",
)
def fact_sales_quarantine_main():
    joined_df = dlt.read("fact_sales_joined_internal")

    return joined_df.filter(
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
