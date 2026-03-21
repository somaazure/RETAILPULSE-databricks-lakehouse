"""Enterprise hybrid DLT pipeline for Bronze and Silver only."""

import dlt
from pyspark.sql import functions as F

SOURCE_PATH = "/Volumes/retailpulse/bronze/orders_files/orders/"
SCHEMA_LOCATION = "/Volumes/retailpulse/bronze/orders_files/checkpoints/dlt_orders_schema"

BRONZE_ORDERS_TABLE = "retailpulse.bronze.orders"
SILVER_ORDERS_TABLE = "retailpulse.silver.orders"
OPS_SILVER_ORDERS_QUARANTINE = "retailpulse.ops.silver_orders_quarantine"


@dlt.table(
    name=BRONZE_ORDERS_TABLE,
    comment="Bronze orders ingested from landed CSV files via Auto Loader in DLT.",
)
def bronze_orders_main():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .option("header", "true")
        .load(SOURCE_PATH)
        .select(
            F.col("order_id"),
            F.col("customer_id"),
            F.col("product_id"),
            F.col("quantity"),
            F.col("price"),
            F.col("_rescued_data"),
            F.col("_metadata.file_path").alias("source_file_name"),
            F.current_timestamp().alias("ingest_ts"),
        )
    )


@dlt.view(name="silver_orders_standardized_enterprise_internal")
def silver_orders_standardized_enterprise_internal():
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


@dlt.view(name="silver_orders_curated_enterprise_internal")
def silver_orders_curated_enterprise_internal():
    standardized_df = dlt.read("silver_orders_standardized_enterprise_internal")

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
    name=SILVER_ORDERS_TABLE,
    comment="Enterprise curated silver orders table managed by DLT.",
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
    return dlt.read("silver_orders_curated_enterprise_internal")


@dlt.table(
    name=OPS_SILVER_ORDERS_QUARANTINE,
    comment="Operational quarantine table for invalid silver orders.",
)
def silver_orders_quarantine_main():
    standardized_df = dlt.read("silver_orders_standardized_enterprise_internal")

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
