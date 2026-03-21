"""End-to-end DLT pipeline for RetailPulse main Bronze, Silver, Gold, and quarantine tables."""

import dlt
from pyspark.sql import functions as F

SOURCE_PATH = "/Volumes/retailpulse/bronze/orders_files/orders/"
SCHEMA_LOCATION = "/Volumes/retailpulse/bronze/orders_files/checkpoints/dlt_orders_schema"

BRONZE_ORDERS_TABLE = "retailpulse.bronze.orders"
SILVER_ORDERS_TABLE = "retailpulse.silver.orders"
SILVER_PRODUCTS_TABLE = "retailpulse.silver.products"
GOLD_DIM_PRODUCT_TABLE = "retailpulse.gold.dim_product"
GOLD_DIM_CUSTOMER_TABLE = "retailpulse.gold.dim_customer"
GOLD_DIM_DATE_TABLE = "retailpulse.gold.dim_date"
GOLD_FACT_SALES_TABLE = "retailpulse.gold.fact_sales"
OPS_SILVER_ORDERS_QUARANTINE = "retailpulse.ops.silver_orders_quarantine"
OPS_FACT_SALES_QUARANTINE = "retailpulse.ops.fact_sales_quarantine"


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


@dlt.view(name="silver_orders_standardized_main_internal")
def silver_orders_standardized_main_internal():
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


@dlt.view(name="silver_orders_curated_main_internal")
def silver_orders_curated_main_internal():
    standardized_df = dlt.read("silver_orders_standardized_main_internal")

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
    comment="Curated main silver orders table managed by DLT.",
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
    return dlt.read("silver_orders_curated_main_internal")


@dlt.table(
    name=OPS_SILVER_ORDERS_QUARANTINE,
    comment="Operational quarantine table for invalid silver orders.",
)
def silver_orders_quarantine_main():
    standardized_df = dlt.read("silver_orders_standardized_main_internal")

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


@dlt.view(name="silver_products_main_internal")
def silver_products_main_internal():
    return (
        spark.range(100, 1000)
        .select(
            F.col("id").cast("int").alias("product_id"),
            F.concat(F.lit("Product-"), F.col("id")).alias("product_name"),
            ((F.col("id") % 10) + F.lit(10)).cast("int").alias("category_id"),
            (F.round((F.col("id") * F.lit(1.75)) % F.lit(495) + F.lit(5), 2)).cast("decimal(12,2)").alias("price"),
        )
    )


@dlt.table(
    name=SILVER_PRODUCTS_TABLE,
    comment="Deterministic product master covering generated product IDs 100-999.",
)
@dlt.expect("product_id_not_null", "product_id IS NOT NULL")
@dlt.expect("valid_category_id", "category_id BETWEEN 10 AND 19")
@dlt.expect("valid_price", "price > 0")
def silver_products_main():
    return dlt.read("silver_products_main_internal")


@dlt.view(name="dim_product_main_internal")
def dim_product_main_internal():
    return dlt.read("silver_products_main_internal").select(
        F.col("product_id").cast("bigint").alias("product_sk"),
        F.col("product_id"),
        F.col("product_name"),
        F.col("category_id"),
        F.col("price").cast("decimal(12,2)").alias("current_price"),
        F.current_timestamp().alias("start_date"),
        F.to_timestamp(F.lit("9999-12-31 00:00:00")).alias("end_date"),
        F.lit(True).alias("is_current"),
    )


@dlt.table(
    name=GOLD_DIM_PRODUCT_TABLE,
    comment="Current product dimension for generated product master.",
)
@dlt.expect("product_id_not_null", "product_id IS NOT NULL")
@dlt.expect("product_name_not_blank", "product_name IS NOT NULL AND TRIM(product_name) <> ''")
@dlt.expect("valid_category_id", "category_id BETWEEN 10 AND 19")
@dlt.expect("valid_current_price", "current_price > 0")
def dim_product_main():
    return dlt.read("dim_product_main_internal")


@dlt.view(name="dim_customer_main_internal")
def dim_customer_main_internal():
    return (
        dlt.read("silver_orders_curated_main_internal")
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


@dlt.table(
    name=GOLD_DIM_CUSTOMER_TABLE,
    comment="Customer dimension derived from curated silver orders.",
)
@dlt.expect("customer_id_not_null", "customer_id IS NOT NULL")
def dim_customer_main():
    return dlt.read("dim_customer_main_internal")


@dlt.view(name="dim_date_main_internal")
def dim_date_main_internal():
    return (
        dlt.read("silver_orders_curated_main_internal")
        .select(F.to_date(F.col("ingest_ts")).alias("full_date"))
        .dropDuplicates(["full_date"])
        .select(
            F.date_format(F.col("full_date"), "yyyyMMdd").cast("int").alias("date_id"),
            F.col("full_date"),
            F.dayofmonth(F.col("full_date")).alias("day"),
            F.month(F.col("full_date")).alias("month"),
            F.year(F.col("full_date")).alias("year"),
            F.weekofyear(F.col("full_date")).alias("week_of_year"),
        )
    )


@dlt.table(
    name=GOLD_DIM_DATE_TABLE,
    comment="Date dimension derived from curated silver orders.",
)
@dlt.expect("date_id_not_null", "date_id IS NOT NULL")
def dim_date_main():
    return dlt.read("dim_date_main_internal")


@dlt.view(name="fact_sales_joined_e2e_internal")
def fact_sales_joined_e2e_internal():
    orders_df = dlt.read("silver_orders_curated_main_internal").alias("orders")
    dim_product_df = dlt.read("dim_product_main_internal").select("product_sk", "product_id").alias("dim_product")
    dim_customer_df = dlt.read("dim_customer_main_internal").select("customer_id").alias("dim_customer")
    dim_date_df = dlt.read("dim_date_main_internal").select("date_id", "full_date").alias("dim_date")

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
    name=GOLD_FACT_SALES_TABLE,
    comment="Curated gold fact sales table with DQ checks and quarantine handling.",
)
@dlt.expect("order_id_not_null", "order_id IS NOT NULL")
@dlt.expect("product_fk_present", "product_sk IS NOT NULL")
@dlt.expect("customer_fk_present", "customer_id IS NOT NULL")
@dlt.expect("date_fk_present", "date_id IS NOT NULL")
@dlt.expect("valid_quantity", "quantity > 0")
@dlt.expect("valid_price", "price > 0")
@dlt.expect("valid_sales_amount", "sales_amount = quantity * price")
def fact_sales_main():
    joined_df = dlt.read("fact_sales_joined_e2e_internal")

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
    comment="Operational quarantine table for unresolved fact rows.",
)
def fact_sales_quarantine_main():
    joined_df = dlt.read("fact_sales_joined_e2e_internal")

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
