"""Delta Live Tables pipeline for RetailPulse with data quality checks."""

import dlt
from pyspark.sql import functions as F

BRONZE_ORDERS_TABLE = "retailpulse.bronze.orders"
SILVER_PRODUCTS_TABLE = "retailpulse.silver.products"
GOLD_DIM_PRODUCT_TABLE = "retailpulse.gold.dim_product"
GOLD_DIM_CUSTOMER_TABLE = "retailpulse.gold.dim_customer"
GOLD_DIM_DATE_TABLE = "retailpulse.gold.dim_date"


@dlt.table(
    name="silver_orders_dlt",
    comment="Curated silver orders with DLT expectations applied to bronze data.",
)
@dlt.expect_all_or_drop(
    {
        "valid_order_id": "order_id IS NOT NULL AND TRIM(order_id) <> ''",
        "valid_customer_id": "customer_id IS NOT NULL",
        "valid_product_id": "product_id IS NOT NULL",
        "valid_quantity": "quantity IS NOT NULL AND quantity > 0",
        "valid_price": "price IS NOT NULL AND price > 0",
        "valid_ingest_ts": "ingest_ts IS NOT NULL AND ingest_ts <= current_timestamp()",
    }
)
def silver_orders_dlt():
    bronze_df = spark.read.table(BRONZE_ORDERS_TABLE)

    standardized_df = bronze_df.select(
        F.trim(F.col("order_id")).cast("string").alias("order_id"),
        F.col("customer_id").cast("int").alias("customer_id"),
        F.col("product_id").cast("int").alias("product_id"),
        F.col("quantity").cast("int").alias("quantity"),
        F.col("price").cast("decimal(12,2)").alias("price"),
        F.col("source_file_name").cast("string").alias("source_file_name"),
        F.col("ingest_ts").alias("ingest_ts"),
        F.col("_rescued_data").cast("string").alias("_rescued_data"),
    )

    valid_orders_df = standardized_df.filter(F.col("_rescued_data").isNull())

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
    name="silver_orders_quarantine",
    comment="Rejected silver order rows that fail DQ conditions.",
)
def silver_orders_quarantine():
    bronze_df = spark.read.table(BRONZE_ORDERS_TABLE)

    standardized_df = bronze_df.select(
        F.trim(F.col("order_id")).cast("string").alias("order_id"),
        F.col("customer_id").cast("int").alias("customer_id"),
        F.col("product_id").cast("int").alias("product_id"),
        F.col("quantity").cast("int").alias("quantity"),
        F.col("price").cast("decimal(12,2)").alias("price"),
        F.col("source_file_name").cast("string").alias("source_file_name"),
        F.col("ingest_ts").alias("ingest_ts"),
        F.col("_rescued_data").cast("string").alias("_rescued_data"),
    )

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


@dlt.table(
    name="dim_product_current_dlt",
    comment="Current product dimension rows validated for downstream fact joins.",
)
@dlt.expect("product_id_not_null", "product_id IS NOT NULL")
@dlt.expect("product_name_not_blank", "product_name IS NOT NULL AND TRIM(product_name) <> ''")
@dlt.expect("valid_category_id", "category_id BETWEEN 1 AND 999")
@dlt.expect("valid_current_price", "current_price IS NOT NULL AND current_price > 0")
@dlt.expect("valid_scd_dates", "start_date IS NOT NULL AND end_date IS NOT NULL AND start_date < end_date")
def dim_product_current_dlt():
    return spark.read.table(GOLD_DIM_PRODUCT_TABLE).filter(F.col("is_current") == F.lit(True))


@dlt.table(
    name="dim_customer_dlt",
    comment="Customer dimension with domain checks for segment and status.",
)
@dlt.expect("customer_id_not_null", "customer_id IS NOT NULL")
@dlt.expect("customer_name_not_blank", "customer_name IS NOT NULL AND TRIM(customer_name) <> ''")
@dlt.expect(
    "valid_customer_segment",
    "customer_segment IN ('Premium', 'Standard', 'Basic')",
)
@dlt.expect(
    "valid_customer_status",
    "customer_status IN ('Active', 'Inactive')",
)
def dim_customer_dlt():
    return spark.read.table(GOLD_DIM_CUSTOMER_TABLE)


@dlt.table(
    name="dim_date_dlt",
    comment="Date dimension with calendar range validation.",
)
@dlt.expect("date_id_not_null", "date_id IS NOT NULL")
@dlt.expect("full_date_not_null", "full_date IS NOT NULL")
@dlt.expect("valid_day", "day BETWEEN 1 AND 31")
@dlt.expect("valid_month", "month BETWEEN 1 AND 12")
@dlt.expect("valid_year", "year BETWEEN 2020 AND 2100")
@dlt.expect("valid_week_of_year", "week_of_year BETWEEN 1 AND 53")
def dim_date_dlt():
    return spark.read.table(GOLD_DIM_DATE_TABLE)


@dlt.table(
    name="silver_products_dlt",
    comment="Validated silver products source for dimension maintenance.",
)
@dlt.expect("product_id_not_null", "product_id IS NOT NULL")
@dlt.expect("product_name_not_blank", "product_name IS NOT NULL AND TRIM(product_name) <> ''")
@dlt.expect("valid_category_id", "category_id BETWEEN 1 AND 999")
@dlt.expect("valid_price", "price IS NOT NULL AND price > 0")
def silver_products_dlt():
    return spark.read.table(SILVER_PRODUCTS_TABLE)


@dlt.table(
    name="fact_sales_dlt",
    comment="Gold fact sales table with validated dimension joins and amount checks.",
)
@dlt.expect_or_fail("order_id_not_null", "order_id IS NOT NULL")
@dlt.expect("product_fk_present", "product_sk IS NOT NULL")
@dlt.expect("customer_fk_present", "customer_id IS NOT NULL")
@dlt.expect("date_fk_present", "date_id IS NOT NULL")
@dlt.expect("valid_quantity", "quantity > 0")
@dlt.expect("valid_price", "price > 0")
@dlt.expect("valid_sales_amount", "sales_amount = quantity * price")
def fact_sales_dlt():
    orders_df = dlt.read("silver_orders_dlt").alias("orders")
    dim_product_df = dlt.read("dim_product_current_dlt").select("product_sk", "product_id").alias("dim_product")
    dim_customer_df = dlt.read("dim_customer_dlt").select("customer_id").alias("dim_customer")
    dim_date_df = dlt.read("dim_date_dlt").select("date_id", "full_date").alias("dim_date")

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
            F.col("orders.customer_id").alias("customer_id"),
            F.col("dim_date.date_id").alias("date_id"),
            F.col("orders.quantity").alias("quantity"),
            F.col("orders.price").alias("price"),
            (F.col("orders.quantity") * F.col("orders.price")).alias("sales_amount"),
            F.col("orders.ingest_ts").alias("order_ts"),
        )
        .dropDuplicates(["order_id"])
    )


@dlt.table(
    name="fact_sales_quarantine",
    comment="Fact rows that failed dimension resolution before entering gold facts.",
)
def fact_sales_quarantine():
    orders_df = dlt.read("silver_orders_dlt").alias("orders")
    dim_product_df = dlt.read("dim_product_current_dlt").select("product_sk", "product_id").alias("dim_product")
    dim_customer_df = dlt.read("dim_customer_dlt").select("customer_id").alias("dim_customer")
    dim_date_df = dlt.read("dim_date_dlt").select("date_id", "full_date").alias("dim_date")

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
    )

    return joined_df.filter(
        F.col("dim_product.product_sk").isNull()
        | F.col("dim_customer.customer_id").isNull()
        | F.col("dim_date.date_id").isNull()
    ).select(
        F.col("orders.order_id").alias("order_id"),
        F.col("orders.customer_id").alias("customer_id"),
        F.col("orders.product_id").alias("product_id"),
        F.col("orders.quantity").alias("quantity"),
        F.col("orders.price").alias("price"),
        F.col("orders.ingest_ts").alias("order_ts"),
        F.concat_ws(
            "; ",
            F.when(F.col("dim_product.product_sk").isNull(), F.lit("missing_product_dimension")),
            F.when(F.col("dim_customer.customer_id").isNull(), F.lit("missing_customer_dimension")),
            F.when(F.col("dim_date.date_id").isNull(), F.lit("missing_date_dimension")),
        ).alias("dq_reason"),
    )





