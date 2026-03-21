"""Create and maintain RetailPulse dimension tables."""

from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, StringType, StructField, StructType

spark = SparkSession.builder.getOrCreate()

SILVER_ORDERS_TABLE = "retailpulse.silver.orders"
SILVER_PRODUCTS_TABLE = "retailpulse.silver.products"
GOLD_DIM_PRODUCT_TABLE = "retailpulse.gold.dim_product"
GOLD_DIM_CUSTOMER_TABLE = "retailpulse.gold.dim_customer"
GOLD_DIM_DATE_TABLE = "retailpulse.gold.dim_date"


def ensure_target_schemas() -> None:
    """Create the target catalog and schemas if they do not exist."""
    spark.sql("CREATE CATALOG IF NOT EXISTS retailpulse")
    spark.sql("CREATE SCHEMA IF NOT EXISTS retailpulse.silver")
    spark.sql("CREATE SCHEMA IF NOT EXISTS retailpulse.gold")



def build_products_dataframe() -> DataFrame:
    """Create a small sample products dataset for downstream modeling."""
    product_data = [
        (101, "Wireless Mouse", 10, Decimal("24.99")),
        (102, "Mechanical Keyboard", 10, Decimal("79.99")),
        (103, "USB-C Charger", 11, Decimal("19.50")),
        (104, "Noise Cancelling Headphones", 12121, Decimal("149.00")),
        (105, "Laptop Stand", 13, Decimal("39.95")),
    ]

    schema = StructType(
        [
            StructField("product_id", IntegerType(), False),
            StructField("product_name", StringType(), False),
            StructField("category_id", IntegerType(), False),
            StructField("price", DecimalType(12, 2), False),
        ]
    )

    return spark.createDataFrame(product_data, schema=schema)



def write_products_table(products_df: DataFrame) -> None:
    """Persist the sample product master as a Delta table."""
    (
        products_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(SILVER_PRODUCTS_TABLE)
    )



def ensure_dim_product_table() -> None:
    """Create the SCD2 product dimension target table if it does not exist."""
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



def prepare_dim_product_stage() -> None:
    """Stage the current silver product snapshot for SCD2 processing."""
    staged_df = (
        spark.table(SILVER_PRODUCTS_TABLE)
        .filter(F.col("product_id").isNotNull())
        .select(
            F.col("product_id").cast("int").alias("product_id"),
            F.col("product_name").cast("string").alias("product_name"),
            F.col("category_id").cast("int").alias("category_id"),
            F.col("price").cast("decimal(12,2)").alias("current_price"),
            F.current_timestamp().alias("start_date"),
        )
    )

    staged_df.createOrReplaceTempView("stg_dim_product")



def expire_changed_dim_product_rows() -> None:
    """Close existing current rows when tracked attributes change."""
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



def insert_new_dim_product_versions() -> None:
    """Insert brand new products and changed current versions."""
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



def build_dim_customer_dataframe() -> DataFrame:
    """Create a customer dimension from unique customer ids in silver orders."""
    return (
        spark.table(SILVER_ORDERS_TABLE)
        .filter(F.col("customer_id").isNotNull())
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



def write_dim_customer_table(dim_customer_df: DataFrame) -> None:
    """Persist the customer dimension as a Delta table."""
    (
        dim_customer_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(GOLD_DIM_CUSTOMER_TABLE)
    )



def build_dim_date_dataframe() -> DataFrame:
    """Create a date dimension from the order timestamp available in silver orders."""
    return (
        spark.table(SILVER_ORDERS_TABLE)
        .filter(F.col("ingest_ts").isNotNull())
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
        .orderBy("full_date")
    )



def write_dim_date_table(dim_date_df: DataFrame) -> None:
    """Persist the date dimension as a Delta table."""
    (
        dim_date_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(GOLD_DIM_DATE_TABLE)
    )



def main() -> None:
    """Build the silver products table and maintain gold dimensions."""
    ensure_target_schemas()

    products_df = build_products_dataframe()
    write_products_table(products_df)

    ensure_dim_product_table()
    prepare_dim_product_stage()
    expire_changed_dim_product_rows()
    insert_new_dim_product_versions()

    dim_customer_df = build_dim_customer_dataframe()
    write_dim_customer_table(dim_customer_df)

    dim_date_df = build_dim_date_dataframe()
    write_dim_date_table(dim_date_df)

    print(f"Created sample products table: {SILVER_PRODUCTS_TABLE}")
    print(f"Silver products row count: {products_df.count()}")
    print(f"Updated SCD2 dimension table: {GOLD_DIM_PRODUCT_TABLE}")
    print(f"Current dim_product row count: {spark.table(GOLD_DIM_PRODUCT_TABLE).count()}")
    print(f"Created customer dimension table: {GOLD_DIM_CUSTOMER_TABLE}")
    print(f"dim_customer row count: {dim_customer_df.count()}")
    print(f"Created date dimension table: {GOLD_DIM_DATE_TABLE}")
    print(f"dim_date row count: {dim_date_df.count()}")


if __name__ == "__main__":
    main()
