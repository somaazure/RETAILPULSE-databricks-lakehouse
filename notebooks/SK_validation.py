# Databricks notebook source
# DBTITLE 1,Cell 1
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# Step 1: Select a sample product from silver.products
sample_pd = spark.sql("""
  SELECT product_id, product_name, category_id, price
  FROM retailpulse.silver.products
  LIMIT 1
""").first()

if not sample_pd:
    print("No products found in retailpulse.silver.products. Please populate the table.")
else:
    product_id = sample_pd['product_id']

    # Step 2: Insert initial version in gold.dim_product
    spark.sql(f"""
      INSERT INTO retailpulse.gold.dim_product
        (product_id, product_name, category_id, current_price, start_date, end_date, is_current)
      VALUES (
        {sample_pd['product_id']},
        '{sample_pd['product_name']}',
        {sample_pd['category_id']},
        {sample_pd['price']},
        '2024-06-01',
        NULL,
        TRUE
      )
    """)

    # Mark the first version as non-current
    spark.sql(f"""
      UPDATE retailpulse.gold.dim_product
      SET end_date = '2024-06-14', is_current = FALSE
      WHERE product_id = {sample_pd['product_id']} AND is_current = TRUE AND start_date = '2024-06-01'
    """)

    # Step 2.2: Insert second SCD2 version (price updated)
    spark.sql(f"""
      INSERT INTO retailpulse.gold.dim_product
        (product_id, product_name, category_id, current_price, start_date, end_date, is_current)
      VALUES (
        {sample_pd['product_id']},
        '{sample_pd['product_name']}',
        {sample_pd['category_id']},
        {sample_pd['price'] + 10},
        '2024-06-15',
        NULL,
        TRUE
      )
    """)

    # Step 3: Lookup both product_sk versions
    prod_versions = spark.sql(f"""
      SELECT product_sk, current_price, start_date, end_date, is_current
      FROM retailpulse.gold.dim_product
      WHERE product_id = {sample_pd['product_id']}
      ORDER BY start_date
    """).collect()

    print(f"Versions for product_id = {sample_pd['product_id']}:")
    for row in prod_versions:
        print(row.asDict())

    # Step 4: Insert fact_sales rows for both SKs
    for i, row in enumerate(prod_versions):
        spark.sql(f"""
          INSERT INTO retailpulse.gold.fact_sales
          (order_id, product_sk, customer_id, date_id, quantity, price, sales_amount, order_ts)
          VALUES (
            {i+1000},
            {row['product_sk']},
            202,
            {20240601 if i == 0 else 20240615},
            5,
            {row['current_price']},
            {5 * row['current_price']},
            '{row['start_date']}'
          )
        """)

    # Step 5: Validate mapping
    result_dim = spark.sql(f"""
      SELECT product_sk, product_id, product_name, current_price, start_date, end_date, is_current
      FROM retailpulse.gold.dim_product
      WHERE product_id = {sample_pd['product_id']}
      ORDER BY product_sk
    """)
    print("\nUpdated gold.dim_product:")
    result_dim.show(truncate=False)

    result_sales = spark.sql(f"""
      SELECT order_id, product_sk, customer_id, price, sales_amount, order_ts
      FROM retailpulse.gold.fact_sales
      WHERE product_sk IN ({','.join(str(r['product_sk']) for r in prod_versions)})
      ORDER BY product_sk
    """)
    print("\nInserted fact_sales rows:")
    result_sales.show(truncate=False)

    print("\nSCD2 simulation and SK mapping validation completed.")

# COMMAND ----------

from pyspark.sql.functions import col

# Cast timestamps to string for pandas compatibility
result_dim_string = result_dim.select(
    col("product_sk"),
    col("product_id"),
    col("product_name"),
    col("current_price"),
    col("start_date").cast("string"),
    col("end_date").cast("string"),
    col("is_current")
)
print("\nUpdated gold.dim_product:")
print(result_dim_string.toPandas())

result_sales_string = result_sales.select(
    col("order_id"),
    col("product_sk"),
    col("customer_id"),
    col("price"),
    col("sales_amount"),
    col("order_ts").cast("string")
)
print("\nInserted fact_sales rows:")
print(result_sales_string.toPandas())