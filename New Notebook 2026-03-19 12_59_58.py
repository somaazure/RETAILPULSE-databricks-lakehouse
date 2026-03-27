# Databricks notebook source
display(dbutils.fs.ls("dbfs:/Volumes/retailpulse/bronze/orders_files/orders/"))

# COMMAND ----------

for q in spark.streams.active:
    print(q.id, q.status)


# COMMAND ----------

spark.read.table("retailpulse.bronze.orders").display()

# COMMAND ----------

spark.read.table("retailpulse.silver.orders").display()