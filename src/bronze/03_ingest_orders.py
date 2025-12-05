# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *

try:
    catalog = dbutils.widgets.get("catalog")
    schema = dbutils.widgets.get("schema")
except:
    catalog = "natyra_demo"
    schema = "sales_bronze"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

raw_data = [
    (1001, 1, 101, 1, 1299.99, "2024-01-05 10:30:00", "delivered", "credit_card"),
    (1002, 1, 102, 2, 29.99, "2024-01-05 10:30:00", "delivered", "credit_card"),
    (1003, 2, 104, 1, 399.99, "2024-01-08 14:15:00", "delivered", "paypal"),
    (1004, 3, 105, 1, 299.99, "2024-01-10 09:45:00", "delivered", "credit_card"),
    (1005, 4, 101, 1, 1299.99, "2024-01-12 16:20:00", "delivered", "debit_card"),
    (1006, 5, 103, 2, 49.99, "2024-01-15 11:00:00", "delivered", "credit_card"),
]

schema_def = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("order_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("payment_method", StringType(), True),
])

df = spark.createDataFrame(raw_data, schema_def).withColumn("_ingested_at", F.current_timestamp()).withColumn("_source_system", F.lit("ORDERS"))
df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.raw_orders")
print(f"raw_orders: {df.count()} records")
