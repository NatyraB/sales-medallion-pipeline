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
    (101, "Laptop Pro 15", "Electronics", "Computers", 1299.99, 50, "active"),
    (102, "Wireless Mouse", "Electronics", "Accessories", 29.99, 200, "active"),
    (103, "USB-C Hub", "Electronics", "Accessories", 49.99, 150, "active"),
    (104, "Monitor 27inch", "Electronics", "Displays", 399.99, 75, "active"),
    (105, "Office Chair", "Furniture", "Seating", 299.99, 40, "active"),
]

schema_def = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("subcategory", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("stock_quantity", IntegerType(), True),
    StructField("status", StringType(), True),
])

df = spark.createDataFrame(raw_data, schema_def).withColumn("_ingested_at", F.current_timestamp()).withColumn("_source_system", F.lit("INVENTORY"))
df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.raw_products")
print(f"raw_products: {df.count()} records")
