# Databricks notebook source
from pyspark.sql import functions as F

try:
    catalog = dbutils.widgets.get("catalog")
    schema_bronze = dbutils.widgets.get("schema_bronze")
    schema_silver = dbutils.widgets.get("schema_silver")
except:
    catalog = "natyra_demo"
    schema_bronze = "sales_bronze"
    schema_silver = "sales_silver"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_silver}")

df = spark.table(f"{catalog}.{schema_bronze}.raw_products")
df_silver = df.withColumn("price_tier", F.when(F.col("price") < 50, "Budget").when(F.col("price") < 200, "Mid-Range").when(F.col("price") < 500, "Premium").otherwise("Luxury")).withColumn("is_available", (F.col("status") == "active") & (F.col("stock_quantity") > 0)).withColumn("_transformed_at", F.current_timestamp()).select("product_id", "product_name", "category", "subcategory", "price", "price_tier", "stock_quantity", "is_available", "status", "_transformed_at", "_source_system")
df_silver.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema_silver}.dim_products")
print(f"dim_products: {df_silver.count()} records")
