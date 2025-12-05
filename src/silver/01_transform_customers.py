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

df = spark.table(f"{catalog}.{schema_bronze}.raw_customers")
df_silver = df.withColumn("full_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name"))).withColumn("email", F.lower(F.col("email"))).withColumn("is_active", F.when(F.col("status") == "active", True).otherwise(False)).withColumn("_transformed_at", F.current_timestamp()).select("customer_id", "first_name", "last_name", "full_name", "email", "phone", "city", "state", "is_active", "_transformed_at", "_source_system")
df_silver.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema_silver}.dim_customers")
print(f"dim_customers: {df_silver.count()} records")
