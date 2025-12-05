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

df_orders = spark.table(f"{catalog}.{schema_bronze}.raw_orders").filter(F.col("quantity") > 0)
df_customers = spark.table(f"{catalog}.{schema_silver}.dim_customers")
df_products = spark.table(f"{catalog}.{schema_silver}.dim_products")

df_silver = df_orders.withColumn("order_date", F.to_timestamp(F.col("order_date"))).withColumn("order_date_only", F.to_date(F.col("order_date"))).withColumn("line_total", F.round(F.col("quantity") * F.col("unit_price"), 2)).withColumn("status_category", F.when(F.col("status") == "delivered", "Completed").otherwise("In Progress")).join(df_customers.select(F.col("customer_id"), F.col("full_name").alias("customer_name"), F.col("city").alias("customer_city"), F.col("state").alias("customer_state")), "customer_id", "left").join(df_products.select(F.col("product_id"), F.col("product_name"), F.col("category").alias("product_category"), F.col("price_tier")), "product_id", "left").withColumn("_transformed_at", F.current_timestamp())
df_silver.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema_silver}.fact_orders")
print(f"fact_orders: {df_silver.count()} records")
