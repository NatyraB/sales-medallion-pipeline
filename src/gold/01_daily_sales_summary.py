# Databricks notebook source
from pyspark.sql import functions as F

try:
    catalog = dbutils.widgets.get("catalog")
    schema_silver = dbutils.widgets.get("schema_silver")
    schema_gold = dbutils.widgets.get("schema_gold")
except:
    catalog = "natyra_demo"
    schema_silver = "sales_silver"
    schema_gold = "sales_gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_gold}")

df = spark.table(f"{catalog}.{schema_silver}.fact_orders").filter(F.col("status_category") == "Completed")
df_daily = df.groupBy("order_date_only").agg(F.countDistinct("order_id").alias("total_orders"), F.sum("quantity").alias("total_units_sold"), F.round(F.sum("line_total"), 2).alias("total_revenue"), F.countDistinct("customer_id").alias("unique_customers")).withColumn("_aggregated_at", F.current_timestamp()).orderBy("order_date_only")
df_daily.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema_gold}.daily_sales_summary")
print(f"daily_sales_summary: {df_daily.count()} records")
