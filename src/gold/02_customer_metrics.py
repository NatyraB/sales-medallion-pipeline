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
df_metrics = df.groupBy("customer_id", "customer_name", "customer_city", "customer_state").agg(F.countDistinct("order_id").alias("total_orders"), F.sum("quantity").alias("total_items"), F.round(F.sum("line_total"), 2).alias("total_revenue"), F.round(F.avg("line_total"), 2).alias("avg_order_value")).withColumn("customer_segment", F.when(F.col("total_revenue") >= 1000, "VIP").when(F.col("total_orders") >= 2, "Loyal").otherwise("Regular")).withColumn("_aggregated_at", F.current_timestamp())
df_metrics.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema_gold}.customer_metrics")
print(f"customer_metrics: {df_metrics.count()} records")
