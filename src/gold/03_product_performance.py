# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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
df_perf = df.groupBy("product_id", "product_name", "product_category", "price_tier").agg(F.countDistinct("order_id").alias("times_ordered"), F.sum("quantity").alias("total_units_sold"), F.round(F.sum("line_total"), 2).alias("total_revenue"), F.countDistinct("customer_id").alias("unique_customers")).withColumn("revenue_rank", F.dense_rank().over(Window.orderBy(F.desc("total_revenue")))).withColumn("_aggregated_at", F.current_timestamp())
df_perf.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema_gold}.product_performance")
print(f"product_performance: {df_perf.count()} records")
