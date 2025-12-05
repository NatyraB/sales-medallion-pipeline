# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 Gold Layer - Company Size Analytics
# MAGIC 
# MAGIC This notebook creates aggregated company size analytics with:
# MAGIC - Metrics segmented by company size classification
# MAGIC - Revenue and employee distributions
# MAGIC - Business insights by size tier

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

# Get parameters from job or use defaults
try:
    catalog = dbutils.widgets.get("catalog")
    schema = dbutils.widgets.get("schema")
except:
    catalog = "natyra_demo"
    schema = "sales"

print(f"📊 Gold Size Analytics Configuration:")
print(f"   Catalog: {catalog}")
print(f"   Schema: {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Catalog and Schema

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read from Silver Layer

# COMMAND ----------

silver_table = f"{catalog}.{schema}.silver_accounts"
df_silver = spark.table(silver_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Size-Level Aggregations

# COMMAND ----------

# Aggregate metrics by company size
df_size_analytics = df_silver.groupBy("company_size").agg(
    # Count metrics
    F.count("*").alias("total_accounts"),
    F.countDistinct("country").alias("unique_countries"),
    F.countDistinct("industry").alias("unique_industries"),
    
    # Employee metrics
    F.sum("employee_count").alias("total_employees"),
    F.avg("employee_count").alias("avg_employees"),
    F.min("employee_count").alias("min_employees"),
    F.max("employee_count").alias("max_employees"),
    
    # Revenue metrics
    F.sum("annual_revenue_abs").alias("total_revenue"),
    F.avg("annual_revenue_abs").alias("avg_revenue"),
    F.min("annual_revenue_abs").alias("min_revenue"),
    F.max("annual_revenue_abs").alias("max_revenue"),
    F.stddev("annual_revenue_abs").alias("revenue_stddev"),
    
    # Revenue tier distribution
    F.sum(F.when(F.col("revenue_tier") == "Tier 1 - $100M+", 1).otherwise(0)).alias("tier1_accounts"),
    F.sum(F.when(F.col("revenue_tier") == "Tier 2 - $10M-$100M", 1).otherwise(0)).alias("tier2_accounts"),
    F.sum(F.when(F.col("revenue_tier") == "Tier 3 - $1M-$10M", 1).otherwise(0)).alias("tier3_accounts"),
    F.sum(F.when(F.col("revenue_tier") == "Tier 4 - Under $1M", 1).otherwise(0)).alias("tier4_accounts")
)

# Add size ordering and calculated metrics
size_order = F.when(F.col("company_size") == "Enterprise", 1) \
              .when(F.col("company_size") == "Large", 2) \
              .when(F.col("company_size") == "Medium", 3) \
              .when(F.col("company_size") == "Small", 4) \
              .otherwise(5)

# Calculate total for percentage
total_accounts = df_silver.count()
total_revenue = df_silver.agg(F.sum("annual_revenue_abs")).collect()[0][0] or 0

df_size_analytics = df_size_analytics.withColumn(
    "size_order", size_order
).withColumn(
    "pct_of_total_accounts", 
    F.round((F.col("total_accounts") / F.lit(total_accounts)) * 100, 2)
).withColumn(
    "pct_of_total_revenue",
    F.round((F.col("total_revenue") / F.lit(total_revenue)) * 100, 2)
).withColumn(
    "revenue_per_employee",
    F.when(F.col("total_employees") > 0,
           F.round(F.col("total_revenue") / F.col("total_employees"), 2))
     .otherwise(0)
).withColumn(
    "processed_timestamp", F.current_timestamp()
).orderBy("size_order")

print(f"📊 Size analytics records: {df_size_analytics.count()}")
df_size_analytics.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold Table

# COMMAND ----------

gold_table = f"{catalog}.{schema}.gold_size_analytics"

# Overwrite for aggregation tables (full refresh)
df_size_analytics.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(gold_table)

print(f"✅ Gold size analytics table updated: {gold_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Table Metadata

# COMMAND ----------

# Add table properties
spark.sql(f"""
ALTER TABLE {gold_table}
SET TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'quality_tier' = 'gold',
    'data_domain' = 'sales',
    'pipeline_name' = 'medallion-accounts',
    'table_type' = 'aggregation',
    'refresh_pattern' = 'full_overwrite'
)
""")

spark.sql(f"""
COMMENT ON TABLE {gold_table} IS 
'Gold layer - Company size segmented analytics. Provides revenue and employee metrics by company size classification (Enterprise, Large, Medium, Small, Micro).'
""")

# Add column comments
column_comments = {
    "company_size": "Primary Key - Company size classification",
    "total_accounts": "Total number of accounts in size segment",
    "unique_countries": "Count of distinct countries represented",
    "unique_industries": "Count of distinct industries represented",
    "total_employees": "Sum of all employees across accounts",
    "avg_employees": "Average employee count per account",
    "min_employees": "Minimum employee count in segment",
    "max_employees": "Maximum employee count in segment",
    "total_revenue": "Sum of annual revenue (USD)",
    "avg_revenue": "Average revenue per account (USD)",
    "min_revenue": "Minimum revenue in segment (USD)",
    "max_revenue": "Maximum revenue in segment (USD)",
    "revenue_stddev": "Standard deviation of revenue (USD)",
    "tier1_accounts": "Count of Tier 1 ($100M+) accounts",
    "tier2_accounts": "Count of Tier 2 ($10M-$100M) accounts",
    "tier3_accounts": "Count of Tier 3 ($1M-$10M) accounts",
    "tier4_accounts": "Count of Tier 4 (Under $1M) accounts",
    "size_order": "Sort order (1=Enterprise to 5=Micro)",
    "pct_of_total_accounts": "Percentage of total account count",
    "pct_of_total_revenue": "Percentage of total revenue",
    "revenue_per_employee": "Revenue efficiency (revenue/employees)",
    "processed_timestamp": "Processing timestamp"
}

for col_name, comment in column_comments.items():
    try:
        spark.sql(f"ALTER TABLE {gold_table} ALTER COLUMN {col_name} COMMENT '{comment}'")
    except Exception as e:
        print(f"Warning: Could not add comment for {col_name}: {e}")

print(f"✅ Table metadata updated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Summary

# COMMAND ----------

print(f"📊 Size Analytics Summary:")
spark.sql(f"""
    SELECT company_size, total_accounts, pct_of_total_accounts, 
           total_revenue, pct_of_total_revenue
    FROM {gold_table}
    ORDER BY size_order
""").display()
