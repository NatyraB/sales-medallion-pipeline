# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 Gold Layer - Country Analytics
# MAGIC 
# MAGIC This notebook creates aggregated country-level analytics with:
# MAGIC - Account counts by country
# MAGIC - Revenue and employee aggregations
# MAGIC - Business intelligence metrics

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

print(f"📊 Gold Country Analytics Configuration:")
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
# MAGIC ## Create Country-Level Aggregations

# COMMAND ----------

# Aggregate metrics by country
df_country_analytics = df_silver.groupBy("country").agg(
    # Count metrics
    F.count("*").alias("total_accounts"),
    F.countDistinct("industry").alias("unique_industries"),
    
    # Employee metrics
    F.sum("employee_count").alias("total_employees"),
    F.avg("employee_count").alias("avg_employees_per_account"),
    F.max("employee_count").alias("max_employees"),
    
    # Revenue metrics
    F.sum("annual_revenue_abs").alias("total_revenue"),
    F.avg("annual_revenue_abs").alias("avg_revenue_per_account"),
    F.max("annual_revenue_abs").alias("max_revenue"),
    
    # Size distribution
    F.sum(F.when(F.col("company_size") == "Enterprise", 1).otherwise(0)).alias("enterprise_accounts"),
    F.sum(F.when(F.col("company_size") == "Large", 1).otherwise(0)).alias("large_accounts"),
    F.sum(F.when(F.col("company_size") == "Medium", 1).otherwise(0)).alias("medium_accounts"),
    F.sum(F.when(F.col("company_size") == "Small", 1).otherwise(0)).alias("small_accounts"),
    F.sum(F.when(F.col("company_size") == "Micro", 1).otherwise(0)).alias("micro_accounts")
)

# Add calculated metrics and rankings
window_revenue = Window.orderBy(F.desc("total_revenue"))
window_accounts = Window.orderBy(F.desc("total_accounts"))

df_country_analytics = df_country_analytics.withColumn(
    "revenue_rank", F.rank().over(window_revenue)
).withColumn(
    "accounts_rank", F.rank().over(window_accounts)
).withColumn(
    "revenue_per_employee", 
    F.when(F.col("total_employees") > 0, 
           F.round(F.col("total_revenue") / F.col("total_employees"), 2))
     .otherwise(0)
).withColumn(
    "processed_timestamp", F.current_timestamp()
)

print(f"📊 Country analytics records: {df_country_analytics.count()}")
df_country_analytics.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold Table

# COMMAND ----------

gold_table = f"{catalog}.{schema}.gold_country_analytics"

# Overwrite for aggregation tables (full refresh)
df_country_analytics.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(gold_table)

print(f"✅ Gold country analytics table updated: {gold_table}")

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
'Gold layer - Country-level aggregated analytics for accounts. Includes revenue metrics, employee counts, and account distributions by country.'
""")

# Add column comments
column_comments = {
    "country": "Primary Key - Country code",
    "total_accounts": "Total number of accounts in country",
    "unique_industries": "Count of distinct industries represented",
    "total_employees": "Sum of all employees across accounts",
    "avg_employees_per_account": "Average employee count per account",
    "max_employees": "Maximum employee count for single account",
    "total_revenue": "Sum of annual revenue across accounts (USD)",
    "avg_revenue_per_account": "Average revenue per account (USD)",
    "max_revenue": "Maximum revenue for single account (USD)",
    "enterprise_accounts": "Count of Enterprise-sized accounts",
    "large_accounts": "Count of Large-sized accounts",
    "medium_accounts": "Count of Medium-sized accounts",
    "small_accounts": "Count of Small-sized accounts",
    "micro_accounts": "Count of Micro-sized accounts",
    "revenue_rank": "Rank by total revenue (1 = highest)",
    "accounts_rank": "Rank by account count (1 = highest)",
    "revenue_per_employee": "Revenue efficiency metric (revenue/employees)",
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

print(f"📊 Country Analytics Summary:")
spark.sql(f"""
    SELECT country, total_accounts, total_revenue, revenue_rank
    FROM {gold_table}
    ORDER BY revenue_rank
""").display()
