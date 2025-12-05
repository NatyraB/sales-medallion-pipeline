# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 Gold Layer - Curated Accounts Dimension
# MAGIC 
# MAGIC This notebook creates a business-ready accounts dimension table with:
# MAGIC - Clean, curated columns for analytics
# MAGIC - Proper column ordering for business use
# MAGIC - Comprehensive metadata and documentation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

# Get parameters from job or use defaults
try:
    catalog = dbutils.widgets.get("catalog")
    schema = dbutils.widgets.get("schema")
except:
    catalog = "natyra_demo"
    schema = "sales"

print(f"📊 Gold Layer Configuration:")
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

print(f"📥 Silver records to process: {df_silver.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Gold Accounts Dimension

# COMMAND ----------

# Select and order columns for business consumption
df_gold_accounts = df_silver.select(
    # Primary key
    F.col("account_id"),
    
    # Core business attributes
    F.col("account_name"),
    F.col("account_type"),
    F.col("industry"),
    
    # Geographic attributes
    F.col("country"),
    F.col("hq_country"),
    
    # Size metrics
    F.col("employee_count"),
    F.col("company_size"),
    
    # Financial metrics
    F.col("annual_revenue_abs"),
    F.col("revenue_tier"),
    
    # Timestamp for tracking
    F.col("processed_timestamp")
)

print(f"📊 Gold accounts records: {df_gold_accounts.count()}")
df_gold_accounts.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold Table

# COMMAND ----------

gold_table = f"{catalog}.{schema}.gold_accounts"

# Write with MERGE for idempotency
df_gold_accounts.createOrReplaceTempView("gold_accounts_staging")

spark.sql(f"""
MERGE INTO {gold_table} AS target
USING gold_accounts_staging AS source
ON target.account_id = source.account_id
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *
""")

print(f"✅ Gold accounts table updated: {gold_table}")

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
    'table_type' = 'dimension'
)
""")

spark.sql(f"""
COMMENT ON TABLE {gold_table} IS 
'Gold layer - Curated accounts dimension table for business analytics. Contains clean, business-ready account attributes with size and revenue classifications.'
""")

# Add column comments
column_comments = {
    "account_id": "Primary Key - Unique account identifier",
    "account_name": "Business name of the account",
    "account_type": "Account segment (Enterprise, Mid-Market, SMB, Startup)",
    "industry": "Industry classification",
    "country": "Primary operating country",
    "hq_country": "Headquarters country",
    "employee_count": "Total employee headcount",
    "company_size": "Size classification (Enterprise, Large, Medium, Small, Micro)",
    "annual_revenue_abs": "Annual revenue in USD",
    "revenue_tier": "Revenue classification tier",
    "processed_timestamp": "Last processing timestamp"
}

for col_name, comment in column_comments.items():
    try:
        spark.sql(f"ALTER TABLE {gold_table} ALTER COLUMN {col_name} COMMENT '{comment}'")
    except Exception as e:
        print(f"Warning: Could not add comment for {col_name}: {e}")

print(f"✅ Table metadata updated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

# Final validation
gold_count = spark.table(gold_table).count()

print(f"📊 Gold Accounts Summary:")
print(f"   Total Records: {gold_count}")

# Distribution by company size
print(f"\n📈 Distribution by Company Size:")
spark.sql(f"""
    SELECT company_size, COUNT(*) as count, 
           ROUND(AVG(annual_revenue_abs), 2) as avg_revenue
    FROM {gold_table}
    GROUP BY company_size
    ORDER BY avg_revenue DESC
""").display()
