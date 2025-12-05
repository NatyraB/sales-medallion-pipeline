# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 Silver Layer - Account Data Transformation
# MAGIC 
# MAGIC This notebook transforms Bronze account data into the Silver layer with:
# MAGIC - Column standardization (snake_case naming)
# MAGIC - Data quality validation
# MAGIC - Derived columns (company_size, revenue_tier)
# MAGIC - Null handling and data cleansing

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

# Get parameters from job or use defaults
try:
    catalog = dbutils.widgets.get("catalog")
    schema = dbutils.widgets.get("schema")
except:
    catalog = "natyra_demo"
    schema = "sales"

print(f"📊 Silver Layer Configuration:")
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
# MAGIC ## Read from Bronze Layer

# COMMAND ----------

bronze_table = f"{catalog}.{schema}.bronze_accounts"
df_bronze = spark.table(bronze_table)

print(f"📥 Bronze records to process: {df_bronze.count()}")
df_bronze.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks & Filtering

# COMMAND ----------

# Define data quality rules
df_valid = df_bronze.filter(
    # Must have AccountId
    F.col("AccountId").isNotNull() &
    (F.length(F.col("AccountId")) > 0) &
    # Must have AccountName
    F.col("AccountName").isNotNull() &
    (F.length(F.col("AccountName")) > 0) &
    # Employee count must be positive or null
    ((F.col("NumberOfEmployees").isNull()) | (F.col("NumberOfEmployees") >= 0)) &
    # Revenue must be positive or null
    ((F.col("AnnualRevenue").isNull()) | (F.col("AnnualRevenue") >= 0))
)

# Quarantine invalid records for review
df_quarantine = df_bronze.subtract(df_valid)
quarantine_count = df_quarantine.count()

if quarantine_count > 0:
    print(f"⚠️ Quarantined {quarantine_count} records due to data quality issues")
    df_quarantine.display()

print(f"✅ Valid records for Silver: {df_valid.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Column Standardization & Transformation

# COMMAND ----------

# Transform to Silver layer format - matches existing table schema
df_silver = df_valid.select(
    # Standardize column names to snake_case
    F.col("AccountId").alias("account_id"),
    F.trim(F.col("AccountName")).alias("account_name"),
    
    # Clean and standardize industry
    F.coalesce(F.trim(F.col("AccountIndustry")), F.lit("Unknown")).alias("industry"),
    
    # Clean account type
    F.coalesce(F.trim(F.col("AccountType")), F.lit("Unknown")).alias("account_type"),
    
    # Standardize country fields
    F.coalesce(F.upper(F.trim(F.col("AccountHQCountry"))), F.lit("UNKNOWN")).alias("hq_country"),
    F.coalesce(F.upper(F.trim(F.col("AccountCountry"))), F.lit("UNKNOWN")).alias("country"),
    
    # Clean numeric fields
    F.coalesce(F.col("NumberOfEmployees"), F.lit(0)).alias("employee_count"),
    
    # Keep original revenue and add absolute value
    F.coalesce(F.col("AnnualRevenue"), F.lit(0.0)).alias("annual_revenue"),
    
    # Keep original metadata
    F.col("ingestion_timestamp"),
    F.col("source_system"),
    
    # Add absolute revenue value
    F.abs(F.coalesce(F.col("AnnualRevenue"), F.lit(0.0))).alias("annual_revenue_abs")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Derived Columns

# COMMAND ----------

# Add business-derived columns
df_silver = df_silver.withColumn(
    # Company size classification based on employees
    "company_size",
    F.when(F.col("employee_count") >= 10000, "Enterprise")
     .when(F.col("employee_count") >= 1000, "Large")
     .when(F.col("employee_count") >= 100, "Medium")
     .when(F.col("employee_count") >= 10, "Small")
     .otherwise("Micro")
).withColumn(
    # Revenue tier classification
    "revenue_tier",
    F.when(F.col("annual_revenue_abs") >= 100000000, "Tier 1 - $100M+")
     .when(F.col("annual_revenue_abs") >= 10000000, "Tier 2 - $10M-$100M")
     .when(F.col("annual_revenue_abs") >= 1000000, "Tier 3 - $1M-$10M")
     .otherwise("Tier 4 - Under $1M")
).withColumn(
    # Processing timestamp
    "processed_timestamp", F.current_timestamp()
)

print(f"📊 Silver records with derived columns: {df_silver.count()}")
df_silver.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Table

# COMMAND ----------

silver_table = f"{catalog}.{schema}.silver_accounts"

# Write with MERGE for idempotency
df_silver.createOrReplaceTempView("silver_staging")

spark.sql(f"""
MERGE INTO {silver_table} AS target
USING silver_staging AS source
ON target.account_id = source.account_id
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *
""")

print(f"✅ Silver table updated: {silver_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Table and Column Metadata

# COMMAND ----------

# Add table properties
spark.sql(f"""
ALTER TABLE {silver_table}
SET TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'quality_tier' = 'silver',
    'data_domain' = 'sales',
    'pipeline_name' = 'medallion-accounts'
)
""")

spark.sql(f"""
COMMENT ON TABLE {silver_table} IS 
'Silver layer - Cleansed and standardized account data. Includes data quality filtering, column standardization, and derived business columns (company_size, revenue_tier).'
""")

# Add column comments
column_comments = {
    "account_id": "Unique identifier for the account (from source)",
    "account_name": "Cleaned business name of the account",
    "industry": "Standardized industry classification",
    "account_type": "Account segment (Enterprise, Mid-Market, SMB, Startup)",
    "hq_country": "Country code where headquarters is located (uppercase)",
    "country": "Primary operating country code (uppercase)",
    "employee_count": "Total employee count (validated, non-negative)",
    "annual_revenue": "Original annual revenue in USD",
    "annual_revenue_abs": "Annual revenue in USD (absolute value, non-negative)",
    "company_size": "Derived: Employee-based size classification",
    "revenue_tier": "Derived: Revenue-based tier classification",
    "ingestion_timestamp": "Original Bronze ingestion timestamp",
    "source_system": "Source system identifier",
    "processed_timestamp": "Timestamp when record was processed into Silver"
}

for col_name, comment in column_comments.items():
    try:
        spark.sql(f"ALTER TABLE {silver_table} ALTER COLUMN {col_name} COMMENT '{comment}'")
    except Exception as e:
        print(f"Warning: Could not add comment for {col_name}: {e}")

print(f"✅ Table metadata updated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Summary

# COMMAND ----------

# Final validation
silver_count = spark.table(silver_table).count()
bronze_count = spark.table(bronze_table).count()

print(f"📊 Silver Layer Summary:")
print(f"   Bronze Input Records: {bronze_count}")
print(f"   Silver Output Records: {silver_count}")
print(f"   Quarantined Records: {quarantine_count}")
print(f"   Processing Rate: {(silver_count/bronze_count)*100:.1f}%")

# Show sample
spark.sql(f"SELECT * FROM {silver_table} LIMIT 5").display()
