# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 Bronze Layer - Account Data Ingestion
# MAGIC 
# MAGIC This notebook ingests raw account data into the Bronze layer with:
# MAGIC - Ingestion metadata (timestamp, source system)
# MAGIC - Data quality rescue column
# MAGIC - Table comments and column descriptions

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

print(f"📊 Bronze Layer Configuration:")
print(f"   Catalog: {catalog}")
print(f"   Schema: {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Catalog and Schema

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Sample Raw Account Data
# MAGIC 
# MAGIC In a real scenario, this would read from a landing zone, external source, or streaming input.

# COMMAND ----------

# Sample raw account data (simulating source system data)
raw_data = [
    ("ACC001", "Acme Corporation", "Technology", "Enterprise", "USA", "USA", 5000, 50000000.0),
    ("ACC002", "GlobalTech Inc", "Technology", "Enterprise", "UK", "UK", 3000, 35000000.0),
    ("ACC003", "DataFlow Systems", "Software", "Mid-Market", "Germany", "Germany", 500, 8000000.0),
    ("ACC004", "CloudFirst Ltd", "Cloud Services", "SMB", "USA", "Canada", 150, 2500000.0),
    ("ACC005", "Analytics Pro", "Analytics", "Enterprise", "Japan", "Japan", 2000, 25000000.0),
    ("ACC006", "TechStart GmbH", "Technology", "Startup", "Germany", "Germany", 25, 500000.0),
    ("ACC007", "MegaCorp International", "Conglomerate", "Enterprise", "USA", "USA", 50000, 500000000.0),
    ("ACC008", "LocalBiz Solutions", "Consulting", "SMB", "France", "France", 75, 1200000.0),
    ("ACC009", "InnovateTech", "Technology", "Mid-Market", "UK", "UK", 800, 12000000.0),
    ("ACC010", "DataDriven Inc", "Analytics", "Enterprise", "USA", "USA", 1500, 18000000.0),
    # Some records with data quality issues for testing
    ("ACC011", None, "Unknown", "Unknown", "Unknown", None, -100, -5000000.0),
    ("ACC012", "Bad Data Corp", None, "Enterprise", "", "", None, None),
]

# Define schema for raw data
raw_schema = StructType([
    StructField("AccountId", StringType(), True),
    StructField("AccountName", StringType(), True),
    StructField("AccountIndustry", StringType(), True),
    StructField("AccountType", StringType(), True),
    StructField("AccountHQCountry", StringType(), True),
    StructField("AccountCountry", StringType(), True),
    StructField("NumberOfEmployees", IntegerType(), True),
    StructField("AnnualRevenue", DoubleType(), True),
])

# Create DataFrame
df_raw = spark.createDataFrame(raw_data, schema=raw_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Ingestion Metadata

# COMMAND ----------

# Add metadata columns
df_bronze = df_raw.withColumn(
    "ingestion_timestamp", F.current_timestamp()
).withColumn(
    "source_system", F.lit("source_crm")
).withColumn(
    "_rescued_data", F.lit(None).cast(StringType())  # For schema rescue in streaming
)

print(f"📥 Records to ingest: {df_bronze.count()}")
df_bronze.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Bronze Table with Metadata

# COMMAND ----------

# Table name
bronze_table = f"{catalog}.{schema}.bronze_accounts"

# Write to Delta table with MERGE for idempotency
df_bronze.createOrReplaceTempView("bronze_staging")

spark.sql(f"""
MERGE INTO {bronze_table} AS target
USING bronze_staging AS source
ON target.AccountId = source.AccountId
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *
""")

print(f"✅ Bronze table updated: {bronze_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Table and Column Comments

# COMMAND ----------

# Add table comment
spark.sql(f"""
ALTER TABLE {bronze_table}
SET TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'quality_tier' = 'bronze',
    'data_domain' = 'sales',
    'pipeline_name' = 'medallion-accounts'
)
""")

spark.sql(f"""
COMMENT ON TABLE {bronze_table} IS 
'Bronze layer - Raw account data ingested from source CRM system. Contains original field names and values with ingestion metadata.'
""")

# Add column comments
column_comments = {
    "AccountId": "Unique identifier for the account from source system",
    "AccountName": "Business name of the account",
    "AccountIndustry": "Industry classification from source",
    "AccountType": "Account segment (Enterprise, Mid-Market, SMB, Startup)",
    "AccountHQCountry": "Country where account headquarters is located",
    "AccountCountry": "Primary operating country",
    "NumberOfEmployees": "Total employee count",
    "AnnualRevenue": "Annual revenue in USD",
    "_rescued_data": "Column for rescued data during schema evolution",
    "ingestion_timestamp": "Timestamp when record was ingested into Bronze",
    "source_system": "Source system identifier"
}

for col_name, comment in column_comments.items():
    try:
        spark.sql(f"ALTER TABLE {bronze_table} ALTER COLUMN {col_name} COMMENT '{comment}'")
    except Exception as e:
        print(f"Warning: Could not add comment for {col_name}: {e}")

print(f"✅ Table metadata updated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

# Verify data
row_count = spark.table(bronze_table).count()
print(f"📊 Bronze Table Statistics:")
print(f"   Table: {bronze_table}")
print(f"   Row Count: {row_count}")

# Show sample
spark.sql(f"SELECT * FROM {bronze_table} LIMIT 5").display()
