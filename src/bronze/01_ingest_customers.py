# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *

try:
    catalog = dbutils.widgets.get("catalog")
    schema = dbutils.widgets.get("schema")
except:
    catalog = "natyra_demo"
    schema = "sales_bronze"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

raw_data = [
    (1, "John", "Doe", "john.doe@email.com", "123-456-7890", "New York", "NY", "active"),
    (2, "Jane", "Smith", "jane.smith@email.com", "234-567-8901", "Los Angeles", "CA", "active"),
    (3, "Bob", "Johnson", "bob.j@email.com", "345-678-9012", "Chicago", "IL", "active"),
    (4, "Alice", "Williams", "alice.w@email.com", "456-789-0123", "Houston", "TX", "inactive"),
    (5, "Charlie", "Brown", "charlie.b@email.com", "567-890-1234", "Phoenix", "AZ", "active"),
]

schema_def = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("status", StringType(), True),
])

df = spark.createDataFrame(raw_data, schema_def).withColumn("_ingested_at", F.current_timestamp()).withColumn("_source_system", F.lit("CRM"))
df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.raw_customers")
print(f"raw_customers: {df.count()} records")
