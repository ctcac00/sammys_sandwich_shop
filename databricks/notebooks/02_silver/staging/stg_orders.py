# Databricks notebook source
# MAGIC %md
# MAGIC # Staging: Orders
# MAGIC Performs data type casting and basic cleaning on order data.
# MAGIC 
# MAGIC Mirrors: `dbt/models/staging/stg_orders.sql`

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

from pyspark.sql.functions import col, trim, to_date, to_timestamp, coalesce, lit
from pyspark.sql.types import DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Bronze Data

# COMMAND ----------

df = spark.table(bronze_table("orders"))
print(f"Loaded {df.count()} rows from bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Transformations

# COMMAND ----------

staged = df.select(
    # Primary key
    col("order_id"),
    
    # Foreign keys
    trim(col("customer_id")).alias("customer_id"),
    trim(col("employee_id")).alias("employee_id"),
    trim(col("location_id")).alias("location_id"),
    
    # Date/time
    to_date(col("order_date"), "yyyy-MM-dd").alias("order_date"),
    col("order_time").alias("order_time"),  # Keep as string, will be converted in intermediate
    
    # Order details
    trim(col("order_type")).alias("order_type"),
    trim(col("order_status")).alias("order_status"),
    trim(col("payment_method")).alias("payment_method"),
    
    # Amounts
    coalesce(col("subtotal").cast(DoubleType()), lit(0.0)).alias("subtotal"),
    coalesce(col("tax_amount").cast(DoubleType()), lit(0.0)).alias("tax_amount"),
    coalesce(col("discount_amount").cast(DoubleType()), lit(0.0)).alias("discount_amount"),
    coalesce(col("tip_amount").cast(DoubleType()), lit(0.0)).alias("tip_amount"),
    coalesce(col("total_amount").cast(DoubleType()), lit(0.0)).alias("total_amount"),
    
    # Metadata
    col("_loaded_at"),
    col("_source_file")
).filter(col("order_id").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Table

# COMMAND ----------

table_name = silver_table("stg_orders")
staged.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✓ Written {staged.count()} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Data

# COMMAND ----------

display(spark.table(table_name).limit(10))
