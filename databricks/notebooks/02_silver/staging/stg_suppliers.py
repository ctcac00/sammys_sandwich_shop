# Databricks notebook source
# MAGIC %md
# MAGIC # Staging: Suppliers
# MAGIC Performs data type casting and basic cleaning on supplier data.
# MAGIC 
# MAGIC Mirrors: `dbt/models/staging/stg_suppliers.sql`

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

from pyspark.sql.functions import col, trim, lower, upper, when, lit
from pyspark.sql.types import IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Bronze Data

# COMMAND ----------

df = spark.table(bronze_table("suppliers"))
print(f"Loaded {df.count()} rows from bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Transformations

# COMMAND ----------

staged = df.select(
    # Primary key
    col("supplier_id"),
    
    # Attributes
    trim(col("supplier_name")).alias("supplier_name"),
    trim(col("contact_name")).alias("contact_name"),
    lower(trim(col("email"))).alias("email"),
    trim(col("phone")).alias("phone"),
    trim(col("address")).alias("address"),
    trim(col("city")).alias("city"),
    upper(trim(col("state"))).alias("state"),
    trim(col("zip_code")).alias("zip_code"),
    trim(col("payment_terms")).alias("payment_terms"),
    col("lead_time_days").cast(IntegerType()).alias("lead_time_days"),
    
    # Flags
    when(upper(trim(col("is_active"))).isin("TRUE", "YES", "1"), lit(True))
        .otherwise(lit(False)).alias("is_active"),
    
    # Metadata
    col("_loaded_at"),
    col("_source_file")
).filter(col("supplier_id").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Table

# COMMAND ----------

table_name = silver_table("stg_suppliers")
staged.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✓ Written {staged.count()} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Data

# COMMAND ----------

display(spark.table(table_name).limit(10))
