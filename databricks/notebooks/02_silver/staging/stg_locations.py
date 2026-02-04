# Databricks notebook source
# MAGIC %md
# MAGIC # Staging: Locations
# MAGIC Performs data type casting and basic cleaning on location data.
# MAGIC 
# MAGIC Mirrors: `dbt/models/staging/stg_locations.sql`

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

from pyspark.sql.functions import col, trim, upper, to_date, when, lit
from pyspark.sql.types import IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Bronze Data

# COMMAND ----------

df = spark.table(bronze_table("locations"))
print(f"Loaded {df.count()} rows from bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Transformations

# COMMAND ----------

staged = df.select(
    # Primary key
    col("location_id"),
    
    # Attributes
    trim(col("location_name")).alias("location_name"),
    trim(col("address")).alias("address"),
    trim(col("city")).alias("city"),
    upper(trim(col("state"))).alias("state"),
    trim(col("zip_code")).alias("zip_code"),
    trim(col("phone")).alias("phone"),
    trim(col("manager_employee_id")).alias("manager_employee_id"),
    
    # Type conversions
    to_date(col("open_date"), "yyyy-MM-dd").alias("open_date"),
    col("seating_capacity").cast(IntegerType()).alias("seating_capacity"),
    when(upper(trim(col("has_drive_thru"))).isin("TRUE", "YES", "1"), lit(True))
        .otherwise(lit(False)).alias("has_drive_thru"),
    
    # Metadata
    col("_loaded_at"),
    col("_source_file")
).filter(col("location_id").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Table

# COMMAND ----------

table_name = silver_table("stg_locations")
staged.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✓ Written {staged.count()} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Data

# COMMAND ----------

display(spark.table(table_name).limit(10))
