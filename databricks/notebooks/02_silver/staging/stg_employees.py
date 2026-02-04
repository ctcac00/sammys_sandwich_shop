# Databricks notebook source
# MAGIC %md
# MAGIC # Staging: Employees
# MAGIC Performs data type casting and basic cleaning on employee data.
# MAGIC 
# MAGIC Mirrors: `dbt/models/staging/stg_employees.sql`

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

from pyspark.sql.functions import col, trim, lower, initcap, to_date
from pyspark.sql.types import DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Bronze Data

# COMMAND ----------

df = spark.table(bronze_table("employees"))
print(f"Loaded {df.count()} rows from bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Transformations

# COMMAND ----------

staged = df.select(
    # Primary key
    col("employee_id"),
    
    # Name fields
    initcap(trim(col("first_name"))).alias("first_name"),
    initcap(trim(col("last_name"))).alias("last_name"),
    
    # Contact info
    lower(trim(col("email"))).alias("email"),
    trim(col("phone")).alias("phone"),
    
    # Employment info
    to_date(col("hire_date"), "yyyy-MM-dd").alias("hire_date"),
    trim(col("job_title")).alias("job_title"),
    trim(col("department")).alias("department"),
    col("hourly_rate").cast(DoubleType()).alias("hourly_rate"),
    trim(col("location_id")).alias("location_id"),
    trim(col("manager_id")).alias("manager_id"),
    trim(col("employment_status")).alias("employment_status"),
    
    # Metadata
    col("_loaded_at"),
    col("_source_file")
).filter(col("employee_id").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Table

# COMMAND ----------

table_name = silver_table("stg_employees")
staged.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✓ Written {staged.count()} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Data

# COMMAND ----------

display(spark.table(table_name).limit(10))
