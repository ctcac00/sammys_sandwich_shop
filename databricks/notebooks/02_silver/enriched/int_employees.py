# Databricks notebook source
# MAGIC %md
# MAGIC # Intermediate: Employees
# MAGIC Adds derived fields and business logic enrichments.
# MAGIC 
# MAGIC Mirrors: `dbt/models/intermediate/int_employees.sql`

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

from pyspark.sql.functions import (
    col, concat_ws, datediff, current_date, current_timestamp, when
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Staged Data

# COMMAND ----------

df = spark.table(silver_table("stg_employees"))
print(f"Loaded {df.count()} rows from staging")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Enrichments

# COMMAND ----------

enriched = df.select(
    # Original fields
    col("employee_id"),
    col("first_name"),
    col("last_name"),
    concat_ws(" ", col("first_name"), col("last_name")).alias("full_name"),
    col("email"),
    col("phone"),
    col("hire_date"),
    col("job_title"),
    col("department"),
    col("hourly_rate"),
    col("location_id"),
    col("manager_id"),
    col("employment_status"),
    
    # Derived fields - tenure calculation
    datediff(current_date(), col("hire_date")).alias("tenure_days"),
    
    # Is manager flag (employees who are managers of locations)
    col("manager_id").isNull().alias("is_manager"),
    
    # Metadata
    current_timestamp().alias("_enriched_at")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Table

# COMMAND ----------

table_name = silver_table("int_employees")
enriched.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✓ Written {enriched.count()} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Data

# COMMAND ----------

display(spark.table(table_name).limit(10))
