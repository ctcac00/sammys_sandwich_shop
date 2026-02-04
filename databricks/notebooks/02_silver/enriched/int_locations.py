# Databricks notebook source
# MAGIC %md
# MAGIC # Intermediate: Locations
# MAGIC Adds derived fields and business logic enrichments.
# MAGIC 
# MAGIC Mirrors: `dbt/models/intermediate/int_locations.sql`

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

from pyspark.sql.functions import (
    col, datediff, current_date, current_timestamp, round as spark_round
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Staged Data

# COMMAND ----------

df_locations = spark.table(silver_table("stg_locations"))
df_employees = spark.table(silver_table("int_employees"))
print(f"Loaded {df_locations.count()} locations and {df_employees.count()} employees")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Enrichments

# COMMAND ----------

enriched = df_locations.alias("l").join(
    df_employees.select(
        col("employee_id"),
        col("full_name").alias("manager_name")
    ).alias("e"),
    col("l.manager_employee_id") == col("e.employee_id"),
    "left"
).select(
    # Original fields
    col("l.location_id"),
    col("l.location_name"),
    col("l.address"),
    col("l.city"),
    col("l.state"),
    col("l.zip_code"),
    col("l.phone"),
    col("l.manager_employee_id"),
    col("l.open_date"),
    col("l.seating_capacity"),
    col("l.has_drive_thru"),
    
    # Derived fields
    spark_round(datediff(current_date(), col("l.open_date")) / 365.25, 2).alias("years_in_operation"),
    
    # Manager name from employees
    col("e.manager_name"),
    
    # Metadata
    current_timestamp().alias("_enriched_at")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Table

# COMMAND ----------

table_name = silver_table("int_locations")
enriched.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✓ Written {enriched.count()} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Data

# COMMAND ----------

display(spark.table(table_name).limit(10))
