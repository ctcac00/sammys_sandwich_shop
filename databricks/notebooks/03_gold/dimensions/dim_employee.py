# Databricks notebook source
# MAGIC %md
# MAGIC # Dimension: Employee
# MAGIC SCD Type 1 implementation (current state only).
# MAGIC 
# MAGIC Mirrors: `dbt/models/marts/dimensions/dim_employee.sql`

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, when, current_timestamp, current_date, floor, md5
)
from pyspark.sql.types import StringType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Enriched Data

# COMMAND ----------

df = spark.table(silver_table("int_employees"))
print(f"Loaded {df.count()} employees")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Employee Dimension

# COMMAND ----------

employee_dimension = df.select(
    col("employee_id"),
    col("first_name"),
    col("last_name"),
    col("full_name"),
    col("job_title"),
    col("department"),
    col("hourly_rate"),
    
    # Rate band
    when(col("hourly_rate") < 12, "Entry Level")
    .when(col("hourly_rate") < 15, "Standard")
    .when(col("hourly_rate") < 20, "Senior")
    .otherwise("Management").alias("rate_band"),
    
    col("location_id"),
    col("is_manager"),
    col("hire_date"),
    
    # Tenure in months
    floor(col("tenure_days") / 30.0).alias("tenure_months"),
    
    col("employment_status"),
    
    # SCD Type 1 fields
    current_date().alias("effective_date"),
    lit(True).alias("is_current")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Unknown Employee Record

# COMMAND ----------

from pyspark.sql.functions import to_date as spark_to_date

unknown_employee = spark.createDataFrame([{
    "employee_id": UNKNOWN_EMPLOYEE_ID,
    "first_name": "Unknown",
    "last_name": "Employee",
    "full_name": "Unknown Employee",
    "job_title": "Unknown",
    "department": "Unknown",
    "hourly_rate": 0.0,
    "rate_band": "Unknown",
    "location_id": None,
    "is_manager": False,
    "hire_date": None,
    "tenure_months": 0,
    "employment_status": "Unknown",
    "effective_date": "1900-01-01",
    "is_current": True
}])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combine and Add Surrogate Key

# COMMAND ----------

final = employee_dimension.union(
    unknown_employee.select(
        col("employee_id"),
        col("first_name"),
        col("last_name"),
        col("full_name"),
        col("job_title"),
        col("department"),
        col("hourly_rate"),
        col("rate_band"),
        col("location_id"),
        col("is_manager"),
        spark_to_date(col("hire_date")).alias("hire_date"),
        col("tenure_months").cast("long"),
        col("employment_status"),
        spark_to_date(col("effective_date")).alias("effective_date"),
        col("is_current")
    )
)

# Add surrogate key and metadata
dim_employee = final.select(
    md5(col("employee_id").cast(StringType())).alias("employee_sk"),
    "*",
    current_timestamp().alias("_created_at"),
    current_timestamp().alias("_updated_at")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold Table

# COMMAND ----------

table_name = gold_table("dim_employee")
dim_employee.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✓ Written {dim_employee.count()} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Data

# COMMAND ----------

display(spark.table(table_name).limit(10))
