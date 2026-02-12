# Databricks notebook source
# MAGIC %md
# MAGIC # Intermediate: Employees
# MAGIC Adds derived fields and business logic enrichments to employee data.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, concat_ws, datediff, current_date, current_timestamp, floor
)

# COMMAND ----------

@dlt.table(
    name="int_employees",
    comment="Employee data with derived fields",
    table_properties={"quality": "silver"}
)
def int_employees():
    return (
        dlt.read("stg_employees")
        .select(
            col("employee_id"),
            col("first_name"),
            col("last_name"),
            concat_ws(" ", col("first_name"), col("last_name")).alias("full_name"),
            col("email"),
            col("phone"),
            col("hire_date"),
            col("termination_date"),
            col("job_title"),
            col("department"),
            col("location_id"),
            col("hourly_rate"),
            col("is_active"),
            # Tenure calculation
            datediff(current_date(), col("hire_date")).alias("tenure_days"),
            floor(datediff(current_date(), col("hire_date")) / 30.0).alias("tenure_months"),
            floor(datediff(current_date(), col("hire_date")) / 365.25).alias("tenure_years"),
            current_timestamp().alias("_enriched_at")
        )
    )
