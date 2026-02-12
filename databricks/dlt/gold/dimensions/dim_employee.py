# Databricks notebook source
# MAGIC %md
# MAGIC # Dimension: Employee
# MAGIC Employee dimension with tenure and role info (SCD Type 1).

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, lit, when, current_timestamp, current_date, md5
)
from pyspark.sql.types import StringType

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

@dlt.table(
    name="dim_employee",
    comment="Employee dimension with tenure and role info",
    table_properties={"quality": "gold"}
)
def dim_employee():
    employees = dlt.read("int_employees")
    
    employee_dim = (
        employees
        .select(
            md5(col("employee_id").cast(StringType())).alias("employee_sk"),
            col("employee_id"),
            col("first_name"),
            col("last_name"),
            col("full_name"),
            col("email"),
            col("job_title"),
            col("department"),
            col("location_id"),
            col("hire_date"),
            col("termination_date"),
            col("hourly_rate"),
            col("is_active"),
            col("tenure_days"),
            col("tenure_months"),
            col("tenure_years"),
            # Tenure grouping
            when(col("tenure_months") < 3, "New (0-3 mo)")
            .when(col("tenure_months") < 12, "Growing (3-12 mo)")
            .when(col("tenure_years") < 3, "Experienced (1-3 yr)")
            .otherwise("Veteran (3+ yr)").alias("tenure_group"),
            current_date().alias("effective_date"),
            lit(True).alias("is_current"),
            current_timestamp().alias("_created_at"),
            current_timestamp().alias("_updated_at")
        )
    )
    
    # Add unknown employee
    unknown_employee = spark.sql(f"""
        SELECT 
            md5('{UNKNOWN_EMPLOYEE_ID}') as employee_sk,
            '{UNKNOWN_EMPLOYEE_ID}' as employee_id,
            'Unknown' as first_name,
            'Employee' as last_name,
            'Unknown Employee' as full_name,
            null as email,
            'Unknown' as job_title,
            'Unknown' as department,
            null as location_id,
            null as hire_date,
            null as termination_date,
            0.0 as hourly_rate,
            false as is_active,
            0 as tenure_days,
            0 as tenure_months,
            0 as tenure_years,
            'Unknown' as tenure_group,
            to_date('1900-01-01') as effective_date,
            true as is_current,
            current_timestamp() as _created_at,
            current_timestamp() as _updated_at
    """)
    
    return employee_dim.unionByName(unknown_employee, allowMissingColumns=True)
