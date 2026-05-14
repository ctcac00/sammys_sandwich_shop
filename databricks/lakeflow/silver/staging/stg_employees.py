# Databricks notebook source
# MAGIC %md
# MAGIC # Staging: Employees
# MAGIC Performs data type casting and basic cleaning on raw employee data.
# MAGIC Includes data quality expectations for validation.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, trim, lower, upper, initcap, to_date, coalesce, lit, when
)
from pyspark.sql.types import DoubleType

# COMMAND ----------

@dp.materialized_view(
    name="stg_employees",
    comment="Cleaned and typed employee data",
    table_properties={"quality": "silver"}
)
@dp.expect_or_drop("valid_employee_id", "employee_id IS NOT NULL")
@dp.expect("valid_hire_date", "hire_date IS NOT NULL")
def stg_employees():
    return (
        spark.read.table("bronze_employees")
        .select(
            col("employee_id"),
            initcap(trim(col("first_name"))).alias("first_name"),
            initcap(trim(col("last_name"))).alias("last_name"),
            lower(trim(col("email"))).alias("email"),
            trim(col("phone")).alias("phone"),
            to_date(col("hire_date"), "yyyy-MM-dd").alias("hire_date"),
            to_date(col("termination_date"), "yyyy-MM-dd").alias("termination_date"),
            trim(col("job_title")).alias("job_title"),
            trim(col("department")).alias("department"),
            trim(col("location_id")).alias("location_id"),
            coalesce(col("hourly_rate").cast(DoubleType()), lit(0.0)).alias("hourly_rate"),
            when(upper(trim(col("is_active"))).isin("TRUE", "YES", "1"), lit(True))
                .otherwise(lit(False)).alias("is_active"),
            col("_loaded_at"),
            col("_source_file")
        )
    )
