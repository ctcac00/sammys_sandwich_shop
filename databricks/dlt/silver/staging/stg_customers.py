# Databricks notebook source
# MAGIC %md
# MAGIC # Staging: Customers
# MAGIC Performs data type casting and basic cleaning on raw customer data.
# MAGIC Includes data quality expectations for validation.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, trim, lower, upper, initcap, to_date, coalesce, lit, when
)
from pyspark.sql.types import IntegerType

# COMMAND ----------

@dlt.table(
    name="stg_customers",
    comment="Cleaned and typed customer data",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect("valid_email_format", "email IS NULL OR email LIKE '%@%.%'")
def stg_customers():
    return (
        dlt.read("bronze_customers")
        .select(
            col("customer_id"),
            initcap(trim(col("first_name"))).alias("first_name"),
            initcap(trim(col("last_name"))).alias("last_name"),
            lower(trim(col("email"))).alias("email"),
            trim(col("phone")).alias("phone"),
            trim(col("address")).alias("address"),
            trim(col("city")).alias("city"),
            upper(trim(col("state"))).alias("state"),
            trim(col("zip_code")).alias("zip_code"),
            to_date(col("birth_date"), "yyyy-MM-dd").alias("birth_date"),
            to_date(col("signup_date"), "yyyy-MM-dd").alias("signup_date"),
            coalesce(trim(col("loyalty_tier")), lit("Bronze")).alias("loyalty_tier"),
            coalesce(col("loyalty_points").cast(IntegerType()), lit(0)).alias("loyalty_points"),
            trim(col("preferred_location")).alias("preferred_location_id"),
            when(upper(trim(col("marketing_opt_in"))).isin("TRUE", "YES", "1"), lit(True))
                .otherwise(lit(False)).alias("marketing_opt_in"),
            col("_loaded_at"),
            col("_source_file")
        )
    )
