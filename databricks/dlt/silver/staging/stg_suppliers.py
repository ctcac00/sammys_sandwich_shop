# Databricks notebook source
# MAGIC %md
# MAGIC # Staging: Suppliers
# MAGIC Performs data type casting and basic cleaning on raw supplier data.
# MAGIC Includes data quality expectations for validation.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, trim, lower, upper, lit, when
)
from pyspark.sql.types import IntegerType

# COMMAND ----------

@dlt.table(
    name="stg_suppliers",
    comment="Cleaned and typed supplier data",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_supplier_id", "supplier_id IS NOT NULL")
def stg_suppliers():
    return (
        dlt.read("bronze_suppliers")
        .select(
            col("supplier_id"),
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
            when(upper(trim(col("is_active"))).isin("TRUE", "YES", "1"), lit(True))
                .otherwise(lit(False)).alias("is_active"),
            col("_loaded_at"),
            col("_source_file")
        )
    )
