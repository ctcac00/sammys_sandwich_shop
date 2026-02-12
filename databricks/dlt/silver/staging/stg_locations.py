# Databricks notebook source
# MAGIC %md
# MAGIC # Staging: Locations
# MAGIC Performs data type casting and basic cleaning on raw location data.
# MAGIC Includes data quality expectations for validation.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, trim, upper, to_date, lit, when
)
from pyspark.sql.types import IntegerType

# COMMAND ----------

@dlt.table(
    name="stg_locations",
    comment="Cleaned and typed location data",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_location_id", "location_id IS NOT NULL")
def stg_locations():
    return (
        dlt.read("bronze_locations")
        .select(
            col("location_id"),
            trim(col("location_name")).alias("location_name"),
            trim(col("address")).alias("address"),
            trim(col("city")).alias("city"),
            upper(trim(col("state"))).alias("state"),
            trim(col("zip_code")).alias("zip_code"),
            trim(col("region")).alias("region"),
            trim(col("district")).alias("district"),
            to_date(col("open_date"), "yyyy-MM-dd").alias("open_date"),
            trim(col("manager_id")).alias("manager_id"),
            col("seating_capacity").cast(IntegerType()).alias("seating_capacity"),
            when(upper(trim(col("has_drive_thru"))).isin("TRUE", "YES", "1"), lit(True))
                .otherwise(lit(False)).alias("has_drive_thru"),
            when(upper(trim(col("is_active"))).isin("TRUE", "YES", "1"), lit(True))
                .otherwise(lit(False)).alias("is_active"),
            col("_loaded_at"),
            col("_source_file")
        )
    )
