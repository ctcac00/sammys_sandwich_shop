# Databricks notebook source
# MAGIC %md
# MAGIC # Dimension: Location
# MAGIC Location dimension with geography and manager info (SCD Type 1).

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
    name="dim_location",
    comment="Location dimension with geography and manager info",
    table_properties={"quality": "gold"}
)
def dim_location():
    locations = dlt.read("int_locations")
    
    location_dim = (
        locations
        .select(
            md5(col("location_id").cast(StringType())).alias("location_sk"),
            col("location_id"),
            col("location_name"),
            col("address"),
            col("city"),
            col("state"),
            col("zip_code"),
            col("city_state"),
            col("region"),
            col("district"),
            col("open_date"),
            col("days_open"),
            col("manager_id"),
            col("manager_name"),
            col("seating_capacity"),
            col("has_drive_thru"),
            col("is_active"),
            # Location age grouping
            when(col("days_open") < 365, "New (< 1 yr)")
            .when(col("days_open") < 1095, "Established (1-3 yr)")
            .otherwise("Mature (3+ yr)").alias("location_age_group"),
            current_date().alias("effective_date"),
            lit(True).alias("is_current"),
            current_timestamp().alias("_created_at"),
            current_timestamp().alias("_updated_at")
        )
    )
    
    # Add unknown location
    unknown_location = spark.sql(f"""
        SELECT 
            md5('{UNKNOWN_LOCATION_ID}') as location_sk,
            '{UNKNOWN_LOCATION_ID}' as location_id,
            'Unknown Location' as location_name,
            null as address,
            null as city,
            null as state,
            null as zip_code,
            null as city_state,
            'Unknown' as region,
            'Unknown' as district,
            null as open_date,
            0 as days_open,
            null as manager_id,
            'Unknown' as manager_name,
            0 as seating_capacity,
            false as has_drive_thru,
            false as is_active,
            'Unknown' as location_age_group,
            to_date('1900-01-01') as effective_date,
            true as is_current,
            current_timestamp() as _created_at,
            current_timestamp() as _updated_at
    """)
    
    return location_dim.unionByName(unknown_location, allowMissingColumns=True)
