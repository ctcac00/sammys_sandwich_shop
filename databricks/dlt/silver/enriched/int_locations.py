# Databricks notebook source
# MAGIC %md
# MAGIC # Intermediate: Locations
# MAGIC Adds manager info and derived fields to location data.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, concat_ws, datediff, current_date, current_timestamp, coalesce, lit
)

# COMMAND ----------

@dlt.table(
    name="int_locations",
    comment="Location data with manager info",
    table_properties={"quality": "silver"}
)
def int_locations():
    locations = dlt.read("stg_locations")
    employees = dlt.read("int_employees")
    
    return (
        locations.alias("l")
        .join(
            employees.select(
                col("employee_id"),
                col("full_name").alias("manager_name")
            ).alias("e"),
            col("l.manager_id") == col("e.employee_id"),
            "left"
        )
        .select(
            col("l.location_id"),
            col("l.location_name"),
            col("l.address"),
            col("l.city"),
            col("l.state"),
            col("l.zip_code"),
            concat_ws(", ", col("l.city"), col("l.state")).alias("city_state"),
            col("l.region"),
            col("l.district"),
            col("l.open_date"),
            datediff(current_date(), col("l.open_date")).alias("days_open"),
            col("l.manager_id"),
            coalesce(col("e.manager_name"), lit("Unassigned")).alias("manager_name"),
            col("l.seating_capacity"),
            col("l.has_drive_thru"),
            col("l.is_active"),
            current_timestamp().alias("_enriched_at")
        )
    )
