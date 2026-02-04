# Databricks notebook source
# MAGIC %md
# MAGIC # Dimension: Location
# MAGIC SCD Type 1 implementation (current state only).
# MAGIC 
# MAGIC Mirrors: `dbt/models/marts/dimensions/dim_location.sql`

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, when, current_timestamp, current_date, md5
)
from pyspark.sql.types import StringType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Enriched Data

# COMMAND ----------

df = spark.table(silver_table("int_locations"))
print(f"Loaded {df.count()} locations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Location Dimension

# COMMAND ----------

location_dimension = df.select(
    col("location_id"),
    col("location_name"),
    col("address"),
    col("city"),
    col("state"),
    col("zip_code"),
    
    # Region based on state
    when(col("state").isin("CA", "OR", "WA", "NV", "AZ"), "West")
    .when(col("state").isin("TX", "OK", "NM", "CO", "UT"), "Southwest")
    .when(col("state").isin("IL", "OH", "MI", "IN", "WI", "MN"), "Midwest")
    .when(col("state").isin("NY", "PA", "NJ", "MA", "CT"), "Northeast")
    .when(col("state").isin("FL", "GA", "NC", "SC", "VA", "TN"), "Southeast")
    .otherwise("Other").alias("region"),
    
    col("open_date"),
    col("years_in_operation"),
    col("seating_capacity"),
    
    # Capacity tier
    when(col("seating_capacity") < 30, "Small")
    .when(col("seating_capacity") < 60, "Medium")
    .when(col("seating_capacity") < 100, "Large")
    .otherwise("Extra Large").alias("capacity_tier"),
    
    col("has_drive_thru"),
    col("manager_name"),
    
    # SCD Type 1 fields
    current_date().alias("effective_date"),
    lit(True).alias("is_current")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Unknown Location Record

# COMMAND ----------

from pyspark.sql.functions import to_date as spark_to_date

unknown_location = spark.createDataFrame([{
    "location_id": UNKNOWN_LOCATION_ID,
    "location_name": "Unknown Location",
    "address": None,
    "city": "Unknown",
    "state": "XX",
    "zip_code": None,
    "region": "Unknown",
    "open_date": None,
    "years_in_operation": 0.0,
    "seating_capacity": 0,
    "capacity_tier": "Unknown",
    "has_drive_thru": False,
    "manager_name": None,
    "effective_date": "1900-01-01",
    "is_current": True
}])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combine and Add Surrogate Key

# COMMAND ----------

final = location_dimension.union(
    unknown_location.select(
        col("location_id"),
        col("location_name"),
        col("address"),
        col("city"),
        col("state"),
        col("zip_code"),
        col("region"),
        spark_to_date(col("open_date")).alias("open_date"),
        col("years_in_operation"),
        col("seating_capacity"),
        col("capacity_tier"),
        col("has_drive_thru"),
        col("manager_name"),
        spark_to_date(col("effective_date")).alias("effective_date"),
        col("is_current")
    )
)

# Add surrogate key and metadata
dim_location = final.select(
    md5(col("location_id").cast(StringType())).alias("location_sk"),
    "*",
    current_timestamp().alias("_created_at"),
    current_timestamp().alias("_updated_at")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold Table

# COMMAND ----------

table_name = gold_table("dim_location")
dim_location.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✓ Written {dim_location.count()} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Data

# COMMAND ----------

display(spark.table(table_name).limit(10))
