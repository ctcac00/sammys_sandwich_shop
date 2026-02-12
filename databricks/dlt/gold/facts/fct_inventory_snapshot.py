# Databricks notebook source
# MAGIC %md
# MAGIC # Fact: Inventory Snapshot
# MAGIC Grain: one row per location per ingredient.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, current_timestamp, current_date, year, month, dayofmonth
)
from pyspark.sql.types import IntegerType

# COMMAND ----------

@dlt.table(
    name="fct_inventory_snapshot",
    comment="Current inventory status by location and ingredient",
    table_properties={"quality": "gold"}
)
def fct_inventory_snapshot():
    inventory = dlt.read("int_inventory")
    dim_location = dlt.read("dim_location").filter(col("is_current") == True)
    dim_ingredient = dlt.read("dim_ingredient").filter(col("is_current") == True)
    
    return (
        inventory.alias("i")
        .join(dim_location.select("location_id", "location_sk").alias("dl"),
              col("i.location_id") == col("dl.location_id"), "left")
        .join(dim_ingredient.select("ingredient_id", "ingredient_sk").alias("di"),
              col("i.ingredient_id") == col("di.ingredient_id"), "left")
        .select(
            col("i.inventory_id"),
            # Dimension keys
            col("dl.location_sk"),
            col("di.ingredient_sk"),
            # Snapshot date
            current_date().alias("snapshot_date"),
            (year(current_date()) * 10000 + 
             month(current_date()) * 100 + 
             dayofmonth(current_date())).cast(IntegerType()).alias("date_key"),
            # Measures
            col("i.quantity_on_hand"),
            col("i.inventory_value"),
            col("i.reorder_level"),
            col("i.reorder_quantity"),
            col("i.needs_reorder"),
            col("i.days_since_restock"),
            col("i.days_until_expiration"),
            col("i.expiring_soon"),
            col("i.last_restock_date"),
            col("i.expiration_date"),
            # Metadata
            current_timestamp().alias("_created_at")
        )
    )
