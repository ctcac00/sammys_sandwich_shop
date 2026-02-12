# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Inventory Value by Location
# MAGIC Inventory value breakdown by location.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, count, sum as spark_sum, countDistinct
)

# COMMAND ----------

@dlt.view(
    name="rpt_inventory_value_by_location",
    comment="Inventory value breakdown by location"
)
def rpt_inventory_value_by_location():
    fi = dlt.read("fct_inventory_snapshot")
    dl = dlt.read("dim_location").filter(col("is_current") == True)
    
    return (
        fi
        .join(dl, fi.location_sk == dl.location_sk)
        .groupBy(dl.location_name, dl.region)
        .agg(
            countDistinct("ingredient_sk").alias("unique_ingredients"),
            spark_sum("quantity_on_hand").alias("total_units_on_hand"),
            spark_sum("inventory_value").alias("total_inventory_value"),
            spark_sum(col("needs_reorder").cast("int")).alias("items_needing_reorder"),
            spark_sum(col("expiring_soon").cast("int")).alias("items_expiring_soon")
        )
        .orderBy(col("total_inventory_value").desc())
    )
