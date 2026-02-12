# Databricks notebook source
# MAGIC %md
# MAGIC # Dimension: Ingredient
# MAGIC Ingredient dimension with supplier and cost info.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, lit, current_timestamp, current_date, md5
)
from pyspark.sql.types import StringType

# COMMAND ----------

@dlt.table(
    name="dim_ingredient",
    comment="Ingredient dimension with supplier and cost info",
    table_properties={"quality": "gold"}
)
def dim_ingredient():
    return (
        dlt.read("int_ingredients")
        .select(
            md5(col("ingredient_id").cast(StringType())).alias("ingredient_sk"),
            col("ingredient_id"),
            col("ingredient_name"),
            col("category"),
            col("unit_of_measure"),
            col("unit_cost"),
            col("supplier_id"),
            col("supplier_name"),
            col("supplier_lead_time_days"),
            col("reorder_level"),
            col("reorder_quantity"),
            col("is_perishable"),
            col("shelf_life_days"),
            current_date().alias("effective_date"),
            lit(True).alias("is_current"),
            current_timestamp().alias("_created_at"),
            current_timestamp().alias("_updated_at")
        )
    )
