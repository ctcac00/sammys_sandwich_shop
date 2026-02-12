# Databricks notebook source
# MAGIC %md
# MAGIC # Intermediate: Menu Item Ingredients
# MAGIC Enriches menu item ingredients with cost calculations.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

@dlt.table(
    name="int_menu_item_ingredients",
    comment="Menu item ingredients with cost calculations",
    table_properties={"quality": "silver"}
)
def int_menu_item_ingredients():
    mappings = dlt.read("stg_menu_item_ingredients")
    ingredients = dlt.read("int_ingredients")
    
    return (
        mappings.alias("m")
        .join(
            ingredients.select(
                col("ingredient_id"),
                col("ingredient_name"),
                col("unit_cost")
            ).alias("i"),
            col("m.ingredient_id") == col("i.ingredient_id"),
            "left"
        )
        .select(
            col("m.menu_item_id"),
            col("m.ingredient_id"),
            col("i.ingredient_name"),
            col("m.quantity"),
            col("m.unit_of_measure"),
            col("i.unit_cost"),
            (col("m.quantity") * col("i.unit_cost")).alias("ingredient_cost"),
            current_timestamp().alias("_enriched_at")
        )
    )
