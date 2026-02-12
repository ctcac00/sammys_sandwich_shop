# Databricks notebook source
# MAGIC %md
# MAGIC # Intermediate: Inventory
# MAGIC Enriches inventory with reorder status and expiration tracking.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, when, lit, datediff, current_date, current_timestamp
)

# COMMAND ----------

@dlt.table(
    name="int_inventory",
    comment="Inventory with reorder status",
    table_properties={"quality": "silver"}
)
def int_inventory():
    inventory = dlt.read("stg_inventory")
    ingredients = dlt.read("int_ingredients")
    
    return (
        inventory.alias("i")
        .join(
            ingredients.select(
                col("ingredient_id"),
                col("ingredient_name"),
                col("unit_of_measure"),
                col("unit_cost"),
                col("reorder_level"),
                col("reorder_quantity"),
                col("is_perishable"),
                col("shelf_life_days")
            ).alias("ing"),
            col("i.ingredient_id") == col("ing.ingredient_id"),
            "left"
        )
        .select(
            col("i.inventory_id"),
            col("i.location_id"),
            col("i.ingredient_id"),
            col("ing.ingredient_name"),
            col("i.quantity_on_hand"),
            col("ing.unit_of_measure"),
            col("ing.unit_cost"),
            (col("i.quantity_on_hand") * col("ing.unit_cost")).alias("inventory_value"),
            col("ing.reorder_level"),
            col("ing.reorder_quantity"),
            when(col("i.quantity_on_hand") <= col("ing.reorder_level"), lit(True))
                .otherwise(lit(False)).alias("needs_reorder"),
            col("i.last_restock_date"),
            datediff(current_date(), col("i.last_restock_date")).alias("days_since_restock"),
            col("i.expiration_date"),
            datediff(col("i.expiration_date"), current_date()).alias("days_until_expiration"),
            col("ing.is_perishable"),
            when(
                col("ing.is_perishable") & (datediff(col("i.expiration_date"), current_date()) <= 3),
                lit(True)
            ).otherwise(lit(False)).alias("expiring_soon"),
            current_timestamp().alias("_enriched_at")
        )
    )
