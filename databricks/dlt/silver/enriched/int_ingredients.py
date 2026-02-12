# Databricks notebook source
# MAGIC %md
# MAGIC # Intermediate: Ingredients
# MAGIC Enriches ingredient data with supplier information.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

@dlt.table(
    name="int_ingredients",
    comment="Ingredient data with supplier info",
    table_properties={"quality": "silver"}
)
def int_ingredients():
    ingredients = dlt.read("stg_ingredients")
    suppliers = dlt.read("stg_suppliers")
    
    return (
        ingredients.alias("i")
        .join(
            suppliers.select(
                col("supplier_id"),
                col("supplier_name"),
                col("lead_time_days")
            ).alias("s"),
            col("i.supplier_id") == col("s.supplier_id"),
            "left"
        )
        .select(
            col("i.ingredient_id"),
            col("i.ingredient_name"),
            col("i.category"),
            col("i.unit_of_measure"),
            col("i.unit_cost"),
            col("i.supplier_id"),
            col("s.supplier_name"),
            col("s.lead_time_days").alias("supplier_lead_time_days"),
            col("i.reorder_level"),
            col("i.reorder_quantity"),
            col("i.is_perishable"),
            col("i.shelf_life_days"),
            current_timestamp().alias("_enriched_at")
        )
    )
