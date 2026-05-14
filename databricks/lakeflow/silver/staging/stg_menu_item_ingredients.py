# Databricks notebook source
# MAGIC %md
# MAGIC # Staging: Menu Item Ingredients
# MAGIC Performs data type casting and basic cleaning on menu item to ingredient mappings.
# MAGIC Includes data quality expectations for validation.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import col, trim
from pyspark.sql.types import DoubleType

# COMMAND ----------

@dp.materialized_view(
    name="stg_menu_item_ingredients",
    comment="Cleaned menu item to ingredient mappings",
    table_properties={"quality": "silver"}
)
@dp.expect_or_drop("valid_mapping", "menu_item_id IS NOT NULL AND ingredient_id IS NOT NULL")
def stg_menu_item_ingredients():
    return (
        spark.read.table("bronze_menu_item_ingredients")
        .select(
            col("menu_item_id"),
            col("ingredient_id"),
            col("quantity").cast(DoubleType()).alias("quantity"),
            trim(col("unit_of_measure")).alias("unit_of_measure"),
            col("_loaded_at"),
            col("_source_file")
        )
    )
