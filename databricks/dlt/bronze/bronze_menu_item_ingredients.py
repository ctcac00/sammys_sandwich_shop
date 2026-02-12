# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Menu Item Ingredients
# MAGIC Loads raw menu item ingredients mapping from CSV into a Bronze Delta Live Table.

# COMMAND ----------

import dlt
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

@dlt.table(
    name="bronze_menu_item_ingredients",
    comment="Raw menu item ingredients mapping from CSV source",
    table_properties={"quality": "bronze"}
)
def bronze_menu_item_ingredients():
    return (
        spark.read.csv(f"{DATA_PATH}/{CSV_FILES['menu_item_ingredients']}", header=True, inferSchema=True)
        .withColumn("_loaded_at", current_timestamp())
        .withColumn("_source_file", lit(CSV_FILES['menu_item_ingredients']))
    )
