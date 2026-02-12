# Databricks notebook source
# MAGIC %md
# MAGIC # Staging: Menu Items
# MAGIC Performs data type casting and basic cleaning on raw menu item data.
# MAGIC Includes data quality expectations for validation.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, trim, upper, lit, when
)
from pyspark.sql.types import IntegerType, DoubleType

# COMMAND ----------

@dlt.table(
    name="stg_menu_items",
    comment="Cleaned and typed menu item data",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_menu_item_id", "menu_item_id IS NOT NULL")
@dlt.expect("positive_price", "price > 0")
def stg_menu_items():
    return (
        dlt.read("bronze_menu_items")
        .select(
            col("menu_item_id"),
            trim(col("item_name")).alias("item_name"),
            trim(col("description")).alias("description"),
            trim(col("category")).alias("category"),
            trim(col("subcategory")).alias("subcategory"),
            col("price").cast(DoubleType()).alias("price"),
            col("cost").cast(DoubleType()).alias("cost"),
            col("calories").cast(IntegerType()).alias("calories"),
            when(upper(trim(col("is_vegetarian"))).isin("TRUE", "YES", "1"), lit(True))
                .otherwise(lit(False)).alias("is_vegetarian"),
            when(upper(trim(col("is_gluten_free"))).isin("TRUE", "YES", "1"), lit(True))
                .otherwise(lit(False)).alias("is_gluten_free"),
            when(upper(trim(col("is_available"))).isin("TRUE", "YES", "1"), lit(True))
                .otherwise(lit(False)).alias("is_available"),
            col("_loaded_at"),
            col("_source_file")
        )
    )
