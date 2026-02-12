# Databricks notebook source
# MAGIC %md
# MAGIC # Staging: Ingredients
# MAGIC Performs data type casting and basic cleaning on raw ingredient data.
# MAGIC Includes data quality expectations for validation.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, trim, upper, lit, when
)
from pyspark.sql.types import IntegerType, DoubleType

# COMMAND ----------

@dlt.table(
    name="stg_ingredients",
    comment="Cleaned and typed ingredient data",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_ingredient_id", "ingredient_id IS NOT NULL")
def stg_ingredients():
    return (
        dlt.read("bronze_ingredients")
        .select(
            col("ingredient_id"),
            trim(col("ingredient_name")).alias("ingredient_name"),
            trim(col("category")).alias("category"),
            trim(col("unit_of_measure")).alias("unit_of_measure"),
            col("unit_cost").cast(DoubleType()).alias("unit_cost"),
            trim(col("supplier_id")).alias("supplier_id"),
            col("reorder_level").cast(IntegerType()).alias("reorder_level"),
            col("reorder_quantity").cast(IntegerType()).alias("reorder_quantity"),
            when(upper(trim(col("is_perishable"))).isin("TRUE", "YES", "1"), lit(True))
                .otherwise(lit(False)).alias("is_perishable"),
            col("shelf_life_days").cast(IntegerType()).alias("shelf_life_days"),
            col("_loaded_at"),
            col("_source_file")
        )
    )
