# Databricks notebook source
# MAGIC %md
# MAGIC # Staging: Inventory
# MAGIC Performs data type casting and basic cleaning on raw inventory data.
# MAGIC Includes data quality expectations for validation.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import DoubleType

# COMMAND ----------

@dlt.table(
    name="stg_inventory",
    comment="Cleaned and typed inventory data",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_inventory_record", "location_id IS NOT NULL AND ingredient_id IS NOT NULL")
@dlt.expect("non_negative_quantity", "quantity_on_hand >= 0")
def stg_inventory():
    return (
        dlt.read("bronze_inventory")
        .select(
            col("inventory_id"),
            col("location_id"),
            col("ingredient_id"),
            col("quantity_on_hand").cast(DoubleType()).alias("quantity_on_hand"),
            to_date(col("last_restock_date"), "yyyy-MM-dd").alias("last_restock_date"),
            to_date(col("expiration_date"), "yyyy-MM-dd").alias("expiration_date"),
            col("_loaded_at"),
            col("_source_file")
        )
    )
