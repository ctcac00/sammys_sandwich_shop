# Databricks notebook source
# MAGIC %md
# MAGIC # Staging: Order Items
# MAGIC Performs data type casting and basic cleaning on raw order item data.
# MAGIC Includes data quality expectations for validation.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import col, trim
from pyspark.sql.types import IntegerType, DoubleType

# COMMAND ----------

@dp.materialized_view(
    name="stg_order_items",
    comment="Cleaned and typed order item data",
    table_properties={"quality": "silver"}
)
@dp.expect_or_drop("valid_order_item", "order_item_id IS NOT NULL")
@dp.expect("valid_quantity", "quantity > 0")
def stg_order_items():
    return (
        spark.read.table("bronze_order_items")
        .select(
            col("order_item_id"),
            col("order_id"),
            col("menu_item_id"),
            col("quantity").cast(IntegerType()).alias("quantity"),
            col("unit_price").cast(DoubleType()).alias("unit_price"),
            col("line_total").cast(DoubleType()).alias("line_total"),
            trim(col("special_instructions")).alias("special_instructions"),
            col("_loaded_at"),
            col("_source_file")
        )
    )
