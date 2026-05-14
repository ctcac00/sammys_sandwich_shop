# Databricks notebook source
# MAGIC %md
# MAGIC # Intermediate: Menu Items
# MAGIC Adds profit margin calculations to menu item data.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import col, when, current_timestamp

# COMMAND ----------

@dp.materialized_view(
    name="int_menu_items",
    comment="Menu item data with profit margins",
    table_properties={"quality": "silver"}
)
def int_menu_items():
    return (
        spark.read.table("stg_menu_items")
        .select(
            col("menu_item_id"),
            col("item_name"),
            col("description"),
            col("category"),
            col("subcategory"),
            col("price"),
            col("cost"),
            (col("price") - col("cost")).alias("profit"),
            when(col("price") > 0, ((col("price") - col("cost")) / col("price") * 100))
                .otherwise(0).alias("profit_margin_pct"),
            col("calories"),
            col("is_vegetarian"),
            col("is_gluten_free"),
            col("is_available"),
            current_timestamp().alias("_enriched_at")
        )
    )
