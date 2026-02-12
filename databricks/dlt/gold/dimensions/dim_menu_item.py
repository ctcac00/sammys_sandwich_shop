# Databricks notebook source
# MAGIC %md
# MAGIC # Dimension: Menu Item
# MAGIC Menu item dimension with profitability info.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, lit, when, current_timestamp, current_date, md5
)
from pyspark.sql.types import StringType

# COMMAND ----------

@dlt.table(
    name="dim_menu_item",
    comment="Menu item dimension with profitability info",
    table_properties={"quality": "gold"}
)
def dim_menu_item():
    return (
        dlt.read("int_menu_items")
        .select(
            md5(col("menu_item_id").cast(StringType())).alias("menu_item_sk"),
            col("menu_item_id"),
            col("item_name"),
            col("description"),
            col("category"),
            col("subcategory"),
            col("price"),
            col("cost"),
            col("profit"),
            col("profit_margin_pct"),
            # Margin grouping
            when(col("profit_margin_pct") < 20, "Low (< 20%)")
            .when(col("profit_margin_pct") < 40, "Medium (20-40%)")
            .when(col("profit_margin_pct") < 60, "High (40-60%)")
            .otherwise("Premium (60%+)").alias("margin_group"),
            col("calories"),
            # Calorie grouping
            when(col("calories") < 300, "Light (< 300)")
            .when(col("calories") < 600, "Medium (300-600)")
            .when(col("calories") < 900, "Hearty (600-900)")
            .otherwise("Indulgent (900+)").alias("calorie_group"),
            col("is_vegetarian"),
            col("is_gluten_free"),
            col("is_available"),
            current_date().alias("effective_date"),
            lit(True).alias("is_current"),
            current_timestamp().alias("_created_at"),
            current_timestamp().alias("_updated_at")
        )
    )
