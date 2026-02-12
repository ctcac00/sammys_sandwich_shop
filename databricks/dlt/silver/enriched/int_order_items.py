# Databricks notebook source
# MAGIC %md
# MAGIC # Intermediate: Order Items
# MAGIC Enriches order items with menu item details and profitability.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

@dlt.table(
    name="int_order_items",
    comment="Order items with menu item details",
    table_properties={"quality": "silver"}
)
def int_order_items():
    order_items = dlt.read("stg_order_items")
    menu_items = dlt.read("int_menu_items")
    
    return (
        order_items.alias("oi")
        .join(
            menu_items.select(
                col("menu_item_id"),
                col("item_name"),
                col("category"),
                col("cost").alias("item_cost"),
                col("profit_margin_pct")
            ).alias("mi"),
            col("oi.menu_item_id") == col("mi.menu_item_id"),
            "left"
        )
        .select(
            col("oi.order_item_id"),
            col("oi.order_id"),
            col("oi.menu_item_id"),
            col("mi.item_name"),
            col("mi.category"),
            col("oi.quantity"),
            col("oi.unit_price"),
            col("oi.line_total"),
            col("mi.item_cost"),
            (col("oi.quantity") * col("mi.item_cost")).alias("line_cost"),
            (col("oi.line_total") - (col("oi.quantity") * col("mi.item_cost"))).alias("line_profit"),
            col("mi.profit_margin_pct"),
            col("oi.special_instructions"),
            current_timestamp().alias("_enriched_at")
        )
    )
