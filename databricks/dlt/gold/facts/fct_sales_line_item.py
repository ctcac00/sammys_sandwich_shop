# Databricks notebook source
# MAGIC %md
# MAGIC # Fact: Sales Line Item
# MAGIC Grain: one row per order line item.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

@dlt.table(
    name="fct_sales_line_item",
    comment="Order line item detail fact table with profitability",
    table_properties={"quality": "gold"}
)
def fct_sales_line_item():
    order_items = dlt.read("int_order_items")
    orders = dlt.read("int_orders").filter(col("order_status") == "Completed")
    dim_menu_item = dlt.read("dim_menu_item").filter(col("is_current") == True)
    fct_sales = dlt.read("fct_sales")
    
    return (
        order_items.alias("oi")
        .join(orders.select("order_id", "order_date", "order_datetime", "location_id").alias("o"),
              col("oi.order_id") == col("o.order_id"), "inner")
        .join(fct_sales.select("order_id", "date_key", "time_key", "customer_sk", 
                               "employee_sk", "location_sk").alias("fs"),
              col("oi.order_id") == col("fs.order_id"), "inner")
        .join(dim_menu_item.alias("dm"),
              col("oi.menu_item_id") == col("dm.menu_item_id"), "left")
        .select(
            col("oi.order_item_id"),
            col("oi.order_id"),
            # Dimension keys
            col("fs.date_key"),
            col("fs.time_key"),
            col("fs.customer_sk"),
            col("fs.employee_sk"),
            col("fs.location_sk"),
            col("dm.menu_item_sk"),
            # Item attributes
            col("oi.menu_item_id"),
            col("oi.item_name"),
            col("oi.category"),
            # Measures
            col("oi.quantity"),
            col("oi.unit_price"),
            col("oi.line_total"),
            col("oi.item_cost"),
            col("oi.line_cost"),
            col("oi.line_profit"),
            col("oi.profit_margin_pct"),
            # Metadata
            current_timestamp().alias("_created_at")
        )
    )
