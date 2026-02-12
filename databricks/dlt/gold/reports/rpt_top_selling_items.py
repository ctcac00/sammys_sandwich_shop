# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Top Selling Items
# MAGIC Most popular menu items by quantity sold.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col

# COMMAND ----------

@dlt.view(
    name="rpt_top_selling_items",
    comment="Most popular menu items by quantity sold"
)
def rpt_top_selling_items():
    fmp = dlt.read("fct_menu_item_performance")
    dm = dlt.read("dim_menu_item").filter(col("is_current") == True)
    
    return (
        fmp
        .join(dm, fmp.menu_item_sk == dm.menu_item_sk)
        .select(
            dm.item_name,
            dm.category,
            dm.subcategory,
            dm.price,
            fmp.total_quantity_sold,
            fmp.total_revenue,
            fmp.total_profit,
            fmp.profit_margin_pct,
            fmp.unique_customers,
            fmp.sales_rank,
            fmp.category_sales_rank
        )
        .orderBy("sales_rank")
    )
