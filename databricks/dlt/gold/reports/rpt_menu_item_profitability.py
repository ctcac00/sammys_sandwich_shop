# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Menu Item Profitability
# MAGIC Menu item profitability analysis.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import col

# COMMAND ----------

@dp.temporary_view(
    name="rpt_menu_item_profitability",
    comment="Menu item profitability analysis"
)
def rpt_menu_item_profitability():
    fmp = spark.read.table("fct_menu_item_performance")
    dm = spark.read.table("dim_menu_item").filter(col("is_current") == True)
    
    return (
        fmp
        .join(dm, fmp.menu_item_sk == dm.menu_item_sk)
        .select(
            dm.item_name,
            dm.category,
            dm.price,
            dm.cost,
            dm.profit.alias("unit_profit"),
            dm.margin_group,
            fmp.total_quantity_sold,
            fmp.total_revenue,
            fmp.total_cost,
            fmp.total_profit,
            fmp.profit_margin_pct,
            fmp.profit_rank
        )
        .orderBy("profit_rank")
    )
