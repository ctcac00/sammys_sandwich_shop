# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Category Performance
# MAGIC Menu category performance summary.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, count, avg, sum as spark_sum

# COMMAND ----------

@dlt.view(
    name="rpt_category_performance",
    comment="Menu category performance summary"
)
def rpt_category_performance():
    fmp = dlt.read("fct_menu_item_performance")
    dm = dlt.read("dim_menu_item").filter(col("is_current") == True)
    
    return (
        fmp
        .join(dm, fmp.menu_item_sk == dm.menu_item_sk)
        .groupBy(dm.category)
        .agg(
            count("*").alias("item_count"),
            spark_sum("total_quantity_sold").alias("total_quantity_sold"),
            spark_sum("total_revenue").alias("total_revenue"),
            spark_sum("total_profit").alias("total_profit"),
            avg("profit_margin_pct").alias("avg_profit_margin_pct"),
            spark_sum("unique_customers").alias("total_unique_customers")
        )
        .orderBy(col("total_revenue").desc())
    )
