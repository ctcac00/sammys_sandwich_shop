# Databricks notebook source
# MAGIC %md
# MAGIC # Fact: Menu Item Performance
# MAGIC Grain: one row per menu item with performance rankings.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, current_timestamp, count, countDistinct, avg, dense_rank,
    sum as spark_sum
)
from pyspark.sql.window import Window

# COMMAND ----------

@dlt.table(
    name="fct_menu_item_performance",
    comment="Menu item performance metrics and rankings",
    table_properties={"quality": "gold"}
)
def fct_menu_item_performance():
    fct_line_items = dlt.read("fct_sales_line_item")
    dim_menu_item = dlt.read("dim_menu_item").filter(col("is_current") == True)
    
    # Menu item aggregates
    item_metrics = (
        fct_line_items
        .groupBy("menu_item_sk")
        .agg(
            count("*").alias("times_ordered"),
            spark_sum("quantity").alias("total_quantity_sold"),
            spark_sum("line_total").alias("total_revenue"),
            spark_sum("line_cost").alias("total_cost"),
            spark_sum("line_profit").alias("total_profit"),
            avg("line_profit").alias("avg_profit_per_sale"),
            countDistinct("order_id").alias("unique_orders"),
            countDistinct("customer_sk").alias("unique_customers")
        )
    )
    
    # Add rankings
    return (
        item_metrics
        .join(dim_menu_item.select("menu_item_sk", "menu_item_id", "item_name", 
                                    "category", "profit_margin_pct"),
              "menu_item_sk", "inner")
        .select(
            col("menu_item_sk"),
            col("menu_item_id"),
            col("item_name"),
            col("category"),
            col("times_ordered"),
            col("total_quantity_sold"),
            col("total_revenue"),
            col("total_cost"),
            col("total_profit"),
            col("profit_margin_pct"),
            col("avg_profit_per_sale"),
            col("unique_orders"),
            col("unique_customers"),
            # Rankings
            dense_rank().over(Window.orderBy(col("total_quantity_sold").desc())).alias("sales_rank"),
            dense_rank().over(Window.orderBy(col("total_revenue").desc())).alias("revenue_rank"),
            dense_rank().over(Window.orderBy(col("total_profit").desc())).alias("profit_rank"),
            dense_rank().over(
                Window.partitionBy("category").orderBy(col("total_quantity_sold").desc())
            ).alias("category_sales_rank"),
            current_timestamp().alias("_created_at")
        )
    )
