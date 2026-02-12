# Databricks notebook source
# MAGIC %md
# MAGIC # Fact: Daily Summary
# MAGIC Grain: one row per location per day.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, when, current_timestamp, count, countDistinct, avg,
    sum as spark_sum, max as spark_max
)

# COMMAND ----------

@dlt.table(
    name="fct_daily_summary",
    comment="Daily aggregated sales metrics per location",
    table_properties={"quality": "gold"}
)
def fct_daily_summary():
    fct_sales = dlt.read("fct_sales")
    
    return (
        fct_sales
        .groupBy("date_key", "location_sk")
        .agg(
            # Volume metrics
            count("*").alias("total_orders"),
            spark_sum("item_count").alias("total_items_sold"),
            countDistinct("customer_sk").alias("unique_customers"),
            spark_sum(when(col("is_guest_order"), 1).otherwise(0)).alias("guest_orders"),
            # Revenue metrics
            spark_sum("subtotal").alias("gross_sales"),
            spark_sum("discount_amount").alias("discounts_given"),
            spark_sum("net_sales").alias("net_sales"),
            spark_sum("tax_amount").alias("tax_collected"),
            spark_sum("tip_amount").alias("tips_received"),
            spark_sum("total_amount").alias("total_revenue"),
            # Performance metrics
            avg("total_amount").alias("avg_order_value"),
            avg("item_count").alias("avg_items_per_order"),
            avg("discount_pct").alias("discount_rate"),
            # Order type breakdown
            spark_sum(when(col("order_type_sk").isNotNull(), 1).otherwise(0)).alias("typed_orders"),
            # Weekend flag
            spark_max("is_weekend").alias("is_weekend"),
            # Metadata
            current_timestamp().alias("_created_at")
        )
    )
