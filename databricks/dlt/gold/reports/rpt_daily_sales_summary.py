# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Daily Sales Summary
# MAGIC Daily sales metrics with day-over-day and week-over-week comparisons.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, lag
from pyspark.sql.window import Window

# COMMAND ----------

@dlt.view(
    name="rpt_daily_sales_summary",
    comment="Daily sales metrics with day-over-day and week-over-week comparisons"
)
def rpt_daily_sales_summary():
    fds = dlt.read("fct_daily_summary")
    dd = dlt.read("dim_date")
    dl = dlt.read("dim_location").filter(col("is_current") == True)
    
    window_spec = Window.partitionBy("location_sk").orderBy("full_date")
    
    return (
        fds
        .join(dd, fds.date_key == dd.date_key)
        .join(dl, fds.location_sk == dl.location_sk)
        .select(
            dd.full_date.alias("order_date"),
            dd.day_name,
            dd.is_weekend,
            dl.location_name,
            dl.region,
            fds.total_orders,
            fds.total_items_sold,
            fds.unique_customers,
            fds.guest_orders,
            fds.gross_sales,
            fds.discounts_given,
            fds.net_sales,
            fds.tax_collected,
            fds.tips_received,
            fds.total_revenue,
            fds.avg_order_value,
            fds.avg_items_per_order,
            fds.discount_rate,
            # Day over day comparison
            lag("total_revenue", 1).over(window_spec).alias("prev_day_revenue"),
            (fds.total_revenue - lag("total_revenue", 1).over(window_spec)).alias("revenue_change"),
            # Week over week comparison
            lag("total_revenue", 7).over(window_spec).alias("same_day_last_week_revenue")
        )
        .orderBy(col("order_date").desc(), "location_name")
    )
