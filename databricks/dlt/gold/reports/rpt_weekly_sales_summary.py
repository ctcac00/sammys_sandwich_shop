# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Weekly Sales Summary
# MAGIC Weekly aggregated sales metrics.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, count, avg, sum as spark_sum
from pyspark.sql.window import Window

# COMMAND ----------

@dlt.view(
    name="rpt_weekly_sales_summary",
    comment="Weekly aggregated sales metrics"
)
def rpt_weekly_sales_summary():
    fds = dlt.read("fct_daily_summary")
    dd = dlt.read("dim_date")
    dl = dlt.read("dim_location").filter(col("is_current") == True)
    
    return (
        fds
        .join(dd, fds.date_key == dd.date_key)
        .join(dl, fds.location_sk == dl.location_sk)
        .groupBy(dd.year, dd.week_of_year, dl.location_name, dl.region)
        .agg(
            spark_sum("total_orders").alias("total_orders"),
            spark_sum("total_items_sold").alias("total_items_sold"),
            spark_sum("gross_sales").alias("gross_sales"),
            spark_sum("net_sales").alias("net_sales"),
            spark_sum("total_revenue").alias("total_revenue"),
            avg("avg_order_value").alias("avg_order_value"),
            count("*").alias("days_with_sales")
        )
        .orderBy(col("year").desc(), col("week_of_year").desc(), "location_name")
    )
