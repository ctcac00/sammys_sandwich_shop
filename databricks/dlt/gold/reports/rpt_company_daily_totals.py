# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Company Daily Totals
# MAGIC Company-wide daily totals across all locations.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, count, countDistinct, avg, sum as spark_sum

# COMMAND ----------

@dlt.view(
    name="rpt_company_daily_totals",
    comment="Company-wide daily totals across all locations"
)
def rpt_company_daily_totals():
    fds = dlt.read("fct_daily_summary")
    dd = dlt.read("dim_date")
    
    return (
        fds
        .join(dd, fds.date_key == dd.date_key)
        .groupBy(dd.date_key, dd.full_date, dd.day_name, dd.is_weekend)
        .agg(
            countDistinct("location_sk").alias("locations_reporting"),
            spark_sum("total_orders").alias("total_orders"),
            spark_sum("total_items_sold").alias("total_items_sold"),
            spark_sum("unique_customers").alias("total_customers"),
            spark_sum("gross_sales").alias("gross_sales"),
            spark_sum("net_sales").alias("net_sales"),
            spark_sum("total_revenue").alias("total_revenue"),
            avg("avg_order_value").alias("avg_order_value")
        )
        .orderBy(col("full_date").desc())
    )
