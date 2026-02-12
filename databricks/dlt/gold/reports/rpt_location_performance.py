# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Location Performance
# MAGIC Location performance metrics.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, count, avg, sum as spark_sum, countDistinct
)

# COMMAND ----------

@dlt.view(
    name="rpt_location_performance",
    comment="Location performance metrics"
)
def rpt_location_performance():
    fds = dlt.read("fct_daily_summary")
    dl = dlt.read("dim_location").filter(col("is_current") == True)
    
    return (
        fds
        .join(dl, fds.location_sk == dl.location_sk)
        .groupBy(
            dl.location_name, dl.city, dl.state, dl.region, dl.district,
            dl.manager_name, dl.location_age_group
        )
        .agg(
            count("*").alias("days_with_sales"),
            spark_sum("total_orders").alias("total_orders"),
            spark_sum("total_items_sold").alias("total_items_sold"),
            spark_sum("total_revenue").alias("total_revenue"),
            spark_sum("net_sales").alias("net_sales"),
            avg("avg_order_value").alias("avg_order_value"),
            avg("total_orders").alias("avg_daily_orders"),
            avg("total_revenue").alias("avg_daily_revenue"),
            avg("unique_customers").alias("avg_daily_customers")
        )
        .orderBy(col("total_revenue").desc())
    )
