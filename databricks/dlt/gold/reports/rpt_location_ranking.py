# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Location Ranking
# MAGIC Location performance with rankings.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, count, avg, dense_rank, sum as spark_sum
)
from pyspark.sql.window import Window

# COMMAND ----------

@dlt.view(
    name="rpt_location_ranking",
    comment="Location performance with rankings"
)
def rpt_location_ranking():
    fds = dlt.read("fct_daily_summary")
    dl = dlt.read("dim_location").filter(col("is_current") == True)
    
    location_metrics = (
        fds
        .join(dl, fds.location_sk == dl.location_sk)
        .groupBy(dl.location_name, dl.region)
        .agg(
            spark_sum("total_revenue").alias("total_revenue"),
            spark_sum("total_orders").alias("total_orders"),
            avg("avg_order_value").alias("avg_order_value"),
            count("*").alias("days_with_sales")
        )
    )
    
    return (
        location_metrics
        .select(
            col("location_name"),
            col("region"),
            col("total_revenue"),
            col("total_orders"),
            col("avg_order_value"),
            dense_rank().over(Window.orderBy(col("total_revenue").desc())).alias("revenue_rank"),
            dense_rank().over(Window.orderBy(col("total_orders").desc())).alias("orders_rank"),
            dense_rank().over(
                Window.partitionBy("region").orderBy(col("total_revenue").desc())
            ).alias("region_rank")
        )
        .orderBy("revenue_rank")
    )
