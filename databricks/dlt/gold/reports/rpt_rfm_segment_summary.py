# Databricks notebook source
# MAGIC %md
# MAGIC # Report: RFM Segment Summary
# MAGIC RFM segmentation analysis.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, count, avg, round as spark_round, sum as spark_sum
)
from pyspark.sql.window import Window

# COMMAND ----------

@dlt.view(
    name="rpt_rfm_segment_summary",
    comment="RFM segmentation analysis"
)
def rpt_rfm_segment_summary():
    fca = dlt.read("fct_customer_activity")
    
    return (
        fca
        .groupBy("rfm_segment")
        .agg(
            count("*").alias("customer_count"),
            spark_sum("total_orders").alias("total_orders"),
            spark_sum("total_revenue").alias("total_revenue"),
            avg("total_orders").alias("avg_orders_per_customer"),
            avg("total_revenue").alias("avg_revenue_per_customer"),
            avg("avg_order_value").alias("avg_order_value"),
            avg("days_since_last_order").alias("avg_days_since_last_order")
        )
        .withColumn("revenue_pct", 
            spark_round(col("total_revenue") / spark_sum("total_revenue").over(Window.partitionBy()) * 100, 2))
        .orderBy(col("total_revenue").desc())
    )
