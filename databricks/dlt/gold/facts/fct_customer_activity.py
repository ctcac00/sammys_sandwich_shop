# Databricks notebook source
# MAGIC %md
# MAGIC # Fact: Customer Activity
# MAGIC Grain: one row per customer with lifetime metrics and RFM scores.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, when, current_timestamp, datediff,
    round as spark_round, sum as spark_sum, count, countDistinct, avg,
    max as spark_max, min as spark_min, ntile
)
from pyspark.sql.window import Window

# COMMAND ----------

@dlt.table(
    name="fct_customer_activity",
    comment="Customer lifetime metrics and RFM segmentation",
    table_properties={"quality": "gold"}
)
def fct_customer_activity():
    fct_sales = dlt.read("fct_sales").filter(~col("is_guest_order"))
    fct_line_items = dlt.read("fct_sales_line_item")
    dim_date = dlt.read("dim_date")
    
    # Get max date from sales
    max_date = fct_sales.join(dim_date, "date_key").agg(spark_max("full_date").alias("max_date"))
    
    # Customer aggregates
    customer_metrics = (
        fct_sales
        .join(dim_date, "date_key")
        .groupBy("customer_sk")
        .agg(
            count("*").alias("total_orders"),
            spark_sum("total_amount").alias("total_revenue"),
            spark_sum("item_count").alias("total_items"),
            avg("total_amount").alias("avg_order_value"),
            spark_min("full_date").alias("first_order_date"),
            spark_max("full_date").alias("last_order_date"),
            countDistinct("location_sk").alias("locations_visited"),
            avg("tip_pct").alias("avg_tip_pct"),
            spark_sum(when(col("has_discount"), 1).otherwise(0)).alias("orders_with_discount")
        )
    )
    
    # Calculate RFM scores using window functions
    window_spec = Window.orderBy("customer_sk")
    
    return (
        customer_metrics
        .crossJoin(max_date)
        .select(
            col("customer_sk"),
            col("total_orders"),
            col("total_revenue"),
            col("total_items"),
            col("avg_order_value"),
            col("first_order_date"),
            col("last_order_date"),
            datediff(col("max_date"), col("last_order_date")).alias("days_since_last_order"),
            datediff(col("last_order_date"), col("first_order_date")).alias("customer_lifespan_days"),
            col("locations_visited"),
            col("avg_tip_pct"),
            col("orders_with_discount"),
            spark_round(col("orders_with_discount") / col("total_orders") * 100, 2).alias("discount_usage_rate"),
            # RFM scores (using ntile for quintiles)
            ntile(5).over(Window.orderBy(col("days_since_last_order").desc())).alias("recency_score"),
            ntile(5).over(Window.orderBy("total_orders")).alias("frequency_score"),
            ntile(5).over(Window.orderBy("total_revenue")).alias("monetary_score"),
            current_timestamp().alias("_created_at")
        )
        .withColumn("rfm_segment",
            when((col("recency_score") >= 4) & (col("frequency_score") >= 4) & (col("monetary_score") >= 4), "Champions")
            .when((col("recency_score") >= 4) & (col("frequency_score") >= 3), "Loyal Customers")
            .when((col("recency_score") >= 4), "Recent Customers")
            .when((col("frequency_score") >= 4), "Frequent Buyers")
            .when((col("monetary_score") >= 4), "Big Spenders")
            .when((col("recency_score") <= 2) & (col("frequency_score") <= 2), "At Risk")
            .when((col("recency_score") <= 2), "Hibernating")
            .otherwise("Average"))
    )
