# Databricks notebook source
# MAGIC %md
# MAGIC # Intermediate: Orders
# MAGIC Enriches orders with item counts and derived flags.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, coalesce, lit, when, current_timestamp, dayofweek, hour,
    sum as spark_sum, count
)

# COMMAND ----------

@dlt.table(
    name="int_orders",
    comment="Orders with item counts and derived flags",
    table_properties={"quality": "silver"}
)
def int_orders():
    orders = dlt.read("stg_orders")
    order_items = dlt.read("stg_order_items")
    
    # Aggregate order items
    item_counts = (
        order_items.groupBy("order_id")
        .agg(
            spark_sum("quantity").alias("item_count"),
            count("*").alias("line_item_count")
        )
    )
    
    return (
        orders.alias("o")
        .join(item_counts.alias("ic"), col("o.order_id") == col("ic.order_id"), "left")
        .select(
            col("o.order_id"),
            col("o.customer_id"),
            col("o.employee_id"),
            col("o.location_id"),
            col("o.order_date"),
            col("o.order_datetime"),
            col("o.order_type"),
            col("o.order_status"),
            col("o.payment_method"),
            coalesce(col("ic.item_count"), lit(0)).alias("item_count"),
            coalesce(col("ic.line_item_count"), lit(0)).alias("line_item_count"),
            col("o.subtotal"),
            col("o.tax_amount"),
            col("o.discount_amount"),
            col("o.tip_amount"),
            col("o.total_amount"),
            # Derived flags
            when(col("o.discount_amount") > 0, lit(True)).otherwise(lit(False)).alias("has_discount"),
            when(col("o.tip_amount") > 0, lit(True)).otherwise(lit(False)).alias("has_tip"),
            when(col("o.customer_id").isNull(), lit(True)).otherwise(lit(False)).alias("is_guest_order"),
            when(dayofweek(col("o.order_date")).isin(1, 7), lit(True)).otherwise(lit(False)).alias("is_weekend"),
            # Time period
            when(hour(col("o.order_datetime")).between(6, 10), "Breakfast")
            .when(hour(col("o.order_datetime")).between(11, 14), "Lunch")
            .when(hour(col("o.order_datetime")).between(15, 17), "Afternoon")
            .when(hour(col("o.order_datetime")).between(18, 21), "Dinner")
            .otherwise("Late Night").alias("order_period"),
            current_timestamp().alias("_enriched_at")
        )
    )
