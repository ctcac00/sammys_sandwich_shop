# Databricks notebook source
# MAGIC %md
# MAGIC # Intermediate: Orders
# MAGIC Adds derived fields and business logic enrichments.
# MAGIC 
# MAGIC Mirrors: `dbt/models/intermediate/int_orders.sql`

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

from pyspark.sql.functions import (
    col, concat_ws, to_timestamp, date_format, hour, dayofweek,
    current_timestamp, when, coalesce, lit, count
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Staged Data

# COMMAND ----------

df_orders = spark.table(silver_table("stg_orders"))
df_order_items = spark.table(silver_table("stg_order_items"))
print(f"Loaded {df_orders.count()} orders and {df_order_items.count()} order items")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate Item Counts

# COMMAND ----------

item_counts = df_order_items.groupBy("order_id").agg(
    count("*").alias("item_count")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Enrichments

# COMMAND ----------

enriched = df_orders.alias("o").join(
    item_counts.alias("oi"),
    col("o.order_id") == col("oi.order_id"),
    "left"
).select(
    # Original fields
    col("o.order_id"),
    col("o.customer_id"),
    col("o.employee_id"),
    col("o.location_id"),
    col("o.order_date"),
    col("o.order_time"),
    col("o.order_type"),
    col("o.order_status"),
    col("o.payment_method"),
    col("o.subtotal"),
    col("o.tax_amount"),
    col("o.discount_amount"),
    col("o.tip_amount"),
    col("o.total_amount"),
    
    # Combined datetime
    to_timestamp(concat_ws(" ", col("o.order_date"), col("o.order_time"))).alias("order_datetime"),
    
    # Day of week
    date_format(col("o.order_date"), "EEEE").alias("order_day_of_week"),
    
    # Hour of order - extract from time string
    hour(to_timestamp(concat_ws(" ", col("o.order_date"), col("o.order_time")))).alias("order_hour"),
    
    # Time period
    when(hour(to_timestamp(concat_ws(" ", col("o.order_date"), col("o.order_time")))).between(6, 10), "Breakfast")
    .when(hour(to_timestamp(concat_ws(" ", col("o.order_date"), col("o.order_time")))).between(11, 14), "Lunch")
    .when(hour(to_timestamp(concat_ws(" ", col("o.order_date"), col("o.order_time")))).between(15, 17), "Afternoon")
    .when(hour(to_timestamp(concat_ws(" ", col("o.order_date"), col("o.order_time")))).between(18, 21), "Dinner")
    .otherwise("Late Night").alias("order_period"),
    
    # Weekend flag (Spark: Sunday=1, Saturday=7)
    dayofweek(col("o.order_date")).isin(1, 7).alias("is_weekend"),
    
    # Boolean flags
    (col("o.discount_amount") > 0).alias("has_discount"),
    (col("o.tip_amount") > 0).alias("has_tip"),
    col("o.customer_id").isNull().alias("is_guest_order"),
    
    # Item count from order items
    coalesce(col("oi.item_count"), lit(0)).alias("item_count"),
    
    # Metadata
    current_timestamp().alias("_enriched_at")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Table

# COMMAND ----------

table_name = silver_table("int_orders")
enriched.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✓ Written {enriched.count()} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Data

# COMMAND ----------

display(spark.table(table_name).limit(10))
