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
    current_timestamp, when, coalesce, lit, count, expr
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

order_time_candidate_sql = """
CASE
  -- Sometimes `order_time` arrives as a full timestamp (e.g. '2026-02-06 11:30:00').
  -- In that case, extract just the time portion so we don't end up with two dates
  -- after concatenating with `order_date`.
  WHEN trim(cast(o.order_time as string)) rlike '^[0-9]{4}-[0-9]{2}-[0-9]{2}[ ]+[0-9]{2}:[0-9]{2}(:[0-9]{2})?$'
    THEN regexp_extract(
      trim(cast(o.order_time as string)),
      '([0-9]{2}:[0-9]{2}(:[0-9]{2})?)',
      1
    )
  ELSE trim(cast(o.order_time as string))
END
""".strip()

order_time_normalized_sql = f"""
CASE
  -- Normalize 'HH:mm' to 'HH:mm:ss' so casts are consistent
  WHEN ({order_time_candidate_sql}) rlike '^[0-9]{{2}}:[0-9]{{2}}$'
    THEN concat(({order_time_candidate_sql}), ':00')
  ELSE ({order_time_candidate_sql})
END
""".strip()

# Use `try_cast` so malformed values return NULL (ANSI-safe) instead of crashing the pipeline.
order_datetime_expr = expr(f"""
coalesce(
  try_cast(concat_ws(' ', cast(o.order_date as string), {order_time_normalized_sql}) as timestamp),
  try_cast(trim(cast(o.order_time as string)) as timestamp)
)
""")

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
    order_datetime_expr.alias("order_datetime"),
    
    # Day of week
    date_format(col("o.order_date"), "EEEE").alias("order_day_of_week"),
    
    # Hour of order (NULL if order_datetime couldn't be parsed)
    hour(order_datetime_expr).alias("order_hour"),
    
    # Time period
    when(order_datetime_expr.isNull(), lit(None).cast("string"))
    .when(hour(order_datetime_expr).between(6, 10), "Breakfast")
    .when(hour(order_datetime_expr).between(11, 14), "Lunch")
    .when(hour(order_datetime_expr).between(15, 17), "Afternoon")
    .when(hour(order_datetime_expr).between(18, 21), "Dinner")
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
