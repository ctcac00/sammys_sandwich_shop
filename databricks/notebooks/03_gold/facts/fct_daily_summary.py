# Databricks notebook source
# MAGIC %md
# MAGIC # Fact: Daily Summary
# MAGIC Grain: one row per location/day.
# MAGIC 
# MAGIC Mirrors: `dbt/models/marts/facts/fct_daily_summary.sql`

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, when, coalesce, current_timestamp, year, month, dayofmonth,
    count, countDistinct, sum as spark_sum, avg as spark_avg, round as spark_round,
    md5, concat_ws
)
from pyspark.sql.types import IntegerType, StringType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

df_orders = spark.table(silver_table("int_orders")).filter(col("order_status") == "Completed")
df_order_items = spark.table(silver_table("int_order_items"))
df_dim_location = spark.table(gold_table("dim_location")).filter(col("is_current") == True)

print(f"Loaded {df_orders.count()} completed orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate Order Items

# COMMAND ----------

order_item_counts = df_order_items.groupBy("order_id").agg(
    spark_sum("quantity").alias("total_items")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate Daily Metrics

# COMMAND ----------

daily_metrics = df_orders.alias("o").join(
    order_item_counts.alias("oic"),
    col("o.order_id") == col("oic.order_id"),
    "left"
).groupBy(
    col("o.order_date"),
    col("o.location_id")
).agg(
    # Order metrics
    countDistinct(col("o.order_id")).alias("total_orders"),
    spark_sum(coalesce(col("oic.total_items"), lit(0))).alias("total_items_sold"),
    countDistinct(when(~col("o.is_guest_order"), col("o.customer_id"))).alias("unique_customers"),
    spark_sum(when(col("o.is_guest_order"), 1).otherwise(0)).alias("guest_orders"),
    
    # Revenue metrics
    spark_sum(col("o.subtotal")).alias("gross_sales"),
    spark_sum(col("o.discount_amount")).alias("discounts_given"),
    spark_sum(col("o.subtotal") - col("o.discount_amount")).alias("net_sales"),
    spark_sum(col("o.tax_amount")).alias("tax_collected"),
    spark_sum(col("o.tip_amount")).alias("tips_received"),
    spark_sum(col("o.total_amount")).alias("total_revenue"),
    
    # Calculated metrics
    spark_round(spark_avg(col("o.total_amount")), 2).alias("avg_order_value"),
    spark_round(spark_avg(coalesce(col("oic.total_items"), lit(0))), 2).alias("avg_items_per_order"),
    
    # Order type breakdown
    spark_sum(when(col("o.order_type") == "Dine-In", 1).otherwise(0)).alias("dine_in_orders"),
    spark_sum(when(col("o.order_type") == "Takeout", 1).otherwise(0)).alias("takeout_orders"),
    spark_sum(when(col("o.order_type") == "Drive-Thru", 1).otherwise(0)).alias("drive_thru_orders"),
    
    # Period breakdown
    spark_sum(when(col("o.order_period") == "Breakfast", 1).otherwise(0)).alias("breakfast_orders"),
    spark_sum(when(col("o.order_period") == "Lunch", 1).otherwise(0)).alias("lunch_orders"),
    spark_sum(when(col("o.order_period") == "Dinner", 1).otherwise(0)).alias("dinner_orders")
)

# Calculate discount rate
daily_metrics = daily_metrics.withColumn(
    "discount_rate",
    spark_round(col("discounts_given") / col("gross_sales") * 100, 2)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join with Dimensions and Add Keys

# COMMAND ----------

fct_daily_summary = daily_metrics.alias("dm").join(
    df_dim_location.alias("dl"),
    col("dm.location_id") == col("dl.location_id"),
    "left"
).select(
    # Surrogate key
    md5(concat_ws("||", col("dm.order_date").cast(StringType()), col("dm.location_id").cast(StringType()))).alias("daily_summary_id"),
    
    # Keys
    (year(col("dm.order_date")) * 10000 + month(col("dm.order_date")) * 100 + dayofmonth(col("dm.order_date"))).cast(IntegerType()).alias("date_key"),
    col("dl.location_sk"),
    
    # Metrics
    col("dm.total_orders"),
    col("dm.total_items_sold"),
    col("dm.unique_customers"),
    col("dm.guest_orders"),
    col("dm.gross_sales"),
    col("dm.discounts_given"),
    col("dm.net_sales"),
    col("dm.tax_collected"),
    col("dm.tips_received"),
    col("dm.total_revenue"),
    col("dm.avg_order_value"),
    col("dm.avg_items_per_order"),
    col("dm.discount_rate"),
    col("dm.dine_in_orders"),
    col("dm.takeout_orders"),
    col("dm.drive_thru_orders"),
    col("dm.breakfast_orders"),
    col("dm.lunch_orders"),
    col("dm.dinner_orders"),
    
    # Metadata
    current_timestamp().alias("_created_at")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold Table

# COMMAND ----------

table_name = gold_table("fct_daily_summary")
fct_daily_summary.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✓ Written {fct_daily_summary.count()} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Data

# COMMAND ----------

display(spark.table(table_name).limit(10))
