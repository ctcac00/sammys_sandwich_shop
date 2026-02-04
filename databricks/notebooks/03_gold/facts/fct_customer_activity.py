# Databricks notebook source
# MAGIC %md
# MAGIC # Fact: Customer Activity (Accumulating Snapshot)
# MAGIC Grain: one row per customer.
# MAGIC 
# MAGIC Mirrors: `dbt/models/marts/facts/fct_customer_activity.sql`

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, when, coalesce, current_timestamp, current_date, datediff,
    count, countDistinct, sum as spark_sum, avg as spark_avg, min as spark_min, max as spark_max,
    round as spark_round, row_number, ntile, to_date, concat
)
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

df_fct_sales = spark.table(gold_table("fct_sales"))
df_fct_line_items = spark.table(gold_table("fct_sales_line_item"))
df_dim_customer = spark.table(gold_table("dim_customer")).filter(
    (col("is_current") == True) & (col("customer_id") != UNKNOWN_CUSTOMER_ID)
)
df_dim_order_type = spark.table(gold_table("dim_order_type"))

print(f"Loaded {df_dim_customer.count()} customers (excluding unknown)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer Order Aggregations

# COMMAND ----------

customer_orders = df_fct_sales.groupBy("customer_sk").agg(
    spark_min("date_key").alias("first_order_date_key"),
    spark_max("date_key").alias("last_order_date_key"),
    countDistinct("order_id").alias("total_orders"),
    spark_sum("total_amount").alias("total_spend"),
    spark_sum("discount_amount").alias("total_discounts"),
    spark_sum("tip_amount").alias("total_tips"),
    spark_round(spark_avg("total_amount"), 2).alias("avg_order_value")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Total Items Purchased

# COMMAND ----------

customer_items = df_fct_line_items.groupBy("customer_sk").agg(
    spark_sum("quantity").alias("total_items_purchased")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Favorite Location

# COMMAND ----------

window_loc = Window.partitionBy("customer_sk").orderBy(col("cnt").desc())
favorite_location = df_fct_sales.groupBy("customer_sk", "location_sk").agg(
    count("*").alias("cnt")
).withColumn("rn", row_number().over(window_loc)).filter(col("rn") == 1).select(
    col("customer_sk"),
    col("location_sk").alias("favorite_location_sk")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Favorite Menu Item

# COMMAND ----------

window_item = Window.partitionBy("customer_sk").orderBy(col("qty").desc())
favorite_item = df_fct_line_items.groupBy("customer_sk", "menu_item_sk").agg(
    spark_sum("quantity").alias("qty")
).withColumn("rn", row_number().over(window_item)).filter(col("rn") == 1).select(
    col("customer_sk"),
    col("menu_item_sk").alias("favorite_menu_item_sk")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Favorite Order Type

# COMMAND ----------

window_ot = Window.partitionBy("customer_sk").orderBy(col("cnt").desc())
favorite_order_type = df_fct_sales.alias("fs").join(
    df_dim_order_type.alias("ot"),
    col("fs.order_type_sk") == col("ot.order_type_sk"),
    "inner"
).groupBy(col("fs.customer_sk"), col("ot.order_type")).agg(
    count("*").alias("cnt")
).withColumn("rn", row_number().over(window_ot)).filter(col("rn") == 1).select(
    col("customer_sk"),
    col("order_type").alias("favorite_order_type")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## RFM Scoring

# COMMAND ----------

# Calculate days since last order
rfm_base = customer_orders.withColumn(
    "days_since_last_order",
    datediff(
        current_date(),
        to_date(col("last_order_date_key").cast("string"), "yyyyMMdd")
    )
).withColumn(
    "customer_lifetime_days",
    datediff(
        current_date(),
        to_date(col("first_order_date_key").cast("string"), "yyyyMMdd")
    )
)

# RFM scores using ntile
window_r = Window.orderBy(col("last_order_date_key"))
window_f = Window.orderBy(col("total_orders"))
window_m = Window.orderBy(col("total_spend"))

rfm_scores = rfm_base.withColumn(
    "recency_score", ntile(5).over(window_r)
).withColumn(
    "frequency_score", ntile(5).over(window_f)
).withColumn(
    "monetary_score", ntile(5).over(window_m)
)

# RFM segment assignment
rfm_segments = rfm_scores.withColumn(
    "rfm_segment",
    when((col("recency_score") >= 4) & (col("frequency_score") >= 4) & (col("monetary_score") >= 4), "Champions")
    .when((col("recency_score") >= 3) & (col("frequency_score") >= 3) & (col("monetary_score") >= 4), "Loyal Customers")
    .when((col("recency_score") >= 4) & (col("frequency_score") <= 2), "New Customers")
    .when((col("recency_score") >= 3) & (col("frequency_score") >= 3), "Potential Loyalists")
    .when((col("recency_score") <= 2) & (col("frequency_score") >= 4), "At Risk")
    .when((col("recency_score") <= 2) & (col("frequency_score") <= 2) & (col("monetary_score") <= 2), "Lost")
    .when((col("recency_score") <= 2) & (col("monetary_score") >= 3), "Need Attention")
    .otherwise("Other")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Customer Activity Fact

# COMMAND ----------

fct_customer_activity = df_dim_customer.alias("dc") \
    .join(rfm_segments.alias("rs"), col("dc.customer_sk") == col("rs.customer_sk"), "inner") \
    .join(customer_items.alias("ci"), col("dc.customer_sk") == col("ci.customer_sk"), "left") \
    .join(favorite_location.alias("fl"), col("dc.customer_sk") == col("fl.customer_sk"), "left") \
    .join(favorite_item.alias("fi"), col("dc.customer_sk") == col("fi.customer_sk"), "left") \
    .join(favorite_order_type.alias("fot"), col("dc.customer_sk") == col("fot.customer_sk"), "left") \
    .select(
        col("dc.customer_sk"),
        col("dc.customer_id"),
        
        # Activity metrics (lifetime)
        col("rs.first_order_date_key"),
        col("rs.last_order_date_key"),
        col("rs.total_orders"),
        coalesce(col("ci.total_items_purchased"), lit(0)).alias("total_items_purchased"),
        col("rs.total_spend"),
        col("rs.total_discounts"),
        col("rs.total_tips"),
        col("rs.avg_order_value"),
        col("rs.customer_lifetime_days"),
        
        # Favorites
        col("fl.favorite_location_sk"),
        col("fi.favorite_menu_item_sk"),
        col("fot.favorite_order_type"),
        
        # Recency metrics
        col("rs.days_since_last_order"),
        
        # RFM scores
        col("rs.recency_score"),
        col("rs.frequency_score"),
        col("rs.monetary_score"),
        col("rs.rfm_segment"),
        
        # Metadata
        current_timestamp().alias("_created_at"),
        current_timestamp().alias("_updated_at")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold Table

# COMMAND ----------

table_name = gold_table("fct_customer_activity")
fct_customer_activity.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✓ Written {fct_customer_activity.count()} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Data

# COMMAND ----------

display(spark.table(table_name).limit(10))
