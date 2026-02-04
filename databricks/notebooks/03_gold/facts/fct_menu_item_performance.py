# Databricks notebook source
# MAGIC %md
# MAGIC # Fact: Menu Item Performance
# MAGIC Grain: one row per menu item.
# MAGIC 
# MAGIC Mirrors: `dbt/models/marts/facts/fct_menu_item_performance.sql`

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, coalesce, current_timestamp,
    count, countDistinct, sum as spark_sum, round as spark_round, row_number
)
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

df_fct_line_items = spark.table(gold_table("fct_sales_line_item"))
df_dim_menu_item = spark.table(gold_table("dim_menu_item")).filter(col("is_current") == True)

print(f"Loaded {df_dim_menu_item.count()} menu items")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Menu Item Aggregations

# COMMAND ----------

item_metrics = df_fct_line_items.groupBy("menu_item_sk").agg(
    spark_sum("quantity").alias("total_quantity_sold"),
    countDistinct("order_id").alias("total_orders"),
    spark_sum("line_total").alias("total_revenue"),
    spark_sum("food_cost").alias("total_food_cost"),
    spark_sum("gross_profit").alias("total_gross_profit")
)

# Daily average
daily_avg = df_fct_line_items.groupBy("menu_item_sk").agg(
    countDistinct("date_key").alias("days_sold"),
    spark_round(spark_sum("quantity") / countDistinct("date_key"), 2).alias("avg_daily_quantity")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Rankings

# COMMAND ----------

window_rev = Window.orderBy(col("total_revenue").desc())
window_qty = Window.orderBy(col("total_quantity_sold").desc())
window_profit = Window.orderBy(col("total_gross_profit").desc())

ranked_items = item_metrics \
    .withColumn("revenue_rank", row_number().over(window_rev)) \
    .withColumn("quantity_rank", row_number().over(window_qty)) \
    .withColumn("profit_rank", row_number().over(window_profit))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Menu Item Performance Fact

# COMMAND ----------

fct_menu_item_performance = df_dim_menu_item.alias("dm") \
    .join(ranked_items.alias("ri"), col("dm.menu_item_sk") == col("ri.menu_item_sk"), "left") \
    .join(daily_avg.alias("da"), col("dm.menu_item_sk") == col("da.menu_item_sk"), "left") \
    .select(
        col("dm.menu_item_sk"),
        col("dm.item_id"),
        
        # Volume metrics
        coalesce(col("ri.total_quantity_sold"), lit(0)).alias("total_quantity_sold"),
        coalesce(col("ri.total_orders"), lit(0)).alias("total_orders"),
        
        # Revenue metrics
        coalesce(col("ri.total_revenue"), lit(0)).alias("total_revenue"),
        coalesce(col("ri.total_food_cost"), lit(0)).alias("total_food_cost"),
        coalesce(col("ri.total_gross_profit"), lit(0)).alias("total_gross_profit"),
        
        # Calculated metrics
        coalesce(col("da.avg_daily_quantity"), lit(0)).alias("avg_daily_quantity"),
        spark_round(
            coalesce(col("ri.total_revenue"), lit(0)) / 
            when(col("ri.total_quantity_sold") > 0, col("ri.total_quantity_sold")).otherwise(lit(1)),
            2
        ).alias("revenue_per_unit"),
        spark_round(
            coalesce(col("ri.total_gross_profit"), lit(0)) / 
            when(col("ri.total_revenue") > 0, col("ri.total_revenue")).otherwise(lit(1)) * 100,
            2
        ).alias("gross_margin_pct"),
        
        # Rankings
        col("ri.revenue_rank"),
        col("ri.quantity_rank"),
        col("ri.profit_rank"),
        
        # Metadata
        current_timestamp().alias("_created_at"),
        current_timestamp().alias("_updated_at")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold Table

# COMMAND ----------

table_name = gold_table("fct_menu_item_performance")
fct_menu_item_performance.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✓ Written {fct_menu_item_performance.count()} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Data

# COMMAND ----------

display(spark.table(table_name).limit(10))
