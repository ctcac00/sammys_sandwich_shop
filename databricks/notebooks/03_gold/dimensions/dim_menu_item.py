# Databricks notebook source
# MAGIC %md
# MAGIC # Dimension: Menu Item
# MAGIC Includes food cost and margin calculations.
# MAGIC 
# MAGIC Mirrors: `dbt/models/marts/dimensions/dim_menu_item.sql`

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, when, current_timestamp, current_date, md5, coalesce, round as spark_round, sum as spark_sum
)
from pyspark.sql.types import StringType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

df_menu_items = spark.table(silver_table("int_menu_items"))
df_bom = spark.table(silver_table("int_menu_item_ingredients"))
print(f"Loaded {df_menu_items.count()} menu items and {df_bom.count()} BOM records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate Food Cost from Ingredients

# COMMAND ----------

item_costs = df_bom.filter(col("is_optional") == False).groupBy("item_id").agg(
    spark_sum("ingredient_cost").alias("food_cost")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Menu Item Dimension

# COMMAND ----------

dim_menu_item = df_menu_items.alias("mi").join(
    item_costs.alias("ic"),
    col("mi.item_id") == col("ic.item_id"),
    "left"
).select(
    # Surrogate key
    md5(col("mi.item_id").cast(StringType())).alias("menu_item_sk"),
    
    col("mi.item_id"),
    col("mi.item_name"),
    col("mi.category"),
    col("mi.subcategory"),
    col("mi.description"),
    col("mi.base_price"),
    col("mi.price_tier"),
    col("mi.calories"),
    col("mi.calorie_category"),
    col("mi.prep_time_minutes"),
    col("mi.is_active"),
    col("mi.is_seasonal"),
    col("mi.introduced_date"),
    
    # Food cost and margins
    coalesce(col("ic.food_cost"), lit(0)).alias("food_cost"),
    spark_round(coalesce(col("ic.food_cost"), lit(0)) / col("mi.base_price") * 100, 2).alias("food_cost_pct"),
    spark_round(col("mi.base_price") - coalesce(col("ic.food_cost"), lit(0)), 2).alias("gross_margin"),
    spark_round((col("mi.base_price") - coalesce(col("ic.food_cost"), lit(0))) / col("mi.base_price") * 100, 2).alias("gross_margin_pct"),
    
    # SCD Type 1 fields
    current_date().alias("effective_date"),
    lit(True).alias("is_current"),
    
    # Metadata
    current_timestamp().alias("_created_at"),
    current_timestamp().alias("_updated_at")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold Table

# COMMAND ----------

table_name = gold_table("dim_menu_item")
dim_menu_item.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✓ Written {dim_menu_item.count()} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Data

# COMMAND ----------

display(spark.table(table_name).limit(10))
