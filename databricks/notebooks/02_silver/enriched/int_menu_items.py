# Databricks notebook source
# MAGIC %md
# MAGIC # Intermediate: Menu Items
# MAGIC Adds derived fields and business logic enrichments.
# MAGIC 
# MAGIC Mirrors: `dbt/models/intermediate/int_menu_items.sql`

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

from pyspark.sql.functions import (
    col, datediff, current_date, current_timestamp, when
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Staged Data

# COMMAND ----------

df = spark.table(silver_table("stg_menu_items"))
print(f"Loaded {df.count()} rows from staging")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Enrichments

# COMMAND ----------

enriched = df.select(
    # Original fields
    col("item_id"),
    col("item_name"),
    col("category"),
    col("subcategory"),
    col("description"),
    col("base_price"),
    col("is_active"),
    col("is_seasonal"),
    col("calories"),
    col("prep_time_minutes"),
    col("introduced_date"),
    
    # Derived fields - days on menu
    datediff(current_date(), col("introduced_date")).alias("days_on_menu"),
    
    # Price tier
    when(col("base_price") < 5, "Budget")
    .when(col("base_price") < 8, "Standard")
    .when(col("base_price") < 12, "Premium")
    .otherwise("Gourmet").alias("price_tier"),
    
    # Calorie category
    when(col("calories") < 300, "Light")
    .when(col("calories") < 500, "Moderate")
    .when(col("calories") < 700, "Hearty")
    .otherwise("Indulgent").alias("calorie_category"),
    
    # Metadata
    current_timestamp().alias("_enriched_at")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Table

# COMMAND ----------

table_name = silver_table("int_menu_items")
enriched.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✓ Written {enriched.count()} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Data

# COMMAND ----------

display(spark.table(table_name).limit(10))
