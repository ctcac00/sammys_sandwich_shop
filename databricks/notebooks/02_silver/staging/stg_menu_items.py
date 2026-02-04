# Databricks notebook source
# MAGIC %md
# MAGIC # Staging: Menu Items
# MAGIC Performs data type casting and basic cleaning on menu item data.
# MAGIC 
# MAGIC Mirrors: `dbt/models/staging/stg_menu_items.sql`

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

from pyspark.sql.functions import col, trim, upper, to_date, when, lit
from pyspark.sql.types import IntegerType, DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Bronze Data

# COMMAND ----------

df = spark.table(bronze_table("menu_items"))
print(f"Loaded {df.count()} rows from bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Transformations

# COMMAND ----------

staged = df.select(
    # Primary key
    col("item_id"),
    
    # Attributes
    trim(col("item_name")).alias("item_name"),
    trim(col("category")).alias("category"),
    trim(col("subcategory")).alias("subcategory"),
    trim(col("description")).alias("description"),
    
    # Pricing
    col("base_price").cast(DoubleType()).alias("base_price"),
    
    # Flags
    when(upper(trim(col("is_active"))).isin("TRUE", "YES", "1"), lit(True))
        .otherwise(lit(False)).alias("is_active"),
    when(upper(trim(col("is_seasonal"))).isin("TRUE", "YES", "1"), lit(True))
        .otherwise(lit(False)).alias("is_seasonal"),
    
    # Nutritional/operational info
    col("calories").cast(IntegerType()).alias("calories"),
    col("prep_time_minutes").cast(IntegerType()).alias("prep_time_minutes"),
    to_date(col("introduced_date"), "yyyy-MM-dd").alias("introduced_date"),
    
    # Metadata
    col("_loaded_at"),
    col("_source_file")
).filter(col("item_id").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Table

# COMMAND ----------

table_name = silver_table("stg_menu_items")
staged.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✓ Written {staged.count()} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Data

# COMMAND ----------

display(spark.table(table_name).limit(10))
