# Databricks notebook source
# MAGIC %md
# MAGIC # Staging: Inventory
# MAGIC Performs data type casting and basic cleaning on inventory snapshot data.
# MAGIC 
# MAGIC Mirrors: `dbt/models/staging/stg_inventory.sql`

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

from pyspark.sql.functions import col, trim, to_date, coalesce, lit
from pyspark.sql.types import IntegerType, DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Bronze Data

# COMMAND ----------

df = spark.table(bronze_table("inventory"))
print(f"Loaded {df.count()} rows from bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Transformations

# COMMAND ----------

staged = df.select(
    # Primary key
    col("inventory_id"),
    
    # Foreign keys
    trim(col("location_id")).alias("location_id"),
    trim(col("ingredient_id")).alias("ingredient_id"),
    
    # Dates
    to_date(col("snapshot_date"), "yyyy-MM-dd").alias("snapshot_date"),
    to_date(col("last_restock_date"), "yyyy-MM-dd").alias("last_restock_date"),
    to_date(col("expiration_date"), "yyyy-MM-dd").alias("expiration_date"),
    
    # Quantities
    coalesce(col("quantity_on_hand").cast(DoubleType()), lit(0.0)).alias("quantity_on_hand"),
    coalesce(col("quantity_reserved").cast(DoubleType()), lit(0.0)).alias("quantity_reserved"),
    coalesce(col("reorder_point").cast(DoubleType()), lit(0.0)).alias("reorder_point"),
    coalesce(col("reorder_quantity").cast(DoubleType()), lit(0.0)).alias("reorder_quantity"),
    
    # Metadata
    col("_loaded_at"),
    col("_source_file")
).filter(col("inventory_id").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Table

# COMMAND ----------

table_name = silver_table("stg_inventory")
staged.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✓ Written {staged.count()} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Data

# COMMAND ----------

display(spark.table(table_name).limit(10))
