# Databricks notebook source
# MAGIC %md
# MAGIC # Staging: Menu Item Ingredients
# MAGIC Performs data type casting and basic cleaning on recipe/BOM data.
# MAGIC 
# MAGIC Mirrors: `dbt/models/staging/stg_menu_item_ingredients.sql`

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

from pyspark.sql.functions import col, trim, upper, coalesce, when, lit
from pyspark.sql.types import DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Bronze Data

# COMMAND ----------

df = spark.table(bronze_table("menu_item_ingredients"))
print(f"Loaded {df.count()} rows from bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Transformations

# COMMAND ----------

staged = df.select(
    # Composite key
    trim(col("item_id")).alias("item_id"),
    trim(col("ingredient_id")).alias("ingredient_id"),
    
    # Attributes
    col("quantity_required").cast(DoubleType()).alias("quantity_required"),
    when(upper(trim(col("is_optional"))).isin("TRUE", "YES", "1"), lit(True))
        .otherwise(lit(False)).alias("is_optional"),
    coalesce(col("extra_charge").cast(DoubleType()), lit(0.0)).alias("extra_charge"),
    
    # Metadata
    col("_loaded_at"),
    col("_source_file")
).filter(
    col("item_id").isNotNull() & 
    col("ingredient_id").isNotNull()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Table

# COMMAND ----------

table_name = silver_table("stg_menu_item_ingredients")
staged.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✓ Written {staged.count()} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Data

# COMMAND ----------

display(spark.table(table_name).limit(10))
