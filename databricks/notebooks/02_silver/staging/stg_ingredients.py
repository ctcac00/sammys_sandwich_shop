# Databricks notebook source
# MAGIC %md
# MAGIC # Staging: Ingredients
# MAGIC Performs data type casting and basic cleaning on ingredient data.
# MAGIC 
# MAGIC Mirrors: `dbt/models/staging/stg_ingredients.sql`

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

from pyspark.sql.functions import col, trim, upper, when, lit
from pyspark.sql.types import IntegerType, DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Bronze Data

# COMMAND ----------

df = spark.table(bronze_table("ingredients"))
print(f"Loaded {df.count()} rows from bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Transformations

# COMMAND ----------

staged = df.select(
    # Primary key
    col("ingredient_id"),
    
    # Attributes
    trim(col("ingredient_name")).alias("ingredient_name"),
    trim(col("category")).alias("category"),
    trim(col("unit_of_measure")).alias("unit_of_measure"),
    col("cost_per_unit").cast(DoubleType()).alias("cost_per_unit"),
    trim(col("supplier_id")).alias("supplier_id"),
    
    # Allergen info
    when(upper(trim(col("is_allergen"))).isin("TRUE", "YES", "1"), lit(True))
        .otherwise(lit(False)).alias("is_allergen"),
    trim(col("allergen_type")).alias("allergen_type"),
    
    # Storage info
    col("shelf_life_days").cast(IntegerType()).alias("shelf_life_days"),
    trim(col("storage_type")).alias("storage_type"),
    
    # Metadata
    col("_loaded_at"),
    col("_source_file")
).filter(col("ingredient_id").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Table

# COMMAND ----------

table_name = silver_table("stg_ingredients")
staged.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✓ Written {staged.count()} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Data

# COMMAND ----------

display(spark.table(table_name).limit(10))
