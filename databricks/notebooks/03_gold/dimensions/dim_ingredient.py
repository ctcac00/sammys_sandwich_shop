# Databricks notebook source
# MAGIC %md
# MAGIC # Dimension: Ingredient
# MAGIC Ingredient dimension with supplier information.
# MAGIC 
# MAGIC Mirrors: `dbt/models/marts/dimensions/dim_ingredient.sql`

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, current_timestamp, current_date, md5
)
from pyspark.sql.types import StringType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

df_ingredients = spark.table(silver_table("int_ingredients"))
df_suppliers = spark.table(silver_table("int_suppliers"))
print(f"Loaded {df_ingredients.count()} ingredients and {df_suppliers.count()} suppliers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Ingredient Dimension

# COMMAND ----------

dim_ingredient = df_ingredients.alias("i").join(
    df_suppliers.select(
        col("supplier_id"),
        col("supplier_name")
    ).alias("s"),
    col("i.supplier_id") == col("s.supplier_id"),
    "left"
).select(
    # Surrogate key
    md5(col("i.ingredient_id").cast(StringType())).alias("ingredient_sk"),
    
    col("i.ingredient_id"),
    col("i.ingredient_name"),
    col("i.category"),
    col("i.unit_of_measure"),
    col("i.cost_per_unit"),
    col("i.cost_tier"),
    col("i.supplier_id"),
    col("s.supplier_name"),
    col("i.is_allergen"),
    col("i.allergen_type"),
    col("i.shelf_life_days"),
    col("i.storage_type"),
    
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

table_name = gold_table("dim_ingredient")
dim_ingredient.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✓ Written {dim_ingredient.count()} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Data

# COMMAND ----------

display(spark.table(table_name).limit(10))
