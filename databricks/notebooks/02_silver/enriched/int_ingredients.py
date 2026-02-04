# Databricks notebook source
# MAGIC %md
# MAGIC # Intermediate: Ingredients
# MAGIC Adds derived fields and business logic enrichments.
# MAGIC 
# MAGIC Mirrors: `dbt/models/intermediate/int_ingredients.sql`

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, when

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Staged Data

# COMMAND ----------

df = spark.table(silver_table("stg_ingredients"))
print(f"Loaded {df.count()} rows from staging")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Enrichments

# COMMAND ----------

enriched = df.select(
    # Original fields
    col("ingredient_id"),
    col("ingredient_name"),
    col("category"),
    col("unit_of_measure"),
    col("cost_per_unit"),
    col("supplier_id"),
    col("is_allergen"),
    col("allergen_type"),
    col("shelf_life_days"),
    col("storage_type"),
    
    # Derived fields - cost tier
    when(col("cost_per_unit") < 0.50, "Low Cost")
    .when(col("cost_per_unit") < 2.00, "Medium Cost")
    .when(col("cost_per_unit") < 5.00, "High Cost")
    .otherwise("Premium").alias("cost_tier"),
    
    # Metadata
    current_timestamp().alias("_enriched_at")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Table

# COMMAND ----------

table_name = silver_table("int_ingredients")
enriched.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✓ Written {enriched.count()} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Data

# COMMAND ----------

display(spark.table(table_name).limit(10))
