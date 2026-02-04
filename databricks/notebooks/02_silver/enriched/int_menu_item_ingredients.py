# Databricks notebook source
# MAGIC %md
# MAGIC # Intermediate: Menu Item Ingredients
# MAGIC Adds ingredient cost calculations for recipe/BOM.
# MAGIC 
# MAGIC Mirrors: `dbt/models/intermediate/int_menu_item_ingredients.sql`

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, round as spark_round

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Staged Data

# COMMAND ----------

df_bom = spark.table(silver_table("stg_menu_item_ingredients"))
df_ingredients = spark.table(silver_table("int_ingredients"))
print(f"Loaded {df_bom.count()} BOM records and {df_ingredients.count()} ingredients")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Enrichments

# COMMAND ----------

enriched = df_bom.alias("bom").join(
    df_ingredients.alias("i"),
    col("bom.ingredient_id") == col("i.ingredient_id"),
    "left"
).select(
    # Original BOM fields
    col("bom.item_id"),
    col("bom.ingredient_id"),
    col("bom.quantity_required"),
    col("bom.is_optional"),
    col("bom.extra_charge"),
    
    # Calculate ingredient cost based on quantity
    spark_round(col("bom.quantity_required") * col("i.cost_per_unit"), 4).alias("ingredient_cost"),
    
    # Ingredient details for reference
    col("i.ingredient_name"),
    col("i.category").alias("ingredient_category"),
    col("i.unit_of_measure"),
    col("i.cost_per_unit"),
    
    # Metadata
    current_timestamp().alias("_enriched_at")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Table

# COMMAND ----------

table_name = silver_table("int_menu_item_ingredients")
enriched.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✓ Written {enriched.count()} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Data

# COMMAND ----------

display(spark.table(table_name).limit(10))
