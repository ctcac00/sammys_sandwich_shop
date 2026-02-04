# Databricks notebook source
# MAGIC %md
# MAGIC # Intermediate: Inventory
# MAGIC Adds derived fields and business logic enrichments.
# MAGIC 
# MAGIC Mirrors: `dbt/models/intermediate/int_inventory.sql`

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, datediff, current_date, when

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Staged Data

# COMMAND ----------

df = spark.table(silver_table("stg_inventory"))
print(f"Loaded {df.count()} rows from staging")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Enrichments

# COMMAND ----------

enriched = df.select(
    # Original fields
    col("inventory_id"),
    col("location_id"),
    col("ingredient_id"),
    col("snapshot_date"),
    col("quantity_on_hand"),
    col("quantity_reserved"),
    col("reorder_point"),
    col("reorder_quantity"),
    col("last_restock_date"),
    col("expiration_date"),
    
    # Derived fields
    (col("quantity_on_hand") - col("quantity_reserved")).alias("quantity_available"),
    datediff(col("expiration_date"), current_date()).alias("days_until_expiration"),
    (col("quantity_on_hand") <= col("reorder_point")).alias("needs_reorder"),
    
    # Stock status
    when(col("quantity_on_hand") == 0, "Out of Stock")
    .when(col("quantity_on_hand") <= col("reorder_point"), "Low Stock")
    .when(col("quantity_on_hand") <= col("reorder_point") * 2, "Adequate")
    .otherwise("Well Stocked").alias("stock_status"),
    
    # Metadata
    current_timestamp().alias("_enriched_at")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Table

# COMMAND ----------

table_name = silver_table("int_inventory")
enriched.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✓ Written {enriched.count()} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Data

# COMMAND ----------

display(spark.table(table_name).limit(10))
