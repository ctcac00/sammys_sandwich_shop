# Databricks notebook source
# MAGIC %md
# MAGIC # Staging: Order Items
# MAGIC Performs data type casting and basic cleaning on order line item data.
# MAGIC 
# MAGIC Mirrors: `dbt/models/staging/stg_order_items.sql`

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

from pyspark.sql.functions import col, trim
from pyspark.sql.types import IntegerType, DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Bronze Data

# COMMAND ----------

df = spark.table(bronze_table("order_items"))
print(f"Loaded {df.count()} rows from bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Transformations

# COMMAND ----------

staged = df.select(
    # Primary key
    col("order_item_id"),
    
    # Foreign keys
    trim(col("order_id")).alias("order_id"),
    trim(col("item_id")).alias("item_id"),
    
    # Attributes
    col("quantity").cast(IntegerType()).alias("quantity"),
    col("unit_price").cast(DoubleType()).alias("unit_price"),
    trim(col("customizations")).alias("customizations"),
    col("line_total").cast(DoubleType()).alias("line_total"),
    
    # Metadata
    col("_loaded_at"),
    col("_source_file")
).filter(
    col("order_item_id").isNotNull() & 
    col("order_id").isNotNull()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Table

# COMMAND ----------

table_name = silver_table("stg_order_items")
staged.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✓ Written {staged.count()} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Data

# COMMAND ----------

display(spark.table(table_name).limit(10))
