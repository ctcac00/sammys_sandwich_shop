# Databricks notebook source
# MAGIC %md
# MAGIC # Fact: Inventory Snapshot
# MAGIC Grain: one row per location/ingredient/day.
# MAGIC 
# MAGIC Mirrors: `dbt/models/marts/facts/fct_inventory_snapshot.sql`

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, when, coalesce, current_timestamp, year, month, dayofmonth,
    round as spark_round
)
from pyspark.sql.types import IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

df_inventory = spark.table(silver_table("int_inventory"))
df_dim_location = spark.table(gold_table("dim_location")).filter(col("is_current") == True)
df_dim_ingredient = spark.table(gold_table("dim_ingredient")).filter(col("is_current") == True)

print(f"Loaded {df_inventory.count()} inventory records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Fact Inventory

# COMMAND ----------

fct_inventory = df_inventory.alias("i") \
    .join(df_dim_location.alias("dl"), col("i.location_id") == col("dl.location_id"), "left") \
    .join(df_dim_ingredient.alias("di"), col("i.ingredient_id") == col("di.ingredient_id"), "left") \
    .select(
        # Keys
        col("i.inventory_id").alias("inventory_snapshot_id"),
        (year(col("i.snapshot_date")) * 10000 + month(col("i.snapshot_date")) * 100 + dayofmonth(col("i.snapshot_date"))).cast(IntegerType()).alias("date_key"),
        col("dl.location_sk"),
        col("di.ingredient_sk"),
        
        # Measures
        col("i.quantity_on_hand"),
        col("i.quantity_reserved"),
        col("i.quantity_available"),
        col("i.reorder_point"),
        col("i.days_until_expiration"),
        
        # Calculated measures
        spark_round(col("i.quantity_on_hand") * col("di.cost_per_unit"), 2).alias("stock_value"),
        when(col("i.reorder_quantity") > 0,
            spark_round(col("i.quantity_available") / col("i.reorder_quantity") * 7, 2)
        ).otherwise(lit(None)).alias("days_of_supply"),
        
        # Status indicators
        col("i.needs_reorder"),
        col("i.stock_status"),
        when(col("i.days_until_expiration") < 0, "Expired")
        .when(col("i.days_until_expiration") <= 3, "Critical")
        .when(col("i.days_until_expiration") <= 7, "Warning")
        .otherwise("OK").alias("expiration_risk"),
        
        # Metadata
        current_timestamp().alias("_created_at")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold Table

# COMMAND ----------

table_name = gold_table("fct_inventory_snapshot")
fct_inventory.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✓ Written {fct_inventory.count()} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Data

# COMMAND ----------

display(spark.table(table_name).limit(10))
