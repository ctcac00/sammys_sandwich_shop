# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Order Items
# MAGIC Loads raw order item data from CSV into a Bronze Delta Live Table.

# COMMAND ----------

import dlt
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

@dlt.table(
    name="bronze_order_items",
    comment="Raw order item data from CSV source",
    table_properties={"quality": "bronze"}
)
def bronze_order_items():
    return (
        spark.read.csv(f"{DATA_PATH}/{CSV_FILES['order_items']}", header=True, inferSchema=True)
        .withColumn("_loaded_at", current_timestamp())
        .withColumn("_source_file", lit(CSV_FILES['order_items']))
    )
