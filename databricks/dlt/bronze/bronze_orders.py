# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Orders
# MAGIC Loads raw order data from CSV into a Bronze Delta Live Table.

# COMMAND ----------

import dlt
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

@dlt.table(
    name="bronze_orders",
    comment="Raw order data from CSV source",
    table_properties={"quality": "bronze"}
)
def bronze_orders():
    return (
        spark.read.csv(f"{DATA_PATH}/{CSV_FILES['orders']}", header=True, inferSchema=True)
        .withColumn("_loaded_at", current_timestamp())
        .withColumn("_source_file", lit(CSV_FILES['orders']))
    )
