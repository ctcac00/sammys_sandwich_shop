# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Inventory
# MAGIC Loads raw inventory data from CSV into a Bronze Delta Live Table.

# COMMAND ----------

import dlt
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

@dlt.table(
    name="bronze_inventory",
    comment="Raw inventory data from CSV source",
    table_properties={"quality": "bronze"}
)
def bronze_inventory():
    return (
        spark.read.csv(f"{DATA_PATH}/{CSV_FILES['inventory']}", header=True, inferSchema=True)
        .withColumn("_loaded_at", current_timestamp())
        .withColumn("_source_file", lit(CSV_FILES['inventory']))
    )
