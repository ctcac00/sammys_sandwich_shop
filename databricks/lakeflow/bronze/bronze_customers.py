# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Customers
# MAGIC Loads raw customer data from CSV into a Bronze Delta Live Table.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

@dp.materialized_view(
    name="bronze_customers",
    comment="Raw customer data from CSV source",
    table_properties={"quality": "bronze"}
)
def bronze_customers():
    return (
        spark.read.csv(f"{DATA_PATH}/{CSV_FILES['customers']}", header=True, inferSchema=True)
        .withColumn("_loaded_at", current_timestamp())
        .withColumn("_source_file", lit(CSV_FILES['customers']))
    )
