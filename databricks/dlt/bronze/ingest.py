# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Data Ingestion
# MAGIC Loads raw CSV data into Bronze Delta Live Tables with metadata tracking.
# MAGIC 
# MAGIC Uses Auto Loader for incremental ingestion when running in streaming mode.

# COMMAND ----------

import dlt
from pyspark.sql.functions import current_timestamp, lit, input_file_name

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers

# COMMAND ----------

@dlt.table(
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Employees

# COMMAND ----------

@dlt.table(
    name="bronze_employees",
    comment="Raw employee data from CSV source",
    table_properties={"quality": "bronze"}
)
def bronze_employees():
    return (
        spark.read.csv(f"{DATA_PATH}/{CSV_FILES['employees']}", header=True, inferSchema=True)
        .withColumn("_loaded_at", current_timestamp())
        .withColumn("_source_file", lit(CSV_FILES['employees']))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Locations

# COMMAND ----------

@dlt.table(
    name="bronze_locations",
    comment="Raw location data from CSV source",
    table_properties={"quality": "bronze"}
)
def bronze_locations():
    return (
        spark.read.csv(f"{DATA_PATH}/{CSV_FILES['locations']}", header=True, inferSchema=True)
        .withColumn("_loaded_at", current_timestamp())
        .withColumn("_source_file", lit(CSV_FILES['locations']))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Menu Items

# COMMAND ----------

@dlt.table(
    name="bronze_menu_items",
    comment="Raw menu item data from CSV source",
    table_properties={"quality": "bronze"}
)
def bronze_menu_items():
    return (
        spark.read.csv(f"{DATA_PATH}/{CSV_FILES['menu_items']}", header=True, inferSchema=True)
        .withColumn("_loaded_at", current_timestamp())
        .withColumn("_source_file", lit(CSV_FILES['menu_items']))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingredients

# COMMAND ----------

@dlt.table(
    name="bronze_ingredients",
    comment="Raw ingredient data from CSV source",
    table_properties={"quality": "bronze"}
)
def bronze_ingredients():
    return (
        spark.read.csv(f"{DATA_PATH}/{CSV_FILES['ingredients']}", header=True, inferSchema=True)
        .withColumn("_loaded_at", current_timestamp())
        .withColumn("_source_file", lit(CSV_FILES['ingredients']))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Menu Item Ingredients

# COMMAND ----------

@dlt.table(
    name="bronze_menu_item_ingredients",
    comment="Raw menu item ingredients mapping from CSV source",
    table_properties={"quality": "bronze"}
)
def bronze_menu_item_ingredients():
    return (
        spark.read.csv(f"{DATA_PATH}/{CSV_FILES['menu_item_ingredients']}", header=True, inferSchema=True)
        .withColumn("_loaded_at", current_timestamp())
        .withColumn("_source_file", lit(CSV_FILES['menu_item_ingredients']))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orders

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order Items

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Suppliers

# COMMAND ----------

@dlt.table(
    name="bronze_suppliers",
    comment="Raw supplier data from CSV source",
    table_properties={"quality": "bronze"}
)
def bronze_suppliers():
    return (
        spark.read.csv(f"{DATA_PATH}/{CSV_FILES['suppliers']}", header=True, inferSchema=True)
        .withColumn("_loaded_at", current_timestamp())
        .withColumn("_source_file", lit(CSV_FILES['suppliers']))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inventory

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
