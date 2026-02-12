# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Bronze Layer - Run All
# MAGIC Entry point for running all Bronze (raw ingestion) DLT tables.
# MAGIC
# MAGIC This notebook imports the shared configuration and all 10 bronze table definitions
# MAGIC into the DLT pipeline context. DLT automatically resolves the dependency graph.
# MAGIC
# MAGIC **Tables included:**
# MAGIC - bronze_customers
# MAGIC - bronze_employees
# MAGIC - bronze_locations
# MAGIC - bronze_menu_items
# MAGIC - bronze_ingredients
# MAGIC - bronze_menu_item_ingredients
# MAGIC - bronze_orders
# MAGIC - bronze_order_items
# MAGIC - bronze_suppliers
# MAGIC - bronze_inventory

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# MAGIC %run ./bronze_customers

# COMMAND ----------

# MAGIC %run ./bronze_employees

# COMMAND ----------

# MAGIC %run ./bronze_locations

# COMMAND ----------

# MAGIC %run ./bronze_menu_items

# COMMAND ----------

# MAGIC %run ./bronze_ingredients

# COMMAND ----------

# MAGIC %run ./bronze_menu_item_ingredients

# COMMAND ----------

# MAGIC %run ./bronze_orders

# COMMAND ----------

# MAGIC %run ./bronze_order_items

# COMMAND ----------

# MAGIC %run ./bronze_suppliers

# COMMAND ----------

# MAGIC %run ./bronze_inventory
