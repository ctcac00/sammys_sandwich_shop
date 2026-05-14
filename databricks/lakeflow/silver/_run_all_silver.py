# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Silver Layer - Run All
# MAGIC Entry point for running all Silver (staging + enriched) DLT tables.
# MAGIC
# MAGIC This notebook imports all 10 staging and 10 enriched table definitions
# MAGIC into the DLT pipeline context. DLT automatically resolves the dependency graph.
# MAGIC
# MAGIC **Staging tables (10):**
# MAGIC - stg_customers, stg_employees, stg_locations, stg_menu_items, stg_ingredients
# MAGIC - stg_menu_item_ingredients, stg_orders, stg_order_items, stg_suppliers, stg_inventory
# MAGIC
# MAGIC **Enriched tables (10):**
# MAGIC - int_customers, int_employees, int_locations, int_menu_items, int_ingredients
# MAGIC - int_menu_item_ingredients, int_order_items, int_orders, int_suppliers, int_inventory

# COMMAND ----------

# --- Staging Tables ---

# COMMAND ----------

# MAGIC %run ./staging/stg_customers

# COMMAND ----------

# MAGIC %run ./staging/stg_employees

# COMMAND ----------

# MAGIC %run ./staging/stg_locations

# COMMAND ----------

# MAGIC %run ./staging/stg_menu_items

# COMMAND ----------

# MAGIC %run ./staging/stg_ingredients

# COMMAND ----------

# MAGIC %run ./staging/stg_menu_item_ingredients

# COMMAND ----------

# MAGIC %run ./staging/stg_orders

# COMMAND ----------

# MAGIC %run ./staging/stg_order_items

# COMMAND ----------

# MAGIC %run ./staging/stg_suppliers

# COMMAND ----------

# MAGIC %run ./staging/stg_inventory

# COMMAND ----------

# --- Enriched Tables ---

# COMMAND ----------

# MAGIC %run ./enriched/int_customers

# COMMAND ----------

# MAGIC %run ./enriched/int_employees

# COMMAND ----------

# MAGIC %run ./enriched/int_locations

# COMMAND ----------

# MAGIC %run ./enriched/int_menu_items

# COMMAND ----------

# MAGIC %run ./enriched/int_ingredients

# COMMAND ----------

# MAGIC %run ./enriched/int_menu_item_ingredients

# COMMAND ----------

# MAGIC %run ./enriched/int_order_items

# COMMAND ----------

# MAGIC %run ./enriched/int_orders

# COMMAND ----------

# MAGIC %run ./enriched/int_suppliers

# COMMAND ----------

# MAGIC %run ./enriched/int_inventory
