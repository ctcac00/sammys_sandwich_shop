# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Gold Layer - Run All
# MAGIC Entry point for running all Gold (dimensions, facts, reports) DLT tables and views.
# MAGIC
# MAGIC This notebook imports the shared configuration and all dimension, fact, and report
# MAGIC definitions into the DLT pipeline context. DLT automatically resolves the dependency graph.
# MAGIC
# MAGIC **Dimensions (9):**
# MAGIC - dim_date, dim_time, dim_payment_method, dim_order_type
# MAGIC - dim_customer, dim_employee, dim_location, dim_ingredient, dim_menu_item
# MAGIC
# MAGIC **Facts (6):**
# MAGIC - fct_sales, fct_sales_line_item, fct_daily_summary
# MAGIC - fct_inventory_snapshot, fct_customer_activity, fct_menu_item_performance
# MAGIC
# MAGIC **Reports (16):**
# MAGIC - rpt_daily_sales_summary, rpt_weekly_sales_summary, rpt_company_daily_totals
# MAGIC - rpt_customer_overview, rpt_rfm_segment_summary, rpt_top_customers
# MAGIC - rpt_customers_at_risk, rpt_loyalty_tier_analysis, rpt_top_selling_items
# MAGIC - rpt_category_performance, rpt_menu_item_profitability
# MAGIC - rpt_location_performance, rpt_location_ranking
# MAGIC - rpt_inventory_status, rpt_inventory_alerts, rpt_inventory_value_by_location

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# --- Dimension Tables ---

# COMMAND ----------

# MAGIC %run ./dimensions/dim_date

# COMMAND ----------

# MAGIC %run ./dimensions/dim_time

# COMMAND ----------

# MAGIC %run ./dimensions/dim_payment_method

# COMMAND ----------

# MAGIC %run ./dimensions/dim_order_type

# COMMAND ----------

# MAGIC %run ./dimensions/dim_customer

# COMMAND ----------

# MAGIC %run ./dimensions/dim_employee

# COMMAND ----------

# MAGIC %run ./dimensions/dim_location

# COMMAND ----------

# MAGIC %run ./dimensions/dim_ingredient

# COMMAND ----------

# MAGIC %run ./dimensions/dim_menu_item

# COMMAND ----------

# --- Fact Tables ---

# COMMAND ----------

# MAGIC %run ./facts/fct_sales

# COMMAND ----------

# MAGIC %run ./facts/fct_sales_line_item

# COMMAND ----------

# MAGIC %run ./facts/fct_daily_summary

# COMMAND ----------

# MAGIC %run ./facts/fct_inventory_snapshot

# COMMAND ----------

# MAGIC %run ./facts/fct_customer_activity

# COMMAND ----------

# MAGIC %run ./facts/fct_menu_item_performance

# COMMAND ----------

# --- Report Views ---

# COMMAND ----------

# MAGIC %run ./reports/rpt_daily_sales_summary

# COMMAND ----------

# MAGIC %run ./reports/rpt_weekly_sales_summary

# COMMAND ----------

# MAGIC %run ./reports/rpt_company_daily_totals

# COMMAND ----------

# MAGIC %run ./reports/rpt_customer_overview

# COMMAND ----------

# MAGIC %run ./reports/rpt_rfm_segment_summary

# COMMAND ----------

# MAGIC %run ./reports/rpt_top_customers

# COMMAND ----------

# MAGIC %run ./reports/rpt_customers_at_risk

# COMMAND ----------

# MAGIC %run ./reports/rpt_loyalty_tier_analysis

# COMMAND ----------

# MAGIC %run ./reports/rpt_top_selling_items

# COMMAND ----------

# MAGIC %run ./reports/rpt_category_performance

# COMMAND ----------

# MAGIC %run ./reports/rpt_menu_item_profitability

# COMMAND ----------

# MAGIC %run ./reports/rpt_location_performance

# COMMAND ----------

# MAGIC %run ./reports/rpt_location_ranking

# COMMAND ----------

# MAGIC %run ./reports/rpt_inventory_status

# COMMAND ----------

# MAGIC %run ./reports/rpt_inventory_alerts

# COMMAND ----------

# MAGIC %run ./reports/rpt_inventory_value_by_location
