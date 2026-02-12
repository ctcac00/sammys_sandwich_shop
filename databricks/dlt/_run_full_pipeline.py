# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Full Pipeline - Run All Layers
# MAGIC Entry point for running the complete DLT pipeline across all layers.
# MAGIC
# MAGIC This notebook imports all Bronze, Silver, and Gold DLT table/view definitions
# MAGIC into a single DLT pipeline context. DLT automatically resolves the full
# MAGIC dependency graph from sources through to reports.
# MAGIC
# MAGIC **Layers:**
# MAGIC - **Bronze** (10 tables): Raw CSV ingestion
# MAGIC - **Silver** (20 tables): Staging (10) + Enriched (10)
# MAGIC - **Gold** (31 tables/views): Dimensions (9) + Facts (6) + Reports (16)
# MAGIC
# MAGIC **Pipeline configuration options:**
# MAGIC - Point to this file to run the complete pipeline
# MAGIC - Point to `bronze/_run_all_bronze.py` to run just the Bronze layer
# MAGIC - Point to `silver/_run_all_silver.py` to run just the Silver layer
# MAGIC - Point to `gold/_run_all_gold.py` to run just the Gold layer
# MAGIC - Point to individual files for isolated development/testing

# COMMAND ----------

# MAGIC %run ./bronze/_run_all_bronze

# COMMAND ----------

# MAGIC %run ./silver/_run_all_silver

# COMMAND ----------

# MAGIC %run ./gold/_run_all_gold
