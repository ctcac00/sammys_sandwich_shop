# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Configuration
# MAGIC Shared configuration for Sammy's Sandwich Shop Delta Live Tables pipeline.

# COMMAND ----------

# Catalog and Schema Configuration
# Note: In DLT, the target catalog/schema is set at the pipeline level
# These are used for referencing external data sources
CATALOG = "sammys_sandwich_shop"

# Data paths - adjust based on your environment
# Option 1: Unity Catalog Volume (recommended)
DATA_PATH = f"/Volumes/{CATALOG}/raw/csv_files"

# Option 2: DBFS (legacy)
# DATA_PATH = "/dbfs/FileStore/sammys_sandwich_shop/csv_files"

# Option 3: External location (cloud storage)
# DATA_PATH = "abfss://container@storage.dfs.core.windows.net/sammys_sandwich_shop/csv_files"

# COMMAND ----------

# Default values for dimensions (matching dbt vars)
UNKNOWN_CUSTOMER_ID = "UNKNOWN"
UNKNOWN_EMPLOYEE_ID = "UNKNOWN"
UNKNOWN_LOCATION_ID = "UNKNOWN"

# Date range for dim_date generation
DATE_SPINE_START = "2020-01-01"
DATE_SPINE_END = "2030-12-31"

# COMMAND ----------

# CSV file names mapping
CSV_FILES = {
    "customers": "customers.csv",
    "employees": "employees.csv",
    "ingredients": "ingredients.csv",
    "inventory": "inventory.csv",
    "locations": "locations.csv",
    "menu_item_ingredients": "menu_item_ingredients.csv",
    "menu_items": "menu_items.csv",
    "order_items": "order_items.csv",
    "orders": "orders.csv",
    "suppliers": "suppliers.csv",
}
