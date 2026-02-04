# Databricks notebook source
# MAGIC %md
# MAGIC # Configuration
# MAGIC Shared configuration for Sammy's Sandwich Shop Databricks project.
# MAGIC This notebook defines catalog, schema names, and data paths used across all notebooks.

# COMMAND ----------

# Catalog and Schema Configuration
CATALOG = "sammys_sandwich_shop"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# Data paths - adjust based on your environment
# Option 1: Unity Catalog Volume
DATA_PATH = f"/Volumes/{CATALOG}/raw/csv_files"

# Option 2: DBFS (legacy)
# DATA_PATH = "/dbfs/FileStore/sammys_sandwich_shop/csv_files"

# Option 3: External location (cloud storage)
# DATA_PATH = "abfss://container@storage.dfs.core.windows.net/sammys_sandwich_shop/csv_files"

# COMMAND ----------

# Table name helpers
def bronze_table(name: str) -> str:
    """Get fully qualified bronze table name."""
    return f"{CATALOG}.{BRONZE_SCHEMA}.{name}"

def silver_table(name: str) -> str:
    """Get fully qualified silver table name."""
    return f"{CATALOG}.{SILVER_SCHEMA}.{name}"

def gold_table(name: str) -> str:
    """Get fully qualified gold table name."""
    return f"{CATALOG}.{GOLD_SCHEMA}.{name}"

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
