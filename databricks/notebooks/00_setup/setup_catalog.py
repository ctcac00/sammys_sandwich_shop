# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Catalog and Schemas
# MAGIC Creates the Unity Catalog structure for Sammy's Sandwich Shop.
# MAGIC 
# MAGIC **Catalog**: sammys_sandwich_shop
# MAGIC - **bronze**: Raw data loaded from CSV files
# MAGIC - **silver**: Cleaned and enriched data
# MAGIC - **gold**: Dimensional model (dimensions, facts, reports)

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"USE CATALOG {CATALOG}")
print(f"✓ Catalog '{CATALOG}' ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Schemas

# COMMAND ----------

# Bronze schema - raw data
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA} COMMENT 'Raw data loaded from CSV sources'")
print(f"✓ Schema '{BRONZE_SCHEMA}' ready")

# Silver schema - staging and enriched data
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA} COMMENT 'Cleaned and enriched data'")
print(f"✓ Schema '{SILVER_SCHEMA}' ready")

# Gold schema - dimensional model
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA} COMMENT 'Dimensional model - dimensions, facts, and reports'")
print(f"✓ Schema '{GOLD_SCHEMA}' ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Volume for CSV Files (Optional)
# MAGIC 
# MAGIC If you want to use Unity Catalog Volumes to store CSV files:

# COMMAND ----------

# Uncomment to create a volume for CSV files
# spark.sql(f"""
#     CREATE VOLUME IF NOT EXISTS {CATALOG}.raw.csv_files
#     COMMENT 'Source CSV files for Sammy''s Sandwich Shop'
# """)
# print(f"✓ Volume '{CATALOG}.raw.csv_files' ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Setup

# COMMAND ----------

# List all schemas in the catalog
print(f"\nSchemas in {CATALOG}:")
schemas = spark.sql(f"SHOW SCHEMAS IN {CATALOG}").collect()
for schema in schemas:
    print(f"  - {schema.databaseName}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Complete
# MAGIC 
# MAGIC The catalog and schemas are now ready. Next steps:
# MAGIC 1. Upload CSV files to your configured data path
# MAGIC 2. Run the Bronze layer notebooks to load data
