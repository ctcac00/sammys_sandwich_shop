# Databricks notebook source
# MAGIC %md
# MAGIC # Intermediate: Suppliers
# MAGIC Adds derived fields and business logic enrichments.
# MAGIC 
# MAGIC Mirrors: `dbt/models/intermediate/int_suppliers.sql`

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, when, lower

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Staged Data

# COMMAND ----------

df = spark.table(silver_table("stg_suppliers"))
print(f"Loaded {df.count()} rows from staging")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Enrichments

# COMMAND ----------

enriched = df.select(
    # Original fields
    col("supplier_id"),
    col("supplier_name"),
    col("contact_name"),
    col("email"),
    col("phone"),
    col("address"),
    col("city"),
    col("state"),
    col("zip_code"),
    col("payment_terms"),
    col("lead_time_days"),
    col("is_active"),
    
    # Derived fields - extract payment days from terms
    when(lower(col("payment_terms")).contains("net 10"), 10)
    .when(lower(col("payment_terms")).contains("net 15"), 15)
    .when(lower(col("payment_terms")).contains("net 30"), 30)
    .when(lower(col("payment_terms")).contains("net 45"), 45)
    .when(lower(col("payment_terms")).contains("net 60"), 60)
    .otherwise(30).alias("payment_days"),  # default
    
    # Metadata
    current_timestamp().alias("_enriched_at")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Table

# COMMAND ----------

table_name = silver_table("int_suppliers")
enriched.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✓ Written {enriched.count()} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Data

# COMMAND ----------

display(spark.table(table_name).limit(10))
