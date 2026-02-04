# Databricks notebook source
# MAGIC %md
# MAGIC # Intermediate: Customers
# MAGIC Adds derived fields and business logic enrichments.
# MAGIC 
# MAGIC Mirrors: `dbt/models/intermediate/int_customers.sql`

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

from pyspark.sql.functions import (
    col, concat_ws, datediff, current_date, current_timestamp, floor, when, upper
)
from pyspark.sql.types import IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Staged Data

# COMMAND ----------

df = spark.table(silver_table("stg_customers"))
print(f"Loaded {df.count()} rows from staging")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Enrichments

# COMMAND ----------

enriched = df.select(
    # Original fields
    col("customer_id"),
    col("first_name"),
    col("last_name"),
    concat_ws(" ", col("first_name"), col("last_name")).alias("full_name"),
    col("email"),
    col("phone"),
    col("address"),
    col("city"),
    col("state"),
    col("zip_code"),
    col("birth_date"),
    col("signup_date"),
    col("loyalty_tier"),
    col("loyalty_points"),
    col("preferred_location_id"),
    col("marketing_opt_in"),
    
    # Derived fields - Age calculation
    floor(datediff(current_date(), col("birth_date")) / 365.25).cast(IntegerType()).alias("age"),
    
    # Loyalty tier rank for sorting
    when(upper(col("loyalty_tier")) == "PLATINUM", 4)
    .when(upper(col("loyalty_tier")) == "GOLD", 3)
    .when(upper(col("loyalty_tier")) == "SILVER", 2)
    .when(upper(col("loyalty_tier")) == "BRONZE", 1)
    .otherwise(0).alias("loyalty_tier_rank"),
    
    # Customer tenure
    datediff(current_date(), col("signup_date")).alias("customer_tenure_days"),
    
    # Metadata
    current_timestamp().alias("_enriched_at")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Table

# COMMAND ----------

table_name = silver_table("int_customers")
enriched.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✓ Written {enriched.count()} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Data

# COMMAND ----------

display(spark.table(table_name).limit(10))
