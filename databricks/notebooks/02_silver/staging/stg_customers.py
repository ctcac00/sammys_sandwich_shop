# Databricks notebook source
# MAGIC %md
# MAGIC # Staging: Customers
# MAGIC Performs data type casting and basic cleaning on customer data.
# MAGIC 
# MAGIC Mirrors: `dbt/models/staging/stg_customers.sql`

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

from pyspark.sql.functions import (
    col, trim, lower, upper, initcap, to_date, coalesce, lit, when
)
from pyspark.sql.types import IntegerType, DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Bronze Data

# COMMAND ----------

df = spark.table(bronze_table("customers"))
print(f"Loaded {df.count()} rows from bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Transformations

# COMMAND ----------

staged = df.select(
    # Primary key
    col("customer_id"),
    
    # Name fields
    initcap(trim(col("first_name"))).alias("first_name"),
    initcap(trim(col("last_name"))).alias("last_name"),
    
    # Contact info
    lower(trim(col("email"))).alias("email"),
    trim(col("phone")).alias("phone"),
    trim(col("address")).alias("address"),
    trim(col("city")).alias("city"),
    upper(trim(col("state"))).alias("state"),
    trim(col("zip_code")).alias("zip_code"),
    
    # Dates
    to_date(col("birth_date"), "yyyy-MM-dd").alias("birth_date"),
    to_date(col("signup_date"), "yyyy-MM-dd").alias("signup_date"),
    
    # Loyalty program
    coalesce(trim(col("loyalty_tier")), lit("Bronze")).alias("loyalty_tier"),
    coalesce(col("loyalty_points").cast(IntegerType()), lit(0)).alias("loyalty_points"),
    trim(col("preferred_location")).alias("preferred_location_id"),
    
    # Boolean conversion
    when(upper(trim(col("marketing_opt_in"))).isin("TRUE", "YES", "1"), lit(True))
        .otherwise(lit(False)).alias("marketing_opt_in"),
    
    # Metadata
    col("_loaded_at"),
    col("_source_file")
).filter(col("customer_id").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Table

# COMMAND ----------

table_name = silver_table("stg_customers")
staged.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✓ Written {staged.count()} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Data

# COMMAND ----------

display(spark.table(table_name).limit(10))
