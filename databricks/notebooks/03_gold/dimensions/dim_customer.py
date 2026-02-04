# Databricks notebook source
# MAGIC %md
# MAGIC # Dimension: Customer
# MAGIC SCD Type 1 implementation (current state only).
# MAGIC 
# MAGIC Mirrors: `dbt/models/marts/dimensions/dim_customer.sql`

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, when, current_timestamp, current_date, floor, md5, concat_ws
)
from pyspark.sql.types import StringType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Enriched Data

# COMMAND ----------

df = spark.table(silver_table("int_customers"))
print(f"Loaded {df.count()} customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Customer Dimension

# COMMAND ----------

customer_dimension = df.select(
    col("customer_id"),
    col("first_name"),
    col("last_name"),
    col("full_name"),
    col("email"),
    col("phone"),
    col("city"),
    col("state"),
    col("zip_code"),
    
    # Age grouping
    when(col("age") < 18, "Under 18")
    .when(col("age").between(18, 24), "18-24")
    .when(col("age").between(25, 34), "25-34")
    .when(col("age").between(35, 44), "35-44")
    .when(col("age").between(45, 54), "45-54")
    .when(col("age").between(55, 64), "55-64")
    .otherwise("65+").alias("age_group"),
    
    col("loyalty_tier"),
    col("loyalty_tier_rank"),
    col("signup_date"),
    
    # Tenure in months
    floor(col("customer_tenure_days") / 30.0).alias("tenure_months"),
    
    # Tenure grouping
    when(col("customer_tenure_days") < 90, "New (0-3 mo)")
    .when(col("customer_tenure_days") < 365, "Growing (3-12 mo)")
    .when(col("customer_tenure_days") < 730, "Established (1-2 yr)")
    .otherwise("Loyal (2+ yr)").alias("tenure_group"),
    
    col("marketing_opt_in"),
    col("preferred_location_id"),
    
    # SCD Type 1 fields
    current_date().alias("effective_date"),
    lit(True).alias("is_current")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Unknown Customer Record

# COMMAND ----------

unknown_customer = spark.createDataFrame([{
    "customer_id": UNKNOWN_CUSTOMER_ID,
    "first_name": "Guest",
    "last_name": "Customer",
    "full_name": "Guest Customer",
    "email": None,
    "phone": None,
    "city": None,
    "state": None,
    "zip_code": None,
    "age_group": "Unknown",
    "loyalty_tier": "None",
    "loyalty_tier_rank": 0,
    "signup_date": None,
    "tenure_months": 0,
    "tenure_group": "Unknown",
    "marketing_opt_in": False,
    "preferred_location_id": None,
    "effective_date": "1900-01-01",
    "is_current": True
}])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combine and Add Surrogate Key

# COMMAND ----------

from pyspark.sql.functions import to_date as spark_to_date

final = customer_dimension.union(
    unknown_customer.select(
        col("customer_id"),
        col("first_name"),
        col("last_name"),
        col("full_name"),
        col("email"),
        col("phone"),
        col("city"),
        col("state"),
        col("zip_code"),
        col("age_group"),
        col("loyalty_tier"),
        col("loyalty_tier_rank").cast("int"),
        spark_to_date(col("signup_date")).alias("signup_date"),
        col("tenure_months").cast("long"),
        col("tenure_group"),
        col("marketing_opt_in"),
        col("preferred_location_id"),
        spark_to_date(col("effective_date")).alias("effective_date"),
        col("is_current")
    )
)

# Add surrogate key and metadata
dim_customer = final.select(
    md5(col("customer_id").cast(StringType())).alias("customer_sk"),
    "*",
    current_timestamp().alias("_created_at"),
    current_timestamp().alias("_updated_at")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold Table

# COMMAND ----------

table_name = gold_table("dim_customer")
dim_customer.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✓ Written {dim_customer.count()} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Data

# COMMAND ----------

display(spark.table(table_name).limit(10))
