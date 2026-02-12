# Databricks notebook source
# MAGIC %md
# MAGIC # Dimension: Customer
# MAGIC Customer dimension with demographics and loyalty info (SCD Type 1).

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, lit, when, current_timestamp, current_date, floor, md5, to_date
)
from pyspark.sql.types import StringType

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

@dlt.table(
    name="dim_customer",
    comment="Customer dimension with demographics and loyalty info",
    table_properties={"quality": "gold"}
)
@dlt.expect("valid_customer_sk", "customer_sk IS NOT NULL")
def dim_customer():
    customers = dlt.read("int_customers")
    
    # Build customer dimension
    customer_dim = (
        customers
        .select(
            md5(col("customer_id").cast(StringType())).alias("customer_sk"),
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
            floor(col("customer_tenure_days") / 30.0).alias("tenure_months"),
            # Tenure grouping
            when(col("customer_tenure_days") < 90, "New (0-3 mo)")
            .when(col("customer_tenure_days") < 365, "Growing (3-12 mo)")
            .when(col("customer_tenure_days") < 730, "Established (1-2 yr)")
            .otherwise("Loyal (2+ yr)").alias("tenure_group"),
            col("marketing_opt_in"),
            col("preferred_location_id"),
            current_date().alias("effective_date"),
            lit(True).alias("is_current"),
            current_timestamp().alias("_created_at"),
            current_timestamp().alias("_updated_at")
        )
    )
    
    # Create unknown customer record
    unknown_customer = spark.createDataFrame([{
        "customer_sk": md5(lit(UNKNOWN_CUSTOMER_ID)).cast(StringType()),
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
        "is_current": True,
    }])
    
    return customer_dim.unionByName(
        unknown_customer.select(
            md5(lit(UNKNOWN_CUSTOMER_ID)).alias("customer_sk"),
            lit(UNKNOWN_CUSTOMER_ID).alias("customer_id"),
            lit("Guest").alias("first_name"),
            lit("Customer").alias("last_name"),
            lit("Guest Customer").alias("full_name"),
            lit(None).cast(StringType()).alias("email"),
            lit(None).cast(StringType()).alias("phone"),
            lit(None).cast(StringType()).alias("city"),
            lit(None).cast(StringType()).alias("state"),
            lit(None).cast(StringType()).alias("zip_code"),
            lit("Unknown").alias("age_group"),
            lit("None").alias("loyalty_tier"),
            lit(0).alias("loyalty_tier_rank"),
            lit(None).cast("date").alias("signup_date"),
            lit(0).cast("long").alias("tenure_months"),
            lit("Unknown").alias("tenure_group"),
            lit(False).alias("marketing_opt_in"),
            lit(None).cast(StringType()).alias("preferred_location_id"),
            to_date(lit("1900-01-01")).alias("effective_date"),
            lit(True).alias("is_current"),
            current_timestamp().alias("_created_at"),
            current_timestamp().alias("_updated_at")
        ),
        allowMissingColumns=True
    )
