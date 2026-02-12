# Databricks notebook source
# MAGIC %md
# MAGIC # Intermediate: Customers
# MAGIC Adds derived fields and business logic enrichments to customer data.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, concat_ws, datediff, current_date, current_timestamp, floor, when, upper
)
from pyspark.sql.types import IntegerType

# COMMAND ----------

@dlt.table(
    name="int_customers",
    comment="Customer data with derived fields",
    table_properties={"quality": "silver"}
)
def int_customers():
    return (
        dlt.read("stg_customers")
        .select(
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
            # Age calculation
            floor(datediff(current_date(), col("birth_date")) / 365.25).cast(IntegerType()).alias("age"),
            # Loyalty tier rank
            when(upper(col("loyalty_tier")) == "PLATINUM", 4)
            .when(upper(col("loyalty_tier")) == "GOLD", 3)
            .when(upper(col("loyalty_tier")) == "SILVER", 2)
            .when(upper(col("loyalty_tier")) == "BRONZE", 1)
            .otherwise(0).alias("loyalty_tier_rank"),
            # Customer tenure
            datediff(current_date(), col("signup_date")).alias("customer_tenure_days"),
            current_timestamp().alias("_enriched_at")
        )
    )
