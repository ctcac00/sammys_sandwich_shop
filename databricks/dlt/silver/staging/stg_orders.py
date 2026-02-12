# Databricks notebook source
# MAGIC %md
# MAGIC # Staging: Orders
# MAGIC Performs data type casting and basic cleaning on raw order data.
# MAGIC Includes data quality expectations for validation.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, trim, to_date, coalesce, lit
)
from pyspark.sql.types import DoubleType

# COMMAND ----------

@dlt.table(
    name="stg_orders",
    comment="Cleaned and typed order data",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect("valid_total", "total_amount >= 0")
def stg_orders():
    return (
        dlt.read("bronze_orders")
        .select(
            col("order_id"),
            trim(col("customer_id")).alias("customer_id"),
            trim(col("employee_id")).alias("employee_id"),
            trim(col("location_id")).alias("location_id"),
            to_date(col("order_date"), "yyyy-MM-dd").alias("order_date"),
            col("order_datetime").cast("timestamp").alias("order_datetime"),
            trim(col("order_type")).alias("order_type"),
            trim(col("order_status")).alias("order_status"),
            trim(col("payment_method")).alias("payment_method"),
            col("subtotal").cast(DoubleType()).alias("subtotal"),
            coalesce(col("tax_amount").cast(DoubleType()), lit(0.0)).alias("tax_amount"),
            coalesce(col("discount_amount").cast(DoubleType()), lit(0.0)).alias("discount_amount"),
            coalesce(col("tip_amount").cast(DoubleType()), lit(0.0)).alias("tip_amount"),
            col("total_amount").cast(DoubleType()).alias("total_amount"),
            col("_loaded_at"),
            col("_source_file")
        )
    )
