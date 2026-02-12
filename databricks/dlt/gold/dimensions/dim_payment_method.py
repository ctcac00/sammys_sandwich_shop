# Databricks notebook source
# MAGIC %md
# MAGIC # Dimension: Payment Method
# MAGIC Payment method reference dimension.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, md5

# COMMAND ----------

@dlt.table(
    name="dim_payment_method",
    comment="Payment method reference dimension",
    table_properties={"quality": "gold"}
)
def dim_payment_method():
    payment_methods = [
        ("Credit Card", "Card", True, 0.0295),
        ("Debit Card", "Card", True, 0.0150),
        ("Cash", "Cash", False, 0.0000),
        ("Mobile Pay", "Digital", True, 0.0250),
        ("Gift Card", "Prepaid", False, 0.0000),
    ]
    
    return (
        spark.createDataFrame(payment_methods, ["payment_method", "payment_type", "is_digital", "processing_fee_pct"])
        .select(
            md5(col("payment_method")).alias("payment_method_sk"),
            col("payment_method"),
            col("payment_type"),
            col("is_digital"),
            col("processing_fee_pct")
        )
    )
