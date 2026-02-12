# Databricks notebook source
# MAGIC %md
# MAGIC # Dimension: Order Type
# MAGIC Order type reference dimension.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, md5

# COMMAND ----------

@dlt.table(
    name="dim_order_type",
    comment="Order type reference dimension",
    table_properties={"quality": "gold"}
)
def dim_order_type():
    order_types = [
        ("Dine-In", True, 5),
        ("Takeout", True, 3),
        ("Drive-Thru", False, 4),
        ("Delivery", False, 30),
        ("Catering", False, 60),
    ]
    
    return (
        spark.createDataFrame(order_types, ["order_type", "is_in_store", "avg_service_minutes"])
        .select(
            md5(col("order_type")).alias("order_type_sk"),
            col("order_type"),
            col("is_in_store"),
            col("avg_service_minutes")
        )
    )
