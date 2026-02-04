# Databricks notebook source
# MAGIC %md
# MAGIC # Dimension: Order Type
# MAGIC Static/seed dimension for order types.
# MAGIC 
# MAGIC Mirrors: `dbt/models/marts/dimensions/dim_order_type.sql`

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, md5
from pyspark.sql.types import StringType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Order Type Dimension

# COMMAND ----------

order_types = [
    {"order_type": "Dine-In", "is_in_store": True, "avg_service_minutes": 5},
    {"order_type": "Takeout", "is_in_store": True, "avg_service_minutes": 3},
    {"order_type": "Drive-Thru", "is_in_store": False, "avg_service_minutes": 4},
    {"order_type": "Delivery", "is_in_store": False, "avg_service_minutes": 30},
    {"order_type": "Catering", "is_in_store": False, "avg_service_minutes": 60},
]

df = spark.createDataFrame(order_types)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Surrogate Key

# COMMAND ----------

dim_order_type = df.select(
    md5(col("order_type").cast(StringType())).alias("order_type_sk"),
    col("order_type"),
    col("is_in_store"),
    col("avg_service_minutes"),
    current_timestamp().alias("_created_at")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold Table

# COMMAND ----------

table_name = gold_table("dim_order_type")
dim_order_type.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✓ Written {dim_order_type.count()} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Data

# COMMAND ----------

display(spark.table(table_name))
