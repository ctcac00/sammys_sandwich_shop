# Databricks notebook source
# MAGIC %md
# MAGIC # Dimension: Payment Method
# MAGIC Static/seed dimension for payment methods.
# MAGIC 
# MAGIC Mirrors: `dbt/models/marts/dimensions/dim_payment_method.sql`

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, md5
from pyspark.sql.types import StringType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Payment Method Dimension

# COMMAND ----------

payment_methods = [
    {"payment_method": "Credit Card", "payment_type": "Card", "is_digital": True, "processing_fee_pct": 0.0295},
    {"payment_method": "Debit Card", "payment_type": "Card", "is_digital": True, "processing_fee_pct": 0.0150},
    {"payment_method": "Cash", "payment_type": "Cash", "is_digital": False, "processing_fee_pct": 0.0000},
    {"payment_method": "Mobile Pay", "payment_type": "Digital", "is_digital": True, "processing_fee_pct": 0.0250},
    {"payment_method": "Gift Card", "payment_type": "Prepaid", "is_digital": False, "processing_fee_pct": 0.0000},
]

df = spark.createDataFrame(payment_methods)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Surrogate Key

# COMMAND ----------

dim_payment_method = df.select(
    md5(col("payment_method").cast(StringType())).alias("payment_method_sk"),
    col("payment_method"),
    col("payment_type"),
    col("is_digital"),
    col("processing_fee_pct"),
    current_timestamp().alias("_created_at")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold Table

# COMMAND ----------

table_name = gold_table("dim_payment_method")
dim_payment_method.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✓ Written {dim_payment_method.count()} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Data

# COMMAND ----------

display(spark.table(table_name))
