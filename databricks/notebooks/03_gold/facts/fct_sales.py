# Databricks notebook source
# MAGIC %md
# MAGIC # Fact: Sales (Order Level)
# MAGIC Grain: one row per order.
# MAGIC 
# MAGIC Mirrors: `dbt/models/marts/facts/fct_sales.sql`

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, when, coalesce, current_timestamp, hour, minute, year, month, dayofmonth,
    round as spark_round
)
from pyspark.sql.types import IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

df_orders = spark.table(silver_table("int_orders")).filter(col("order_status") == "Completed")
df_dim_customer = spark.table(gold_table("dim_customer")).filter(col("is_current") == True)
df_dim_employee = spark.table(gold_table("dim_employee")).filter(col("is_current") == True)
df_dim_location = spark.table(gold_table("dim_location")).filter(col("is_current") == True)
df_dim_payment = spark.table(gold_table("dim_payment_method"))
df_dim_order_type = spark.table(gold_table("dim_order_type"))

print(f"Loaded {df_orders.count()} completed orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Unknown Customer SK

# COMMAND ----------

unknown_customer_sk = df_dim_customer.filter(
    col("customer_id") == UNKNOWN_CUSTOMER_ID
).select("customer_sk").first()[0]

print(f"Unknown customer SK: {unknown_customer_sk}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Fact Sales

# COMMAND ----------

# Join orders with dimensions
fct_sales = df_orders.alias("o") \
    .join(df_dim_customer.alias("dc"), col("o.customer_id") == col("dc.customer_id"), "left") \
    .join(df_dim_employee.alias("de"), col("o.employee_id") == col("de.employee_id"), "left") \
    .join(df_dim_location.alias("dl"), col("o.location_id") == col("dl.location_id"), "left") \
    .join(df_dim_payment.alias("pm"), col("o.payment_method") == col("pm.payment_method"), "left") \
    .join(df_dim_order_type.alias("ot"), col("o.order_type") == col("ot.order_type"), "left") \
    .select(
        # Order identifiers
        col("o.order_id"),
        
        # Dimension keys
        (year(col("o.order_date")) * 10000 + month(col("o.order_date")) * 100 + dayofmonth(col("o.order_date"))).cast(IntegerType()).alias("date_key"),
        (hour(col("o.order_datetime")) * 100 + minute(col("o.order_datetime"))).cast(IntegerType()).alias("time_key"),
        coalesce(col("dc.customer_sk"), lit(unknown_customer_sk)).alias("customer_sk"),
        col("de.employee_sk"),
        col("dl.location_sk"),
        col("pm.payment_method_sk"),
        col("ot.order_type_sk"),
        
        # Degenerate dimensions
        col("o.order_status"),
        
        # Measures
        col("o.item_count"),
        col("o.subtotal"),
        col("o.tax_amount"),
        col("o.discount_amount"),
        col("o.tip_amount"),
        col("o.total_amount"),
        (col("o.subtotal") - col("o.discount_amount")).alias("net_sales"),
        
        # Derived measures
        spark_round(col("o.discount_amount") / col("o.subtotal") * 100, 2).alias("discount_pct"),
        spark_round(col("o.tip_amount") / col("o.subtotal") * 100, 2).alias("tip_pct"),
        spark_round(col("o.subtotal") / col("o.item_count"), 2).alias("avg_item_price"),
        
        # Flags
        col("o.has_discount"),
        col("o.has_tip"),
        col("o.is_guest_order"),
        col("o.is_weekend"),
        
        # Metadata
        current_timestamp().alias("_created_at")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold Table

# COMMAND ----------

table_name = gold_table("fct_sales")
fct_sales.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✓ Written {fct_sales.count()} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Data

# COMMAND ----------

display(spark.table(table_name).limit(10))
