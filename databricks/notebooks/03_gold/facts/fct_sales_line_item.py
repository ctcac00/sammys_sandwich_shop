# Databricks notebook source
# MAGIC %md
# MAGIC # Fact: Sales Line Item
# MAGIC Grain: one row per order item.
# MAGIC 
# MAGIC Mirrors: `dbt/models/marts/facts/fct_sales_line_item.sql`

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, when, coalesce, current_timestamp, year, month, dayofmonth,
    round as spark_round
)
from pyspark.sql.types import IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

df_order_items = spark.table(silver_table("int_order_items"))
df_orders = spark.table(silver_table("int_orders")).filter(col("order_status") == "Completed")
df_dim_customer = spark.table(gold_table("dim_customer")).filter(col("is_current") == True)
df_dim_employee = spark.table(gold_table("dim_employee")).filter(col("is_current") == True)
df_dim_location = spark.table(gold_table("dim_location")).filter(col("is_current") == True)
df_dim_menu_item = spark.table(gold_table("dim_menu_item")).filter(col("is_current") == True)

print(f"Loaded {df_order_items.count()} order items")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Unknown Customer SK

# COMMAND ----------

unknown_customer_sk = df_dim_customer.filter(
    col("customer_id") == UNKNOWN_CUSTOMER_ID
).select("customer_sk").first()[0]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Fact Line Items

# COMMAND ----------

fct_line_items = df_order_items.alias("oi") \
    .join(df_orders.alias("o"), col("oi.order_id") == col("o.order_id"), "inner") \
    .join(df_dim_customer.alias("dc"), col("o.customer_id") == col("dc.customer_id"), "left") \
    .join(df_dim_employee.alias("de"), col("o.employee_id") == col("de.employee_id"), "left") \
    .join(df_dim_location.alias("dl"), col("o.location_id") == col("dl.location_id"), "left") \
    .join(df_dim_menu_item.alias("dm"), col("oi.item_id") == col("dm.item_id"), "left") \
    .select(
        # Keys
        col("oi.order_item_id"),
        col("oi.order_id"),
        (year(col("o.order_date")) * 10000 + month(col("o.order_date")) * 100 + dayofmonth(col("o.order_date"))).cast(IntegerType()).alias("date_key"),
        coalesce(col("dc.customer_sk"), lit(unknown_customer_sk)).alias("customer_sk"),
        col("de.employee_sk"),
        col("dl.location_sk"),
        col("dm.menu_item_sk"),
        
        # Measures
        col("oi.quantity"),
        col("oi.unit_price"),
        col("oi.line_total"),
        (coalesce(col("dm.food_cost"), lit(0)) * col("oi.quantity")).alias("food_cost"),
        (col("oi.line_total") - (coalesce(col("dm.food_cost"), lit(0)) * col("oi.quantity"))).alias("gross_profit"),
        spark_round(
            (col("oi.line_total") - (coalesce(col("dm.food_cost"), lit(0)) * col("oi.quantity"))) / col("oi.line_total") * 100, 
            2
        ).alias("gross_margin_pct"),
        
        # Flags
        col("oi.has_customization"),
        
        # Metadata
        current_timestamp().alias("_created_at")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold Table

# COMMAND ----------

table_name = gold_table("fct_sales_line_item")
fct_line_items.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✓ Written {fct_line_items.count()} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Data

# COMMAND ----------

display(spark.table(table_name).limit(10))
