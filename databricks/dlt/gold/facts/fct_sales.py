# Databricks notebook source
# MAGIC %md
# MAGIC # Fact: Sales (Order Level)
# MAGIC Grain: one row per order.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, lit, when, coalesce, current_timestamp, hour, minute, year, month, dayofmonth,
    round as spark_round, md5
)
from pyspark.sql.types import IntegerType, StringType

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

@dlt.table(
    name="fct_sales",
    comment="Order-level sales fact table",
    table_properties={"quality": "gold"}
)
@dlt.expect("valid_order_id", "order_id IS NOT NULL")
@dlt.expect("valid_date_key", "date_key IS NOT NULL")
def fct_sales():
    orders = dlt.read("int_orders").filter(col("order_status") == "Completed")
    dim_customer = dlt.read("dim_customer").filter(col("is_current") == True)
    dim_employee = dlt.read("dim_employee").filter(col("is_current") == True)
    dim_location = dlt.read("dim_location").filter(col("is_current") == True)
    dim_payment = dlt.read("dim_payment_method")
    dim_order_type = dlt.read("dim_order_type")
    
    # Get unknown customer SK for guest orders
    unknown_customer_sk = md5(lit(UNKNOWN_CUSTOMER_ID))
    
    return (
        orders.alias("o")
        .join(dim_customer.alias("dc"), col("o.customer_id") == col("dc.customer_id"), "left")
        .join(dim_employee.alias("de"), col("o.employee_id") == col("de.employee_id"), "left")
        .join(dim_location.alias("dl"), col("o.location_id") == col("dl.location_id"), "left")
        .join(dim_payment.alias("pm"), col("o.payment_method") == col("pm.payment_method"), "left")
        .join(dim_order_type.alias("ot"), col("o.order_type") == col("ot.order_type"), "left")
        .select(
            col("o.order_id"),
            # Dimension keys
            (year(col("o.order_date")) * 10000 + 
             month(col("o.order_date")) * 100 + 
             dayofmonth(col("o.order_date"))).cast(IntegerType()).alias("date_key"),
            (hour(col("o.order_datetime")) * 100 + 
             minute(col("o.order_datetime"))).cast(IntegerType()).alias("time_key"),
            coalesce(col("dc.customer_sk"), unknown_customer_sk).alias("customer_sk"),
            col("de.employee_sk"),
            col("dl.location_sk"),
            col("pm.payment_method_sk"),
            col("ot.order_type_sk"),
            # Degenerate dimensions
            col("o.order_status"),
            col("o.order_period"),
            # Measures
            col("o.item_count"),
            col("o.line_item_count"),
            col("o.subtotal"),
            col("o.tax_amount"),
            col("o.discount_amount"),
            col("o.tip_amount"),
            col("o.total_amount"),
            (col("o.subtotal") - col("o.discount_amount")).alias("net_sales"),
            # Derived measures
            when(col("o.subtotal") > 0, 
                 spark_round(col("o.discount_amount") / col("o.subtotal") * 100, 2))
                .otherwise(0).alias("discount_pct"),
            when(col("o.subtotal") > 0,
                 spark_round(col("o.tip_amount") / col("o.subtotal") * 100, 2))
                .otherwise(0).alias("tip_pct"),
            when(col("o.item_count") > 0,
                 spark_round(col("o.subtotal") / col("o.item_count"), 2))
                .otherwise(0).alias("avg_item_price"),
            # Flags
            col("o.has_discount"),
            col("o.has_tip"),
            col("o.is_guest_order"),
            col("o.is_weekend"),
            # Metadata
            current_timestamp().alias("_created_at")
        )
    )
