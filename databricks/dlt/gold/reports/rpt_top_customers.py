# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Top Customers
# MAGIC Top customers by revenue.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, dense_rank
from pyspark.sql.window import Window

# COMMAND ----------

@dlt.view(
    name="rpt_top_customers",
    comment="Top customers by revenue"
)
def rpt_top_customers():
    fca = dlt.read("fct_customer_activity")
    dc = dlt.read("dim_customer").filter(col("is_current") == True)
    
    return (
        fca
        .join(dc, fca.customer_sk == dc.customer_sk)
        .select(
            dc.customer_id,
            dc.full_name,
            dc.loyalty_tier,
            fca.total_orders,
            fca.total_revenue,
            fca.avg_order_value,
            fca.rfm_segment,
            dense_rank().over(Window.orderBy(col("total_revenue").desc())).alias("revenue_rank")
        )
        .filter(col("revenue_rank") <= 100)
        .orderBy("revenue_rank")
    )
