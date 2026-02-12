# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Customer Overview
# MAGIC Customer summary with lifetime metrics.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col

# COMMAND ----------

@dlt.view(
    name="rpt_customer_overview",
    comment="Customer summary with lifetime metrics"
)
def rpt_customer_overview():
    fca = dlt.read("fct_customer_activity")
    dc = dlt.read("dim_customer").filter(col("is_current") == True)
    
    return (
        fca
        .join(dc, fca.customer_sk == dc.customer_sk)
        .select(
            dc.customer_id,
            dc.full_name,
            dc.email,
            dc.city,
            dc.state,
            dc.loyalty_tier,
            dc.age_group,
            dc.tenure_group,
            fca.total_orders,
            fca.total_revenue,
            fca.avg_order_value,
            fca.first_order_date,
            fca.last_order_date,
            fca.days_since_last_order,
            fca.recency_score,
            fca.frequency_score,
            fca.monetary_score,
            fca.rfm_segment
        )
        .orderBy(col("total_revenue").desc())
    )
