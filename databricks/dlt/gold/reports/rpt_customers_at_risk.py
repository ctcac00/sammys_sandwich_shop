# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Customers at Risk
# MAGIC Customers showing signs of churn.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col

# COMMAND ----------

@dlt.view(
    name="rpt_customers_at_risk",
    comment="Customers showing signs of churn"
)
def rpt_customers_at_risk():
    fca = dlt.read("fct_customer_activity")
    dc = dlt.read("dim_customer").filter(col("is_current") == True)
    
    return (
        fca
        .filter(col("rfm_segment").isin("At Risk", "Hibernating"))
        .join(dc, fca.customer_sk == dc.customer_sk)
        .select(
            dc.customer_id,
            dc.full_name,
            dc.email,
            dc.loyalty_tier,
            fca.total_orders,
            fca.total_revenue,
            fca.days_since_last_order,
            fca.rfm_segment
        )
        .orderBy(col("total_revenue").desc())
    )
