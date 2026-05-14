# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Loyalty Tier Analysis
# MAGIC Performance metrics by loyalty tier.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import col, count, avg, sum as spark_sum

# COMMAND ----------

@dp.temporary_view(
    name="rpt_loyalty_tier_analysis",
    comment="Performance metrics by loyalty tier"
)
def rpt_loyalty_tier_analysis():
    fca = spark.read.table("fct_customer_activity")
    dc = spark.read.table("dim_customer").filter(col("is_current") == True)
    
    return (
        fca
        .join(dc, fca.customer_sk == dc.customer_sk)
        .groupBy(dc.loyalty_tier, dc.loyalty_tier_rank)
        .agg(
            count("*").alias("customer_count"),
            spark_sum("total_orders").alias("total_orders"),
            spark_sum("total_revenue").alias("total_revenue"),
            avg("total_orders").alias("avg_orders"),
            avg("total_revenue").alias("avg_revenue"),
            avg("avg_order_value").alias("avg_order_value")
        )
        .orderBy("loyalty_tier_rank")
    )
