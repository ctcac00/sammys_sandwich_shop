# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Loyalty Tier Analysis
# MAGIC Performance metrics by customer loyalty tier.

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {gold_table('rpt_loyalty_tier_analysis')} AS
SELECT 
    dc.loyalty_tier,
    dc.loyalty_tier_rank,
    COUNT(DISTINCT dc.customer_sk) as customer_count,
    
    -- Activity metrics
    SUM(fca.total_orders) as total_orders,
    SUM(fca.total_spend) as total_spend,
    ROUND(AVG(fca.total_orders), 1) as avg_orders_per_customer,
    ROUND(AVG(fca.total_spend), 2) as avg_spend_per_customer,
    ROUND(AVG(fca.avg_order_value), 2) as avg_order_value,
    
    -- Engagement
    ROUND(AVG(fca.days_since_last_order), 1) as avg_days_since_last_order,
    
    -- RFM distribution
    ROUND(AVG(fca.recency_score), 2) as avg_recency_score,
    ROUND(AVG(fca.frequency_score), 2) as avg_frequency_score,
    ROUND(AVG(fca.monetary_score), 2) as avg_monetary_score
    
FROM {gold_table('dim_customer')} dc
JOIN {gold_table('fct_customer_activity')} fca ON dc.customer_sk = fca.customer_sk
WHERE dc.is_current = true AND dc.customer_id != '{UNKNOWN_CUSTOMER_ID}'
GROUP BY dc.loyalty_tier, dc.loyalty_tier_rank
ORDER BY dc.loyalty_tier_rank DESC
""")

print(f"✓ Created view {gold_table('rpt_loyalty_tier_analysis')}")

# COMMAND ----------

display(spark.table(gold_table('rpt_loyalty_tier_analysis')))
