# Databricks notebook source
# MAGIC %md
# MAGIC # Report: RFM Segment Summary
# MAGIC Summary metrics by RFM customer segment.

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {gold_table('rpt_rfm_segment_summary')} AS
SELECT 
    fca.rfm_segment,
    COUNT(*) as customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as customer_pct,
    
    -- Activity metrics
    ROUND(AVG(fca.total_orders), 1) as avg_orders,
    ROUND(AVG(fca.total_spend), 2) as avg_spend,
    SUM(fca.total_spend) as total_segment_spend,
    ROUND(SUM(fca.total_spend) * 100.0 / SUM(SUM(fca.total_spend)) OVER(), 2) as spend_share_pct,
    
    -- Recency
    ROUND(AVG(fca.days_since_last_order), 1) as avg_days_since_last_order,
    
    -- Lifetime
    ROUND(AVG(fca.customer_lifetime_days), 1) as avg_lifetime_days,
    ROUND(AVG(fca.avg_order_value), 2) as avg_order_value
    
FROM {gold_table('fct_customer_activity')} fca
GROUP BY fca.rfm_segment
ORDER BY total_segment_spend DESC
""")

print(f"✓ Created view {gold_table('rpt_rfm_segment_summary')}")

# COMMAND ----------

display(spark.table(gold_table('rpt_rfm_segment_summary')))
