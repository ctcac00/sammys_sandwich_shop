# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Customers at Risk
# MAGIC Identifies customers who may be churning.

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {gold_table('rpt_customers_at_risk')} AS
SELECT 
    dc.customer_id,
    dc.full_name,
    dc.email,
    dc.loyalty_tier,
    dc.marketing_opt_in,
    
    -- Metrics
    fca.total_orders,
    fca.total_spend,
    fca.avg_order_value,
    
    -- Risk indicators
    fca.days_since_last_order,
    fca.rfm_segment,
    fca.recency_score,
    fca.frequency_score,
    fca.monetary_score,
    
    -- Last activity
    TO_DATE(CAST(fca.last_order_date_key AS STRING), 'yyyyMMdd') as last_order_date,
    
    -- Favorite location for outreach
    dl.location_name as favorite_location
    
FROM {gold_table('fct_customer_activity')} fca
JOIN {gold_table('dim_customer')} dc ON fca.customer_sk = dc.customer_sk
LEFT JOIN {gold_table('dim_location')} dl ON fca.favorite_location_sk = dl.location_sk
WHERE fca.rfm_segment IN ('At Risk', 'Need Attention', 'Lost')
ORDER BY fca.total_spend DESC
""")

print(f"✓ Created view {gold_table('rpt_customers_at_risk')}")

# COMMAND ----------

display(spark.table(gold_table('rpt_customers_at_risk')).limit(20))
