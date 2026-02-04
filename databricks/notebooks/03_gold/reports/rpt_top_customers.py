# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Top Customers
# MAGIC Top 100 customers by total spend.

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {gold_table('rpt_top_customers')} AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY fca.total_spend DESC) as rank,
    dc.customer_id,
    dc.full_name,
    dc.email,
    dc.loyalty_tier,
    dc.city,
    dc.state,
    
    -- Metrics
    fca.total_orders,
    fca.total_spend,
    fca.avg_order_value,
    fca.total_items_purchased,
    
    -- RFM
    fca.rfm_segment,
    
    -- Activity
    fca.days_since_last_order,
    TO_DATE(CAST(fca.first_order_date_key AS STRING), 'yyyyMMdd') as first_order_date,
    TO_DATE(CAST(fca.last_order_date_key AS STRING), 'yyyyMMdd') as last_order_date
    
FROM {gold_table('fct_customer_activity')} fca
JOIN {gold_table('dim_customer')} dc ON fca.customer_sk = dc.customer_sk
ORDER BY fca.total_spend DESC
LIMIT 100
""")

print(f"✓ Created view {gold_table('rpt_top_customers')}")

# COMMAND ----------

display(spark.table(gold_table('rpt_top_customers')).limit(20))
