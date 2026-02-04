# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Customer Overview
# MAGIC Customer overview with lifetime metrics, RFM scores, and favorites.

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {gold_table('rpt_customer_overview')} AS
SELECT 
    dc.customer_id,
    dc.full_name,
    dc.email,
    dc.city,
    dc.loyalty_tier,
    dc.loyalty_tier_rank,
    dc.age_group,
    dc.tenure_group,
    dc.marketing_opt_in,
    
    -- Activity metrics
    fca.total_orders,
    fca.total_items_purchased,
    fca.total_spend,
    fca.total_tips,
    fca.avg_order_value,
    fca.customer_lifetime_days,
    fca.days_since_last_order,
    
    -- RFM
    fca.recency_score,
    fca.frequency_score,
    fca.monetary_score,
    fca.rfm_segment,
    
    -- Favorites
    dl.location_name as favorite_location,
    dm.item_name as favorite_item,
    fca.favorite_order_type,
    
    -- First/Last order dates
    TO_DATE(CAST(fca.first_order_date_key AS STRING), 'yyyyMMdd') as first_order_date,
    TO_DATE(CAST(fca.last_order_date_key AS STRING), 'yyyyMMdd') as last_order_date
    
FROM {gold_table('fct_customer_activity')} fca
JOIN {gold_table('dim_customer')} dc ON fca.customer_sk = dc.customer_sk
LEFT JOIN {gold_table('dim_location')} dl ON fca.favorite_location_sk = dl.location_sk
LEFT JOIN {gold_table('dim_menu_item')} dm ON fca.favorite_menu_item_sk = dm.menu_item_sk
ORDER BY fca.total_spend DESC
""")

print(f"✓ Created view {gold_table('rpt_customer_overview')}")

# COMMAND ----------

display(spark.table(gold_table('rpt_customer_overview')).limit(20))
