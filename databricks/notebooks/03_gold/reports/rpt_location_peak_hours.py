# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Location Peak Hours
# MAGIC Analyzes orders by time period per location.

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {gold_table('rpt_location_peak_hours')} AS
SELECT 
    dl.location_name,
    dl.region,
    SUM(fds.breakfast_orders) as breakfast_orders,
    SUM(fds.lunch_orders) as lunch_orders,
    SUM(fds.dinner_orders) as dinner_orders,
    SUM(fds.total_orders) - SUM(fds.breakfast_orders) - SUM(fds.lunch_orders) - SUM(fds.dinner_orders) as other_orders,
    
    -- Peak period
    CASE 
        WHEN SUM(fds.breakfast_orders) >= SUM(fds.lunch_orders) 
             AND SUM(fds.breakfast_orders) >= SUM(fds.dinner_orders) THEN 'Breakfast'
        WHEN SUM(fds.lunch_orders) >= SUM(fds.dinner_orders) THEN 'Lunch'
        ELSE 'Dinner'
    END as peak_period,
    
    -- Percentages
    ROUND(SUM(fds.breakfast_orders) * 100.0 / NULLIF(SUM(fds.total_orders), 0), 1) as breakfast_pct,
    ROUND(SUM(fds.lunch_orders) * 100.0 / NULLIF(SUM(fds.total_orders), 0), 1) as lunch_pct,
    ROUND(SUM(fds.dinner_orders) * 100.0 / NULLIF(SUM(fds.total_orders), 0), 1) as dinner_pct
    
FROM {gold_table('fct_daily_summary')} fds
JOIN {gold_table('dim_location')} dl ON fds.location_sk = dl.location_sk
GROUP BY dl.location_name, dl.region
ORDER BY dl.location_name
""")

print(f"✓ Created view {gold_table('rpt_location_peak_hours')}")

# COMMAND ----------

display(spark.table(gold_table('rpt_location_peak_hours')))
