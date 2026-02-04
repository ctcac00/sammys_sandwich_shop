# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Drive-Thru Analysis
# MAGIC Performance analysis for drive-thru enabled locations.

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {gold_table('rpt_drive_thru_analysis')} AS
SELECT 
    dl.location_name,
    dl.region,
    dl.has_drive_thru,
    SUM(fds.total_orders) as total_orders,
    SUM(fds.drive_thru_orders) as drive_thru_orders,
    ROUND(SUM(fds.drive_thru_orders) * 100.0 / NULLIF(SUM(fds.total_orders), 0), 2) as drive_thru_pct,
    SUM(fds.total_revenue) as total_revenue,
    ROUND(AVG(fds.avg_order_value), 2) as avg_order_value
FROM {gold_table('fct_daily_summary')} fds
JOIN {gold_table('dim_location')} dl ON fds.location_sk = dl.location_sk
WHERE dl.has_drive_thru = true
GROUP BY dl.location_name, dl.region, dl.has_drive_thru
ORDER BY drive_thru_orders DESC
""")

print(f"✓ Created view {gold_table('rpt_drive_thru_analysis')}")

# COMMAND ----------

display(spark.table(gold_table('rpt_drive_thru_analysis')))
