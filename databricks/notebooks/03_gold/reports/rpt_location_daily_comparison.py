# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Location Daily Comparison
# MAGIC Daily comparisons across locations.

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {gold_table('rpt_location_daily_comparison')} AS
SELECT 
    dd.full_date as order_date,
    dd.day_name,
    dl.location_name,
    dl.region,
    fds.total_orders,
    fds.total_revenue,
    fds.avg_order_value,
    RANK() OVER (PARTITION BY dd.full_date ORDER BY fds.total_revenue DESC) as daily_revenue_rank,
    ROUND(fds.total_revenue * 100.0 / NULLIF(SUM(fds.total_revenue) OVER (PARTITION BY dd.full_date), 0), 2) as daily_share_pct
FROM {gold_table('fct_daily_summary')} fds
JOIN {gold_table('dim_date')} dd ON fds.date_key = dd.date_key
JOIN {gold_table('dim_location')} dl ON fds.location_sk = dl.location_sk
ORDER BY dd.full_date DESC, fds.total_revenue DESC
""")

print(f"✓ Created view {gold_table('rpt_location_daily_comparison')}")

# COMMAND ----------

display(spark.table(gold_table('rpt_location_daily_comparison')).limit(20))
