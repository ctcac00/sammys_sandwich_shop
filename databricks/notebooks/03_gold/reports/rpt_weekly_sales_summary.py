# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Weekly Sales Summary
# MAGIC Weekly aggregated sales metrics by location.

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Report View

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {gold_table('rpt_weekly_sales_summary')} AS
SELECT 
    dd.year,
    dd.week_of_year,
    MIN(dd.full_date) as week_start_date,
    MAX(dd.full_date) as week_end_date,
    dl.location_name,
    dl.region,
    
    -- Volume metrics
    SUM(fds.total_orders) as total_orders,
    SUM(fds.total_items_sold) as total_items_sold,
    SUM(fds.unique_customers) as total_customer_visits,
    
    -- Revenue metrics
    SUM(fds.gross_sales) as gross_sales,
    SUM(fds.net_sales) as net_sales,
    SUM(fds.total_revenue) as total_revenue,
    SUM(fds.tips_received) as tips_received,
    
    -- Performance metrics
    ROUND(AVG(fds.avg_order_value), 2) as avg_order_value,
    ROUND(SUM(fds.discounts_given) / NULLIF(SUM(fds.gross_sales), 0) * 100, 2) as discount_rate,
    
    -- Days open
    COUNT(DISTINCT fds.date_key) as days_open,
    ROUND(SUM(fds.total_revenue) / NULLIF(COUNT(DISTINCT fds.date_key), 0), 2) as avg_daily_revenue
    
FROM {gold_table('fct_daily_summary')} fds
JOIN {gold_table('dim_date')} dd ON fds.date_key = dd.date_key
JOIN {gold_table('dim_location')} dl ON fds.location_sk = dl.location_sk
GROUP BY dd.year, dd.week_of_year, dl.location_name, dl.region
ORDER BY dd.year DESC, dd.week_of_year DESC, total_revenue DESC
""")

print(f"✓ Created view {gold_table('rpt_weekly_sales_summary')}")

# COMMAND ----------

display(spark.table(gold_table('rpt_weekly_sales_summary')).limit(20))
