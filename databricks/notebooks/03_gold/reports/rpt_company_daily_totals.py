# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Company Daily Totals
# MAGIC Company-wide daily totals across all locations.

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {gold_table('rpt_company_daily_totals')} AS
SELECT 
    dd.full_date as order_date,
    dd.day_name,
    dd.is_weekend,
    
    -- Volume metrics
    SUM(fds.total_orders) as total_orders,
    SUM(fds.total_items_sold) as total_items_sold,
    SUM(fds.unique_customers) as total_customer_visits,
    COUNT(DISTINCT fds.location_sk) as locations_open,
    
    -- Revenue metrics
    SUM(fds.gross_sales) as gross_sales,
    SUM(fds.discounts_given) as discounts_given,
    SUM(fds.net_sales) as net_sales,
    SUM(fds.total_revenue) as total_revenue,
    SUM(fds.tips_received) as tips_received,
    
    -- Performance metrics
    ROUND(SUM(fds.total_revenue) / NULLIF(SUM(fds.total_orders), 0), 2) as avg_order_value,
    ROUND(SUM(fds.discounts_given) / NULLIF(SUM(fds.gross_sales), 0) * 100, 2) as discount_rate
    
FROM {gold_table('fct_daily_summary')} fds
JOIN {gold_table('dim_date')} dd ON fds.date_key = dd.date_key
GROUP BY dd.full_date, dd.day_name, dd.is_weekend
ORDER BY dd.full_date DESC
""")

print(f"✓ Created view {gold_table('rpt_company_daily_totals')}")

# COMMAND ----------

display(spark.table(gold_table('rpt_company_daily_totals')).limit(20))
