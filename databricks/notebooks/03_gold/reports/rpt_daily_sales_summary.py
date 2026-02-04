# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Daily Sales Summary
# MAGIC Daily sales metrics with day-over-day and week-over-week comparisons.

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Report View

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {gold_table('rpt_daily_sales_summary')} AS
SELECT 
    dd.full_date as order_date,
    dd.day_name,
    dd.is_weekend,
    dl.location_name,
    dl.region,
    
    -- Volume metrics
    fds.total_orders,
    fds.total_items_sold,
    fds.unique_customers,
    fds.guest_orders,
    
    -- Revenue metrics
    fds.gross_sales,
    fds.discounts_given,
    fds.net_sales,
    fds.tax_collected,
    fds.tips_received,
    fds.total_revenue,
    
    -- Performance metrics
    fds.avg_order_value,
    fds.avg_items_per_order,
    fds.discount_rate,
    
    -- Order type breakdown
    fds.dine_in_orders,
    fds.takeout_orders,
    fds.drive_thru_orders,
    
    -- Period breakdown
    fds.breakfast_orders,
    fds.lunch_orders,
    fds.dinner_orders,
    
    -- Comparison metrics (vs previous day)
    LAG(fds.total_revenue) OVER (
        PARTITION BY dl.location_sk 
        ORDER BY dd.full_date
    ) as prev_day_revenue,
    fds.total_revenue - LAG(fds.total_revenue) OVER (
        PARTITION BY dl.location_sk 
        ORDER BY dd.full_date
    ) as revenue_change,
    
    -- Week over week comparison
    LAG(fds.total_revenue, 7) OVER (
        PARTITION BY dl.location_sk 
        ORDER BY dd.full_date
    ) as same_day_last_week_revenue
    
FROM {gold_table('fct_daily_summary')} fds
JOIN {gold_table('dim_date')} dd ON fds.date_key = dd.date_key
JOIN {gold_table('dim_location')} dl ON fds.location_sk = dl.location_sk
ORDER BY dd.full_date DESC, dl.location_name
""")

print(f"✓ Created view {gold_table('rpt_daily_sales_summary')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Data

# COMMAND ----------

display(spark.table(gold_table('rpt_daily_sales_summary')).limit(20))
