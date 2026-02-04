# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Location Performance
# MAGIC Comprehensive performance metrics by location.

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {gold_table('rpt_location_performance')} AS
SELECT 
    dl.location_name,
    dl.region,
    dl.city,
    dl.years_in_operation,
    dl.seating_capacity,
    dl.capacity_tier,
    dl.has_drive_thru,
    dl.manager_name,
    
    -- Sales metrics
    SUM(fds.total_orders) as total_orders,
    SUM(fds.total_items_sold) as total_items_sold,
    SUM(fds.gross_sales) as gross_sales,
    SUM(fds.net_sales) as net_sales,
    SUM(fds.total_revenue) as total_revenue,
    SUM(fds.tips_received) as tips_received,
    
    -- Performance metrics
    ROUND(AVG(fds.avg_order_value), 2) as avg_order_value,
    ROUND(AVG(fds.avg_items_per_order), 2) as avg_items_per_order,
    ROUND(AVG(fds.discount_rate), 2) as avg_discount_rate,
    
    -- Customer metrics
    SUM(fds.unique_customers) as total_customer_visits,
    SUM(fds.guest_orders) as guest_orders,
    ROUND(SUM(fds.guest_orders) * 100.0 / NULLIF(SUM(fds.total_orders), 0), 2) as guest_order_pct,
    
    -- Order type breakdown
    SUM(fds.dine_in_orders) as dine_in_orders,
    SUM(fds.takeout_orders) as takeout_orders,
    SUM(fds.drive_thru_orders) as drive_thru_orders,
    
    -- Period breakdown
    SUM(fds.breakfast_orders) as breakfast_orders,
    SUM(fds.lunch_orders) as lunch_orders,
    SUM(fds.dinner_orders) as dinner_orders,
    
    -- Daily averages
    COUNT(DISTINCT fds.date_key) as days_of_data,
    ROUND(SUM(fds.total_revenue) / NULLIF(COUNT(DISTINCT fds.date_key), 0), 2) as avg_daily_revenue,
    ROUND(SUM(fds.total_orders) / NULLIF(COUNT(DISTINCT fds.date_key), 0), 1) as avg_daily_orders
    
FROM {gold_table('fct_daily_summary')} fds
JOIN {gold_table('dim_location')} dl ON fds.location_sk = dl.location_sk
GROUP BY dl.location_name, dl.region, dl.city, dl.years_in_operation,
         dl.seating_capacity, dl.capacity_tier, dl.has_drive_thru, dl.manager_name
ORDER BY total_revenue DESC
""")

print(f"✓ Created view {gold_table('rpt_location_performance')}")

# COMMAND ----------

display(spark.table(gold_table('rpt_location_performance')))
