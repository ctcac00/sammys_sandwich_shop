/*
================================================================================
  REPORT: LOCATION PERFORMANCE
================================================================================
  Compares performance metrics across store locations.
  Identifies high/low performers and operational insights.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

CREATE OR REPLACE VIEW v_rpt_location_performance AS
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
    SUM(fds.total_orders) AS total_orders,
    SUM(fds.total_items_sold) AS total_items_sold,
    SUM(fds.gross_sales) AS gross_sales,
    SUM(fds.net_sales) AS net_sales,
    SUM(fds.total_revenue) AS total_revenue,
    SUM(fds.tips_received) AS tips_received,
    -- Performance metrics
    ROUND(AVG(fds.avg_order_value), 2) AS avg_order_value,
    ROUND(AVG(fds.avg_items_per_order), 2) AS avg_items_per_order,
    ROUND(AVG(fds.discount_rate), 2) AS avg_discount_rate,
    -- Customer metrics
    SUM(fds.unique_customers) AS total_customer_visits,
    SUM(fds.guest_orders) AS guest_orders,
    ROUND(SUM(fds.guest_orders) * 100.0 / NULLIF(SUM(fds.total_orders), 0), 2) AS guest_order_pct,
    -- Order type breakdown
    SUM(fds.dine_in_orders) AS dine_in_orders,
    SUM(fds.takeout_orders) AS takeout_orders,
    SUM(fds.drive_thru_orders) AS drive_thru_orders,
    -- Period breakdown
    SUM(fds.breakfast_orders) AS breakfast_orders,
    SUM(fds.lunch_orders) AS lunch_orders,
    SUM(fds.dinner_orders) AS dinner_orders,
    -- Days of data
    COUNT(DISTINCT fds.date_key) AS days_of_data,
    -- Daily averages
    ROUND(SUM(fds.total_revenue) / NULLIF(COUNT(DISTINCT fds.date_key), 0), 2) AS avg_daily_revenue,
    ROUND(SUM(fds.total_orders) / NULLIF(COUNT(DISTINCT fds.date_key), 0), 1) AS avg_daily_orders
FROM fact_daily_summary fds
JOIN dim_location dl ON fds.location_sk = dl.location_sk
GROUP BY 
    dl.location_name, dl.region, dl.city, dl.years_in_operation,
    dl.seating_capacity, dl.capacity_tier, dl.has_drive_thru, dl.manager_name
ORDER BY total_revenue DESC;
