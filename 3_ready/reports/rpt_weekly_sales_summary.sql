/*
================================================================================
  REPORT: WEEKLY SALES SUMMARY
================================================================================
  Weekly aggregated sales metrics by location.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

CREATE OR REPLACE VIEW v_rpt_weekly_sales_summary AS
SELECT 
    dd.year,
    dd.week_of_year,
    MIN(dd.full_date) AS week_start,
    MAX(dd.full_date) AS week_end,
    dl.location_name,
    -- Volume
    SUM(fds.total_orders) AS total_orders,
    SUM(fds.total_items_sold) AS total_items_sold,
    SUM(fds.unique_customers) AS unique_customer_visits,
    -- Revenue
    SUM(fds.gross_sales) AS gross_sales,
    SUM(fds.net_sales) AS net_sales,
    SUM(fds.total_revenue) AS total_revenue,
    -- Averages
    ROUND(AVG(fds.avg_order_value), 2) AS avg_order_value,
    ROUND(SUM(fds.total_orders) / 7.0, 1) AS avg_orders_per_day,
    -- Best/worst days
    MAX(fds.total_revenue) AS best_day_revenue,
    MIN(fds.total_revenue) AS worst_day_revenue
FROM fact_daily_summary fds
JOIN dim_date dd ON fds.date_key = dd.date_key
JOIN dim_location dl ON fds.location_sk = dl.location_sk
GROUP BY dd.year, dd.week_of_year, dl.location_name
ORDER BY dd.year DESC, dd.week_of_year DESC, total_revenue DESC;
