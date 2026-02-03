/*
================================================================================
  REPORT: DAILY SALES SUMMARY
================================================================================
  Provides daily sales metrics across all locations with comparisons.
  Great for operational dashboards and trend analysis.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

-- Create view for daily sales summary report
CREATE OR REPLACE VIEW v_rpt_daily_sales_summary AS
SELECT 
    dd.full_date AS order_date,
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
        PARTITION BY dl.location_id 
        ORDER BY dd.full_date
    ) AS prev_day_revenue,
    fds.total_revenue - LAG(fds.total_revenue) OVER (
        PARTITION BY dl.location_id 
        ORDER BY dd.full_date
    ) AS revenue_change,
    -- Week over week comparison
    LAG(fds.total_revenue, 7) OVER (
        PARTITION BY dl.location_id 
        ORDER BY dd.full_date
    ) AS same_day_last_week_revenue
FROM fact_daily_summary fds
JOIN dim_date dd ON fds.date_key = dd.date_key
JOIN dim_location dl ON fds.location_sk = dl.location_sk
ORDER BY dd.full_date DESC, dl.location_name;

-- Weekly sales summary
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

-- Company-wide daily totals
CREATE OR REPLACE VIEW v_rpt_company_daily_totals AS
SELECT 
    dd.full_date AS order_date,
    dd.day_name,
    dd.month_name,
    dd.is_weekend,
    dd.is_holiday,
    -- Totals across all locations
    SUM(fds.total_orders) AS total_orders,
    SUM(fds.total_items_sold) AS total_items_sold,
    SUM(fds.unique_customers) AS unique_customers,
    SUM(fds.gross_sales) AS gross_sales,
    SUM(fds.net_sales) AS net_sales,
    SUM(fds.total_revenue) AS total_revenue,
    SUM(fds.tips_received) AS tips_received,
    -- Averages
    ROUND(AVG(fds.avg_order_value), 2) AS avg_order_value,
    -- Running totals
    SUM(SUM(fds.total_revenue)) OVER (
        PARTITION BY dd.year, dd.month 
        ORDER BY dd.full_date
    ) AS mtd_revenue,
    -- 7-day moving average
    ROUND(AVG(SUM(fds.total_revenue)) OVER (
        ORDER BY dd.full_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) AS revenue_7day_avg
FROM fact_daily_summary fds
JOIN dim_date dd ON fds.date_key = dd.date_key
GROUP BY dd.full_date, dd.day_name, dd.month_name, dd.is_weekend, dd.is_holiday, dd.year, dd.month
ORDER BY dd.full_date DESC;

-- Sample query to test the view
-- SELECT * FROM v_rpt_daily_sales_summary WHERE order_date >= DATEADD('day', -7, CURRENT_DATE());
