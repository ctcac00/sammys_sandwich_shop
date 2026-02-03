/*
================================================================================
  REPORT: COMPANY DAILY TOTALS
================================================================================
  Company-wide daily totals across all locations with running totals.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

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
