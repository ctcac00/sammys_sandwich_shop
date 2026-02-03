/*
================================================================================
  REPORT: LOCATION RANKING
================================================================================
  Location rankings by revenue, orders, and other metrics.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

CREATE OR REPLACE VIEW v_rpt_location_ranking AS
SELECT 
    dl.location_name,
    SUM(fds.total_revenue) AS total_revenue,
    SUM(fds.total_orders) AS total_orders,
    ROUND(AVG(fds.avg_order_value), 2) AS avg_order_value,
    SUM(fds.tips_received) AS tips_received,
    -- Rankings
    RANK() OVER (ORDER BY SUM(fds.total_revenue) DESC) AS revenue_rank,
    RANK() OVER (ORDER BY SUM(fds.total_orders) DESC) AS orders_rank,
    RANK() OVER (ORDER BY AVG(fds.avg_order_value) DESC) AS aov_rank,
    RANK() OVER (ORDER BY SUM(fds.tips_received) DESC) AS tips_rank,
    -- Share of company
    ROUND(SUM(fds.total_revenue) * 100.0 / 
          SUM(SUM(fds.total_revenue)) OVER (), 2) AS revenue_share_pct,
    ROUND(SUM(fds.total_orders) * 100.0 / 
          SUM(SUM(fds.total_orders)) OVER (), 2) AS order_share_pct
FROM fact_daily_summary fds
JOIN dim_location dl ON fds.location_sk = dl.location_sk
GROUP BY dl.location_name
ORDER BY total_revenue DESC;
