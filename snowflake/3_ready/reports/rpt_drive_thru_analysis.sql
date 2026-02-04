/*
================================================================================
  REPORT: DRIVE-THRU ANALYSIS
================================================================================
  Drive-thru location comparison and analysis.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

CREATE OR REPLACE VIEW v_rpt_drive_thru_analysis AS
SELECT 
    dl.location_name,
    dl.has_drive_thru,
    -- Overall metrics
    SUM(fds.total_orders) AS total_orders,
    SUM(fds.total_revenue) AS total_revenue,
    -- Drive-thru specific (for locations with drive-thru)
    SUM(fds.drive_thru_orders) AS drive_thru_orders,
    ROUND(SUM(fds.drive_thru_orders) * 100.0 / NULLIF(SUM(fds.total_orders), 0), 2) AS drive_thru_pct,
    -- Compare to non-drive-thru metrics
    SUM(fds.dine_in_orders) AS dine_in_orders,
    SUM(fds.takeout_orders) AS takeout_orders
FROM fact_daily_summary fds
JOIN dim_location dl ON fds.location_sk = dl.location_sk
GROUP BY dl.location_name, dl.has_drive_thru
ORDER BY has_drive_thru DESC, total_revenue DESC;
