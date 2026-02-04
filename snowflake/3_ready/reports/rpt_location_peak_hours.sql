/*
================================================================================
  REPORT: LOCATION PEAK HOURS
================================================================================
  Peak hours analysis by location.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

CREATE OR REPLACE VIEW v_rpt_location_peak_hours AS
SELECT 
    dl.location_name,
    dt.hour_24,
    dt.time_period,
    COUNT(DISTINCT fs.order_id) AS order_count,
    SUM(fs.total_amount) AS revenue,
    ROUND(AVG(fs.total_amount), 2) AS avg_order_value,
    -- Rank hours within location
    RANK() OVER (PARTITION BY dl.location_name ORDER BY COUNT(DISTINCT fs.order_id) DESC) AS hour_rank
FROM fact_sales fs
JOIN dim_location dl ON fs.location_sk = dl.location_sk
JOIN dim_time dt ON fs.time_key = dt.time_key
GROUP BY dl.location_name, dt.hour_24, dt.time_period
ORDER BY dl.location_name, order_count DESC;
