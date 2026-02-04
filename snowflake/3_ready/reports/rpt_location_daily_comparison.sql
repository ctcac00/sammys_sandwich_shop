/*
================================================================================
  REPORT: LOCATION DAILY COMPARISON
================================================================================
  Daily revenue comparison across locations (pivot by location).
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

CREATE OR REPLACE VIEW v_rpt_location_daily_comparison AS
SELECT 
    dd.full_date,
    dd.day_name,
    dd.is_weekend,
    SUM(CASE WHEN dl.location_name = 'Sammy''s Downtown' THEN fds.total_revenue ELSE 0 END) AS downtown_revenue,
    SUM(CASE WHEN dl.location_name = 'Sammy''s Westlake' THEN fds.total_revenue ELSE 0 END) AS westlake_revenue,
    SUM(CASE WHEN dl.location_name = 'Sammy''s University' THEN fds.total_revenue ELSE 0 END) AS university_revenue,
    SUM(CASE WHEN dl.location_name = 'Sammy''s Mueller' THEN fds.total_revenue ELSE 0 END) AS mueller_revenue,
    SUM(CASE WHEN dl.location_name = 'Sammy''s Domain' THEN fds.total_revenue ELSE 0 END) AS domain_revenue,
    SUM(fds.total_revenue) AS company_total
FROM fact_daily_summary fds
JOIN dim_date dd ON fds.date_key = dd.date_key
JOIN dim_location dl ON fds.location_sk = dl.location_sk
GROUP BY dd.full_date, dd.day_name, dd.is_weekend
ORDER BY dd.full_date DESC;
