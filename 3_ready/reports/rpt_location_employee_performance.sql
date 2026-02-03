/*
================================================================================
  REPORT: LOCATION EMPLOYEE PERFORMANCE
================================================================================
  Employee performance metrics by location.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

CREATE OR REPLACE VIEW v_rpt_location_employee_performance AS
SELECT 
    dl.location_name,
    de.full_name AS employee_name,
    de.job_title,
    de.is_manager,
    de.tenure_months,
    -- Sales metrics
    COUNT(DISTINCT fs.order_id) AS orders_processed,
    SUM(fs.total_amount) AS total_sales,
    SUM(fs.tip_amount) AS tips_received,
    ROUND(AVG(fs.total_amount), 2) AS avg_order_value,
    -- Upselling indicator
    ROUND(AVG(fs.item_count), 2) AS avg_items_per_order,
    -- Rankings within location
    RANK() OVER (PARTITION BY dl.location_name ORDER BY SUM(fs.total_amount) DESC) AS sales_rank
FROM fact_sales fs
JOIN dim_location dl ON fs.location_sk = dl.location_sk
JOIN dim_employee de ON fs.employee_sk = de.employee_sk
GROUP BY dl.location_name, de.full_name, de.job_title, de.is_manager, de.tenure_months
ORDER BY dl.location_name, total_sales DESC;
