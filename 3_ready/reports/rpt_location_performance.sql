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

-- Location performance summary
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

-- Location ranking
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

-- Location daily comparison (pivot by location)
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

-- Location peak hours analysis
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

-- Location employee performance
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

-- Drive-thru location comparison
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
