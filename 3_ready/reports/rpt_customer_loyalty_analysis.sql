/*
================================================================================
  REPORT: CUSTOMER LOYALTY ANALYSIS
================================================================================
  Deep dive into customer behavior, loyalty, and lifetime value.
  Supports RFM segmentation and customer retention strategies.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

-- Customer overview with lifetime metrics
CREATE OR REPLACE VIEW v_rpt_customer_overview AS
SELECT 
    dc.customer_id,
    dc.full_name,
    dc.email,
    dc.city,
    dc.loyalty_tier,
    dc.loyalty_tier_rank,
    dc.age_group,
    dc.tenure_group,
    dc.marketing_opt_in,
    -- Activity metrics
    fca.total_orders,
    fca.total_items_purchased,
    fca.total_spend,
    fca.total_tips,
    fca.avg_order_value,
    fca.avg_days_between_orders,
    fca.customer_lifetime_days,
    fca.days_since_last_order,
    -- RFM
    fca.recency_score,
    fca.frequency_score,
    fca.monetary_score,
    fca.rfm_segment,
    -- Favorites
    dl.location_name AS favorite_location,
    dm.item_name AS favorite_item,
    fca.favorite_order_type,
    -- First/Last order dates
    TO_DATE(fca.first_order_date_key::VARCHAR, 'YYYYMMDD') AS first_order_date,
    TO_DATE(fca.last_order_date_key::VARCHAR, 'YYYYMMDD') AS last_order_date
FROM fact_customer_activity fca
JOIN dim_customer dc ON fca.customer_sk = dc.customer_sk
LEFT JOIN dim_location dl ON fca.favorite_location_sk = dl.location_sk
LEFT JOIN dim_menu_item dm ON fca.favorite_menu_item_sk = dm.menu_item_sk
ORDER BY fca.total_spend DESC;

-- RFM Segment Summary
CREATE OR REPLACE VIEW v_rpt_rfm_segment_summary AS
SELECT 
    rfm_segment,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_customers,
    SUM(total_spend) AS segment_total_spend,
    ROUND(SUM(total_spend) * 100.0 / SUM(SUM(total_spend)) OVER (), 2) AS pct_of_revenue,
    ROUND(AVG(total_spend), 2) AS avg_customer_spend,
    ROUND(AVG(total_orders), 1) AS avg_orders,
    ROUND(AVG(days_since_last_order), 0) AS avg_days_since_order,
    ROUND(AVG(avg_order_value), 2) AS avg_order_value
FROM fact_customer_activity
GROUP BY rfm_segment
ORDER BY segment_total_spend DESC;

-- Customer loyalty tier analysis
CREATE OR REPLACE VIEW v_rpt_loyalty_tier_analysis AS
SELECT 
    dc.loyalty_tier,
    dc.loyalty_tier_rank,
    COUNT(DISTINCT dc.customer_sk) AS customer_count,
    SUM(fca.total_orders) AS total_orders,
    SUM(fca.total_spend) AS total_spend,
    ROUND(AVG(fca.total_spend), 2) AS avg_customer_spend,
    ROUND(AVG(fca.total_orders), 1) AS avg_orders_per_customer,
    ROUND(AVG(fca.avg_order_value), 2) AS avg_order_value,
    ROUND(AVG(fca.days_since_last_order), 0) AS avg_days_since_order,
    -- Spend distribution
    MIN(fca.total_spend) AS min_spend,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY fca.total_spend) AS spend_25th_pct,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY fca.total_spend) AS spend_median,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY fca.total_spend) AS spend_75th_pct,
    MAX(fca.total_spend) AS max_spend
FROM fact_customer_activity fca
JOIN dim_customer dc ON fca.customer_sk = dc.customer_sk
GROUP BY dc.loyalty_tier, dc.loyalty_tier_rank
ORDER BY dc.loyalty_tier_rank DESC;

-- Customer retention cohort analysis (by signup month)
CREATE OR REPLACE VIEW v_rpt_customer_cohort_analysis AS
SELECT 
    DATE_TRUNC('month', dc.signup_date) AS cohort_month,
    COUNT(DISTINCT dc.customer_sk) AS cohort_size,
    SUM(fca.total_orders) AS total_orders,
    SUM(fca.total_spend) AS total_spend,
    ROUND(AVG(fca.total_orders), 1) AS avg_orders,
    ROUND(AVG(fca.total_spend), 2) AS avg_spend,
    -- Activity rates
    ROUND(COUNT(CASE WHEN fca.days_since_last_order <= 30 THEN 1 END) * 100.0 / COUNT(*), 2) AS active_last_30_days_pct,
    ROUND(COUNT(CASE WHEN fca.days_since_last_order <= 90 THEN 1 END) * 100.0 / COUNT(*), 2) AS active_last_90_days_pct
FROM fact_customer_activity fca
JOIN dim_customer dc ON fca.customer_sk = dc.customer_sk
WHERE dc.signup_date IS NOT NULL
GROUP BY DATE_TRUNC('month', dc.signup_date)
ORDER BY cohort_month DESC;

-- Top customers by spend
CREATE OR REPLACE VIEW v_rpt_top_customers AS
SELECT 
    dc.customer_id,
    dc.full_name,
    dc.email,
    dc.loyalty_tier,
    fca.total_orders,
    fca.total_spend,
    fca.avg_order_value,
    fca.days_since_last_order,
    fca.rfm_segment,
    -- Ranking
    RANK() OVER (ORDER BY fca.total_spend DESC) AS spend_rank,
    RANK() OVER (ORDER BY fca.total_orders DESC) AS order_rank,
    -- Percentile
    PERCENT_RANK() OVER (ORDER BY fca.total_spend) AS spend_percentile
FROM fact_customer_activity fca
JOIN dim_customer dc ON fca.customer_sk = dc.customer_sk
ORDER BY fca.total_spend DESC;

-- Customers at risk (active customers who haven't ordered recently)
CREATE OR REPLACE VIEW v_rpt_customers_at_risk AS
SELECT 
    dc.customer_id,
    dc.full_name,
    dc.email,
    dc.loyalty_tier,
    fca.total_orders,
    fca.total_spend,
    fca.avg_order_value,
    fca.days_since_last_order,
    fca.avg_days_between_orders,
    fca.rfm_segment,
    -- Risk level
    CASE 
        WHEN fca.days_since_last_order > fca.avg_days_between_orders * 3 THEN 'Critical'
        WHEN fca.days_since_last_order > fca.avg_days_between_orders * 2 THEN 'High'
        WHEN fca.days_since_last_order > fca.avg_days_between_orders * 1.5 THEN 'Medium'
        ELSE 'Low'
    END AS churn_risk,
    dl.location_name AS favorite_location,
    dm.item_name AS favorite_item
FROM fact_customer_activity fca
JOIN dim_customer dc ON fca.customer_sk = dc.customer_sk
LEFT JOIN dim_location dl ON fca.favorite_location_sk = dl.location_sk
LEFT JOIN dim_menu_item dm ON fca.favorite_menu_item_sk = dm.menu_item_sk
WHERE fca.total_orders >= 2  -- Only consider repeat customers
  AND fca.days_since_last_order > fca.avg_days_between_orders
ORDER BY fca.total_spend DESC;
