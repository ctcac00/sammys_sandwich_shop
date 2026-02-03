/*
================================================================================
  REPORT: CUSTOMER COHORT ANALYSIS
================================================================================
  Customer retention cohort analysis by signup month.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

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
