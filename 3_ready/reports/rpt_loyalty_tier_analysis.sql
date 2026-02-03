/*
================================================================================
  REPORT: LOYALTY TIER ANALYSIS
================================================================================
  Customer loyalty tier performance analysis with spend distribution.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

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
