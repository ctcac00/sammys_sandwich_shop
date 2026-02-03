/*
================================================================================
  REPORT: TOP CUSTOMERS
================================================================================
  Top customers ranked by spend with percentile analysis.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

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
