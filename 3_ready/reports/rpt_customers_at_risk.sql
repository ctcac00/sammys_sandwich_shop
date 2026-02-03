/*
================================================================================
  REPORT: CUSTOMERS AT RISK
================================================================================
  Active customers who haven't ordered recently with churn risk scoring.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

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
