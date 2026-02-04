/*
================================================================================
  REPORT: CUSTOMER OVERVIEW
================================================================================
  Customer overview with lifetime metrics, RFM scores, and favorites.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

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
