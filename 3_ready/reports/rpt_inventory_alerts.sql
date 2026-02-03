/*
================================================================================
  REPORT: INVENTORY ALERTS
================================================================================
  Inventory items needing attention (low stock, expiring, etc.).
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

CREATE OR REPLACE VIEW v_rpt_inventory_alerts AS
SELECT 
    dl.location_name,
    di.ingredient_name,
    di.supplier_name,
    fis.quantity_available,
    fis.reorder_point,
    fis.stock_status,
    fis.days_until_expiration,
    fis.expiration_risk,
    -- Alert type
    CASE 
        WHEN fis.stock_status = 'Out of Stock' THEN 'CRITICAL: Out of Stock'
        WHEN fis.stock_status = 'Critical Low' THEN 'URGENT: Critical Low Stock'
        WHEN fis.expiration_risk = 'Expired' THEN 'CRITICAL: Expired'
        WHEN fis.expiration_risk = 'Critical' THEN 'URGENT: Expiring Today/Tomorrow'
        WHEN fis.stock_status = 'Low Stock' THEN 'WARNING: Low Stock'
        WHEN fis.expiration_risk = 'Warning' THEN 'WARNING: Expiring in 3 days'
        ELSE 'Monitor'
    END AS alert_type,
    -- Priority for ordering
    CASE 
        WHEN fis.stock_status IN ('Out of Stock', 'Critical Low') THEN 1
        WHEN fis.expiration_risk IN ('Expired', 'Critical') THEN 1
        WHEN fis.stock_status = 'Low Stock' THEN 2
        WHEN fis.expiration_risk = 'Warning' THEN 2
        ELSE 3
    END AS priority
FROM fact_inventory_snapshot fis
JOIN dim_location dl ON fis.location_sk = dl.location_sk
JOIN dim_ingredient di ON fis.ingredient_sk = di.ingredient_sk
WHERE fis.needs_reorder = TRUE 
   OR fis.expiration_risk IN ('Expired', 'Critical', 'Warning')
   OR fis.stock_status IN ('Out of Stock', 'Critical Low', 'Low Stock')
ORDER BY priority, dl.location_name, di.ingredient_name;
