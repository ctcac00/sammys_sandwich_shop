/*
================================================================================
  REPORT: INVENTORY VALUE BY LOCATION
================================================================================
  Inventory value summary by location and ingredient category.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

CREATE OR REPLACE VIEW v_rpt_inventory_value_by_location AS
SELECT 
    dl.location_name,
    di.category AS ingredient_category,
    COUNT(DISTINCT di.ingredient_id) AS ingredient_count,
    SUM(fis.quantity_on_hand) AS total_quantity,
    SUM(fis.stock_value) AS total_value,
    -- Status breakdown
    SUM(CASE WHEN fis.stock_status = 'Well Stocked' THEN fis.stock_value ELSE 0 END) AS well_stocked_value,
    SUM(CASE WHEN fis.stock_status = 'Adequate' THEN fis.stock_value ELSE 0 END) AS adequate_value,
    SUM(CASE WHEN fis.stock_status IN ('Low Stock', 'Critical Low', 'Out of Stock') THEN fis.stock_value ELSE 0 END) AS low_stock_value,
    -- Expiration risk value
    SUM(CASE WHEN fis.expiration_risk IN ('Expired', 'Critical', 'Warning') THEN fis.stock_value ELSE 0 END) AS at_risk_value
FROM fact_inventory_snapshot fis
JOIN dim_location dl ON fis.location_sk = dl.location_sk
JOIN dim_ingredient di ON fis.ingredient_sk = di.ingredient_sk
GROUP BY dl.location_name, di.category
ORDER BY dl.location_name, total_value DESC;
