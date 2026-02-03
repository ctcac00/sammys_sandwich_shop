/*
================================================================================
  REPORT: INVENTORY STATUS
================================================================================
  Current inventory status by location and ingredient.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

CREATE OR REPLACE VIEW v_rpt_inventory_status AS
SELECT 
    dl.location_name,
    di.ingredient_name,
    di.category AS ingredient_category,
    di.storage_type,
    di.supplier_name,
    -- Quantities
    fis.quantity_on_hand,
    fis.quantity_reserved,
    fis.quantity_available,
    fis.reorder_point,
    -- Values
    fis.stock_value,
    fis.days_of_supply,
    -- Status
    fis.stock_status,
    fis.needs_reorder,
    fis.days_until_expiration,
    fis.expiration_risk,
    -- Allergen info
    di.is_allergen,
    di.allergen_type
FROM fact_inventory_snapshot fis
JOIN dim_location dl ON fis.location_sk = dl.location_sk
JOIN dim_ingredient di ON fis.ingredient_sk = di.ingredient_sk
ORDER BY dl.location_name, fis.stock_status, di.ingredient_name;
