/*
================================================================================
  REPORT: SUPPLIER SPEND
================================================================================
  Supplier spend summary with inventory values.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

CREATE OR REPLACE VIEW v_rpt_supplier_spend AS
SELECT 
    di.supplier_name,
    di.supplier_id,
    COUNT(DISTINCT di.ingredient_id) AS ingredients_supplied,
    SUM(fis.stock_value) AS current_inventory_value,
    -- Ingredient categories supplied
    LISTAGG(DISTINCT di.category, ', ') WITHIN GROUP (ORDER BY di.category) AS categories_supplied
FROM fact_inventory_snapshot fis
JOIN dim_ingredient di ON fis.ingredient_sk = di.ingredient_sk
GROUP BY di.supplier_name, di.supplier_id
ORDER BY current_inventory_value DESC;
