/*
================================================================================
  REPORT: ESTIMATED INGREDIENT USAGE
================================================================================
  Estimated ingredient usage from sales data.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

CREATE OR REPLACE VIEW v_rpt_estimated_ingredient_usage AS
SELECT 
    di.ingredient_name,
    di.category,
    di.unit_of_measure,
    di.cost_per_unit,
    di.supplier_name,
    -- Estimated usage
    SUM(fsl.quantity * emii.quantity_required) AS total_quantity_used,
    -- Estimated cost
    ROUND(SUM(fsl.quantity * emii.ingredient_cost), 2) AS total_ingredient_cost,
    -- Number of menu items using this ingredient
    COUNT(DISTINCT emii.item_id) AS menu_items_using,
    -- Average daily usage
    ROUND(SUM(fsl.quantity * emii.quantity_required) / 
          NULLIF(COUNT(DISTINCT fsl.date_key), 0), 2) AS avg_daily_usage
FROM fact_sales_line_item fsl
JOIN SAMMYS_ENRICHED.enriched_menu_item_ingredients emii 
    ON fsl.menu_item_sk = (
        SELECT menu_item_sk FROM dim_menu_item 
        WHERE item_id = emii.item_id AND is_current = TRUE
    )
JOIN dim_ingredient di ON emii.ingredient_id = di.ingredient_id AND di.is_current = TRUE
JOIN dim_menu_item dm ON fsl.menu_item_sk = dm.menu_item_sk
WHERE dm.item_id = emii.item_id
GROUP BY di.ingredient_name, di.category, di.unit_of_measure, di.cost_per_unit, di.supplier_name
ORDER BY total_ingredient_cost DESC;
