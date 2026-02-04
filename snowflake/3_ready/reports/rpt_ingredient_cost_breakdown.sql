/*
================================================================================
  REPORT: INGREDIENT COST BREAKDOWN
================================================================================
  Ingredient cost contribution to menu items.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

CREATE OR REPLACE VIEW v_rpt_ingredient_cost_breakdown AS
SELECT 
    dm.item_name,
    dm.category,
    dm.base_price,
    di.ingredient_name,
    di.category AS ingredient_category,
    di.cost_per_unit,
    di.unit_of_measure,
    emii.quantity_required,
    emii.ingredient_cost,
    -- Percentage of item cost
    ROUND(emii.ingredient_cost / NULLIF(dm.food_cost, 0) * 100, 2) AS pct_of_food_cost,
    -- Is this a high-cost ingredient?
    CASE WHEN emii.ingredient_cost > 1.00 THEN 'High Cost' ELSE 'Standard' END AS cost_flag
FROM SAMMYS_ENRICHED.enriched_menu_item_ingredients emii
JOIN dim_menu_item dm ON emii.item_id = dm.item_id AND dm.is_current = TRUE
JOIN dim_ingredient di ON emii.ingredient_id = di.ingredient_id AND di.is_current = TRUE
ORDER BY dm.item_name, emii.ingredient_cost DESC;
