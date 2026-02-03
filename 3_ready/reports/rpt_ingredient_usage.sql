/*
================================================================================
  REPORT: INGREDIENT USAGE & COST ANALYSIS
================================================================================
  Analyzes ingredient consumption, costs, and inventory health.
  Supports purchasing decisions and cost management.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

-- Ingredient cost contribution to menu items
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

-- Estimated ingredient usage from sales
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

-- Inventory status report
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

-- Inventory alerts (items needing attention)
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

-- Inventory value by location
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

-- Supplier spend summary
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
