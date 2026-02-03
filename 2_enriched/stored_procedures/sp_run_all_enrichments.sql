/*
================================================================================
  STORED PROCEDURE: SP_RUN_ALL_ENRICHMENTS
================================================================================
  Master procedure to orchestrate all enrichment stored procedures.
  Runs procedures in dependency order to ensure data consistency.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_ENRICHED;

CREATE OR REPLACE PROCEDURE sp_run_all_enrichments()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_result VARCHAR;
    v_results ARRAY DEFAULT ARRAY_CONSTRUCT();
BEGIN
    -- Run enrichment procedures in dependency order
    
    -- No dependencies
    CALL sp_enrich_locations();
    v_results := ARRAY_APPEND(v_results, 'Locations: OK');
    
    CALL sp_enrich_customers();
    v_results := ARRAY_APPEND(v_results, 'Customers: OK');
    
    CALL sp_enrich_employees();
    v_results := ARRAY_APPEND(v_results, 'Employees: OK');
    
    CALL sp_enrich_suppliers();
    v_results := ARRAY_APPEND(v_results, 'Suppliers: OK');
    
    -- Ingredients depends on suppliers (for validation, not join)
    CALL sp_enrich_ingredients();
    v_results := ARRAY_APPEND(v_results, 'Ingredients: OK');
    
    -- Menu items has no dependencies
    CALL sp_enrich_menu_items();
    v_results := ARRAY_APPEND(v_results, 'Menu Items: OK');
    
    -- Menu item ingredients depends on ingredients (for cost lookup)
    CALL sp_enrich_menu_item_ingredients();
    v_results := ARRAY_APPEND(v_results, 'Menu Item Ingredients: OK');
    
    -- Orders and order items
    CALL sp_enrich_order_items();
    v_results := ARRAY_APPEND(v_results, 'Order Items: OK');
    
    CALL sp_enrich_orders();
    v_results := ARRAY_APPEND(v_results, 'Orders: OK');
    
    -- Inventory
    CALL sp_enrich_inventory();
    v_results := ARRAY_APPEND(v_results, 'Inventory: OK');
    
    RETURN 'ALL ENRICHMENTS COMPLETE: ' || ARRAY_TO_STRING(v_results, ', ');
END;
$$;
