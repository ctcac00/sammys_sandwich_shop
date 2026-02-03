/*
================================================================================
  SAMMY'S SANDWICH SHOP - FULL PIPELINE ORCHESTRATION
================================================================================
  Master script to execute the complete data transformation pipeline.
  Run this after initial setup and data loading to transform data through
  all three layers: RAW -> ENRICHED -> READY
================================================================================
*/

-- Set context
USE DATABASE SAMMYS_SANDWICH_SHOP;
USE WAREHOUSE SAMMYS_WH;

-- ============================================================================
-- STEP 1: VERIFY RAW DATA EXISTS
-- ============================================================================
SHOW TABLES IN SCHEMA SAMMYS_RAW;

-- Quick row counts to verify data loaded
SELECT 'RAW LAYER DATA COUNTS' AS layer;
SELECT 'locations' AS table_name, COUNT(*) AS row_count FROM SAMMYS_RAW.locations
UNION ALL SELECT 'customers', COUNT(*) FROM SAMMYS_RAW.customers
UNION ALL SELECT 'employees', COUNT(*) FROM SAMMYS_RAW.employees
UNION ALL SELECT 'menu_items', COUNT(*) FROM SAMMYS_RAW.menu_items
UNION ALL SELECT 'ingredients', COUNT(*) FROM SAMMYS_RAW.ingredients
UNION ALL SELECT 'menu_item_ingredients', COUNT(*) FROM SAMMYS_RAW.menu_item_ingredients
UNION ALL SELECT 'orders', COUNT(*) FROM SAMMYS_RAW.orders
UNION ALL SELECT 'order_items', COUNT(*) FROM SAMMYS_RAW.order_items
UNION ALL SELECT 'suppliers', COUNT(*) FROM SAMMYS_RAW.suppliers
UNION ALL SELECT 'inventory', COUNT(*) FROM SAMMYS_RAW.inventory;

-- ============================================================================
-- STEP 2: RUN ENRICHED LAYER TRANSFORMATIONS
-- ============================================================================
SELECT '=== RUNNING ENRICHED LAYER TRANSFORMATIONS ===' AS status;

-- Run all enrichment procedures
CALL SAMMYS_ENRICHED.sp_enrich_locations();
CALL SAMMYS_ENRICHED.sp_enrich_customers();
CALL SAMMYS_ENRICHED.sp_enrich_employees();
CALL SAMMYS_ENRICHED.sp_enrich_suppliers();
CALL SAMMYS_ENRICHED.sp_enrich_ingredients();
CALL SAMMYS_ENRICHED.sp_enrich_menu_items();
CALL SAMMYS_ENRICHED.sp_enrich_menu_item_ingredients();
CALL SAMMYS_ENRICHED.sp_enrich_order_items();
CALL SAMMYS_ENRICHED.sp_enrich_orders();
CALL SAMMYS_ENRICHED.sp_enrich_inventory();

-- Verify enriched layer
SELECT 'ENRICHED LAYER DATA COUNTS' AS layer;
SELECT 'enriched_locations' AS table_name, COUNT(*) AS row_count FROM SAMMYS_ENRICHED.enriched_locations
UNION ALL SELECT 'enriched_customers', COUNT(*) FROM SAMMYS_ENRICHED.enriched_customers
UNION ALL SELECT 'enriched_employees', COUNT(*) FROM SAMMYS_ENRICHED.enriched_employees
UNION ALL SELECT 'enriched_menu_items', COUNT(*) FROM SAMMYS_ENRICHED.enriched_menu_items
UNION ALL SELECT 'enriched_ingredients', COUNT(*) FROM SAMMYS_ENRICHED.enriched_ingredients
UNION ALL SELECT 'enriched_menu_item_ingredients', COUNT(*) FROM SAMMYS_ENRICHED.enriched_menu_item_ingredients
UNION ALL SELECT 'enriched_orders', COUNT(*) FROM SAMMYS_ENRICHED.enriched_orders
UNION ALL SELECT 'enriched_order_items', COUNT(*) FROM SAMMYS_ENRICHED.enriched_order_items
UNION ALL SELECT 'enriched_suppliers', COUNT(*) FROM SAMMYS_ENRICHED.enriched_suppliers
UNION ALL SELECT 'enriched_inventory', COUNT(*) FROM SAMMYS_ENRICHED.enriched_inventory;

-- ============================================================================
-- STEP 3: LOAD READY LAYER DIMENSIONS
-- ============================================================================
SELECT '=== LOADING READY LAYER DIMENSIONS ===' AS status;

-- Load date and time dimensions first (no dependencies)
CALL SAMMYS_READY.sp_load_dim_date('2020-01-01', '2026-12-31');
CALL SAMMYS_READY.sp_load_dim_time();

-- Load entity dimensions
CALL SAMMYS_READY.sp_load_dim_location();
CALL SAMMYS_READY.sp_load_dim_customer();
CALL SAMMYS_READY.sp_load_dim_employee();
CALL SAMMYS_READY.sp_load_dim_ingredient();
CALL SAMMYS_READY.sp_load_dim_menu_item();

-- Verify dimension tables
SELECT 'READY LAYER DIMENSION COUNTS' AS layer;
SELECT 'dim_date' AS table_name, COUNT(*) AS row_count FROM SAMMYS_READY.dim_date
UNION ALL SELECT 'dim_time', COUNT(*) FROM SAMMYS_READY.dim_time
UNION ALL SELECT 'dim_location', COUNT(*) FROM SAMMYS_READY.dim_location
UNION ALL SELECT 'dim_customer', COUNT(*) FROM SAMMYS_READY.dim_customer
UNION ALL SELECT 'dim_employee', COUNT(*) FROM SAMMYS_READY.dim_employee
UNION ALL SELECT 'dim_ingredient', COUNT(*) FROM SAMMYS_READY.dim_ingredient
UNION ALL SELECT 'dim_menu_item', COUNT(*) FROM SAMMYS_READY.dim_menu_item
UNION ALL SELECT 'dim_payment_method', COUNT(*) FROM SAMMYS_READY.dim_payment_method
UNION ALL SELECT 'dim_order_type', COUNT(*) FROM SAMMYS_READY.dim_order_type;

-- ============================================================================
-- STEP 4: LOAD READY LAYER FACTS
-- ============================================================================
SELECT '=== LOADING READY LAYER FACTS ===' AS status;

-- Load fact tables (depend on dimensions)
CALL SAMMYS_READY.sp_load_fact_sales();
CALL SAMMYS_READY.sp_load_fact_sales_line_item();
CALL SAMMYS_READY.sp_load_fact_inventory();
CALL SAMMYS_READY.sp_load_fact_daily_summary();

-- Load derived facts (depend on base facts)
CALL SAMMYS_READY.sp_load_fact_customer_activity();
CALL SAMMYS_READY.sp_load_fact_menu_item_performance();

-- Verify fact tables
SELECT 'READY LAYER FACT COUNTS' AS layer;
SELECT 'fact_sales' AS table_name, COUNT(*) AS row_count FROM SAMMYS_READY.fact_sales
UNION ALL SELECT 'fact_sales_line_item', COUNT(*) FROM SAMMYS_READY.fact_sales_line_item
UNION ALL SELECT 'fact_inventory_snapshot', COUNT(*) FROM SAMMYS_READY.fact_inventory_snapshot
UNION ALL SELECT 'fact_daily_summary', COUNT(*) FROM SAMMYS_READY.fact_daily_summary
UNION ALL SELECT 'fact_customer_activity', COUNT(*) FROM SAMMYS_READY.fact_customer_activity
UNION ALL SELECT 'fact_menu_item_performance', COUNT(*) FROM SAMMYS_READY.fact_menu_item_performance;

-- ============================================================================
-- STEP 5: VERIFY PIPELINE SUCCESS
-- ============================================================================
SELECT '=== PIPELINE COMPLETE - VERIFICATION ===' AS status;

-- Sample data from key tables
SELECT 'Top 5 Locations by Revenue' AS report;
SELECT * FROM SAMMYS_READY.v_rpt_location_ranking LIMIT 5;

SELECT 'Top 5 Menu Items' AS report;
SELECT item_name, category, total_revenue, revenue_rank 
FROM SAMMYS_READY.v_rpt_top_selling_items 
LIMIT 5;

SELECT 'Customer Segments' AS report;
SELECT * FROM SAMMYS_READY.v_rpt_rfm_segment_summary;

SELECT 'Inventory Alerts' AS report;
SELECT location_name, ingredient_name, alert_type, priority
FROM SAMMYS_READY.v_rpt_inventory_alerts
WHERE priority <= 2
LIMIT 10;

SELECT '=== PIPELINE EXECUTION COMPLETE ===' AS status;
