/*
================================================================================
  STORED PROCEDURE: SP_RUN_DATA_QUALITY_CHECKS
================================================================================
  Comprehensive data quality validation across all layers.
  Performs null checks, duplicate checks, referential integrity, range validation,
  and freshness checks. Results stored in SAMMYS_DATA_QUALITY schema.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_DATA_QUALITY;

CREATE OR REPLACE PROCEDURE sp_run_data_quality_checks(
    p_layer VARCHAR DEFAULT 'all'  -- 'raw', 'enriched', 'ready', or 'all'
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_run_id VARCHAR;
    v_start_time TIMESTAMP_NTZ;
    v_end_time TIMESTAMP_NTZ;
    v_duration_seconds NUMBER DEFAULT 0;
    v_total_checks INTEGER DEFAULT 0;
    v_passed INTEGER DEFAULT 0;
    v_warned INTEGER DEFAULT 0;
    v_failed INTEGER DEFAULT 0;
    v_overall_status VARCHAR;
BEGIN
    -- Generate unique run ID
    v_run_id := 'DQ_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS') || '_' || RANDOM();
    v_start_time := CURRENT_TIMESTAMP();
    
    -- ========================================================================
    -- RAW LAYER CHECKS
    -- ========================================================================
    IF (p_layer = 'all' OR p_layer = 'raw') THEN
        
        -- --------------------------------------------------------------------
        -- NULL CHECKS - Required fields in raw tables
        -- --------------------------------------------------------------------
        
        -- Orders: order_id required
        INSERT INTO data_quality_results (run_id, run_timestamp, check_category, check_name, layer, schema_name, table_name, column_name, status, records_checked, records_failed, failure_percentage, check_sql)
        SELECT 
            :v_run_id,
            :v_start_time,
            'null_check',
            'Order ID cannot be null',
            'raw',
            'SAMMYS_RAW',
            'orders',
            'order_id',
            CASE 
                WHEN COUNT_IF(order_id IS NULL) = 0 THEN 'PASSED'
                WHEN COUNT_IF(order_id IS NULL) / NULLIF(COUNT(*), 0) < 0.05 THEN 'WARNING'
                ELSE 'FAILED'
            END,
            COUNT(*),
            COUNT_IF(order_id IS NULL),
            ROUND(COUNT_IF(order_id IS NULL) / NULLIF(COUNT(*), 0) * 100, 4),
            'SELECT COUNT(*) FROM SAMMYS_RAW.orders WHERE order_id IS NULL'
        FROM SAMMYS_RAW.orders;
        
        -- Customers: customer_id required
        INSERT INTO data_quality_results (run_id, run_timestamp, check_category, check_name, layer, schema_name, table_name, column_name, status, records_checked, records_failed, failure_percentage, check_sql)
        SELECT 
            :v_run_id,
            :v_start_time,
            'null_check',
            'Customer ID cannot be null',
            'raw',
            'SAMMYS_RAW',
            'customers',
            'customer_id',
            CASE 
                WHEN COUNT_IF(customer_id IS NULL) = 0 THEN 'PASSED'
                WHEN COUNT_IF(customer_id IS NULL) / NULLIF(COUNT(*), 0) < 0.05 THEN 'WARNING'
                ELSE 'FAILED'
            END,
            COUNT(*),
            COUNT_IF(customer_id IS NULL),
            ROUND(COUNT_IF(customer_id IS NULL) / NULLIF(COUNT(*), 0) * 100, 4),
            'SELECT COUNT(*) FROM SAMMYS_RAW.customers WHERE customer_id IS NULL'
        FROM SAMMYS_RAW.customers;
        
        -- Locations: location_id required
        INSERT INTO data_quality_results (run_id, run_timestamp, check_category, check_name, layer, schema_name, table_name, column_name, status, records_checked, records_failed, failure_percentage, check_sql)
        SELECT 
            :v_run_id,
            :v_start_time,
            'null_check',
            'Location ID cannot be null',
            'raw',
            'SAMMYS_RAW',
            'locations',
            'location_id',
            CASE 
                WHEN COUNT_IF(location_id IS NULL) = 0 THEN 'PASSED'
                WHEN COUNT_IF(location_id IS NULL) / NULLIF(COUNT(*), 0) < 0.05 THEN 'WARNING'
                ELSE 'FAILED'
            END,
            COUNT(*),
            COUNT_IF(location_id IS NULL),
            ROUND(COUNT_IF(location_id IS NULL) / NULLIF(COUNT(*), 0) * 100, 4),
            'SELECT COUNT(*) FROM SAMMYS_RAW.locations WHERE location_id IS NULL'
        FROM SAMMYS_RAW.locations;
        
        -- Employees: employee_id required
        INSERT INTO data_quality_results (run_id, run_timestamp, check_category, check_name, layer, schema_name, table_name, column_name, status, records_checked, records_failed, failure_percentage, check_sql)
        SELECT 
            :v_run_id,
            :v_start_time,
            'null_check',
            'Employee ID cannot be null',
            'raw',
            'SAMMYS_RAW',
            'employees',
            'employee_id',
            CASE 
                WHEN COUNT_IF(employee_id IS NULL) = 0 THEN 'PASSED'
                WHEN COUNT_IF(employee_id IS NULL) / NULLIF(COUNT(*), 0) < 0.05 THEN 'WARNING'
                ELSE 'FAILED'
            END,
            COUNT(*),
            COUNT_IF(employee_id IS NULL),
            ROUND(COUNT_IF(employee_id IS NULL) / NULLIF(COUNT(*), 0) * 100, 4),
            'SELECT COUNT(*) FROM SAMMYS_RAW.employees WHERE employee_id IS NULL'
        FROM SAMMYS_RAW.employees;
        
        -- Menu Items: item_id required
        INSERT INTO data_quality_results (run_id, run_timestamp, check_category, check_name, layer, schema_name, table_name, column_name, status, records_checked, records_failed, failure_percentage, check_sql)
        SELECT 
            :v_run_id,
            :v_start_time,
            'null_check',
            'Menu Item ID cannot be null',
            'raw',
            'SAMMYS_RAW',
            'menu_items',
            'item_id',
            CASE 
                WHEN COUNT_IF(item_id IS NULL) = 0 THEN 'PASSED'
                WHEN COUNT_IF(item_id IS NULL) / NULLIF(COUNT(*), 0) < 0.05 THEN 'WARNING'
                ELSE 'FAILED'
            END,
            COUNT(*),
            COUNT_IF(item_id IS NULL),
            ROUND(COUNT_IF(item_id IS NULL) / NULLIF(COUNT(*), 0) * 100, 4),
            'SELECT COUNT(*) FROM SAMMYS_RAW.menu_items WHERE item_id IS NULL'
        FROM SAMMYS_RAW.menu_items;
        
        -- --------------------------------------------------------------------
        -- DUPLICATE CHECKS - Primary keys should be unique
        -- --------------------------------------------------------------------
        
        -- Orders: Duplicate order_id
        INSERT INTO data_quality_results (run_id, run_timestamp, check_category, check_name, layer, schema_name, table_name, column_name, status, records_checked, records_failed, failure_percentage, check_sql)
        SELECT 
            :v_run_id,
            :v_start_time,
            'duplicate_check',
            'Order ID must be unique',
            'raw',
            'SAMMYS_RAW',
            'orders',
            'order_id',
            CASE 
                WHEN COUNT(*) - COUNT(DISTINCT order_id) = 0 THEN 'PASSED'
                ELSE 'FAILED'
            END,
            COUNT(*),
            COUNT(*) - COUNT(DISTINCT order_id),
            ROUND((COUNT(*) - COUNT(DISTINCT order_id)) / NULLIF(COUNT(*), 0) * 100, 4),
            'SELECT order_id, COUNT(*) FROM SAMMYS_RAW.orders GROUP BY order_id HAVING COUNT(*) > 1'
        FROM SAMMYS_RAW.orders;
        
        -- Customers: Duplicate customer_id
        INSERT INTO data_quality_results (run_id, run_timestamp, check_category, check_name, layer, schema_name, table_name, column_name, status, records_checked, records_failed, failure_percentage, check_sql)
        SELECT 
            :v_run_id,
            :v_start_time,
            'duplicate_check',
            'Customer ID must be unique',
            'raw',
            'SAMMYS_RAW',
            'customers',
            'customer_id',
            CASE 
                WHEN COUNT(*) - COUNT(DISTINCT customer_id) = 0 THEN 'PASSED'
                ELSE 'FAILED'
            END,
            COUNT(*),
            COUNT(*) - COUNT(DISTINCT customer_id),
            ROUND((COUNT(*) - COUNT(DISTINCT customer_id)) / NULLIF(COUNT(*), 0) * 100, 4),
            'SELECT customer_id, COUNT(*) FROM SAMMYS_RAW.customers GROUP BY customer_id HAVING COUNT(*) > 1'
        FROM SAMMYS_RAW.customers;
        
        -- Locations: Duplicate location_id
        INSERT INTO data_quality_results (run_id, run_timestamp, check_category, check_name, layer, schema_name, table_name, column_name, status, records_checked, records_failed, failure_percentage, check_sql)
        SELECT 
            :v_run_id,
            :v_start_time,
            'duplicate_check',
            'Location ID must be unique',
            'raw',
            'SAMMYS_RAW',
            'locations',
            'location_id',
            CASE 
                WHEN COUNT(*) - COUNT(DISTINCT location_id) = 0 THEN 'PASSED'
                ELSE 'FAILED'
            END,
            COUNT(*),
            COUNT(*) - COUNT(DISTINCT location_id),
            ROUND((COUNT(*) - COUNT(DISTINCT location_id)) / NULLIF(COUNT(*), 0) * 100, 4),
            'SELECT location_id, COUNT(*) FROM SAMMYS_RAW.locations GROUP BY location_id HAVING COUNT(*) > 1'
        FROM SAMMYS_RAW.locations;
        
    END IF;
    
    -- ========================================================================
    -- ENRICHED LAYER CHECKS
    -- ========================================================================
    IF (p_layer = 'all' OR p_layer = 'enriched') THEN
        
        -- --------------------------------------------------------------------
        -- NULL CHECKS - Required fields after enrichment
        -- --------------------------------------------------------------------
        
        -- Enriched Orders: order_id required
        INSERT INTO data_quality_results (run_id, run_timestamp, check_category, check_name, layer, schema_name, table_name, column_name, status, records_checked, records_failed, failure_percentage, check_sql)
        SELECT 
            :v_run_id,
            :v_start_time,
            'null_check',
            'Order ID cannot be null',
            'enriched',
            'SAMMYS_ENRICHED',
            'enriched_orders',
            'order_id',
            CASE 
                WHEN COUNT_IF(order_id IS NULL) = 0 THEN 'PASSED'
                ELSE 'FAILED'
            END,
            COUNT(*),
            COUNT_IF(order_id IS NULL),
            ROUND(COUNT_IF(order_id IS NULL) / NULLIF(COUNT(*), 0) * 100, 4),
            'SELECT COUNT(*) FROM SAMMYS_ENRICHED.enriched_orders WHERE order_id IS NULL'
        FROM SAMMYS_ENRICHED.enriched_orders;
        
        -- Enriched Orders: order_date should be valid date
        INSERT INTO data_quality_results (run_id, run_timestamp, check_category, check_name, layer, schema_name, table_name, column_name, status, records_checked, records_failed, failure_percentage, check_sql)
        SELECT 
            :v_run_id,
            :v_start_time,
            'null_check',
            'Order date should not be null after enrichment',
            'enriched',
            'SAMMYS_ENRICHED',
            'enriched_orders',
            'order_date',
            CASE 
                WHEN COUNT_IF(order_date IS NULL) = 0 THEN 'PASSED'
                WHEN COUNT_IF(order_date IS NULL) / NULLIF(COUNT(*), 0) < 0.01 THEN 'WARNING'
                ELSE 'FAILED'
            END,
            COUNT(*),
            COUNT_IF(order_date IS NULL),
            ROUND(COUNT_IF(order_date IS NULL) / NULLIF(COUNT(*), 0) * 100, 4),
            'SELECT COUNT(*) FROM SAMMYS_ENRICHED.enriched_orders WHERE order_date IS NULL'
        FROM SAMMYS_ENRICHED.enriched_orders;
        
        -- --------------------------------------------------------------------
        -- REFERENTIAL INTEGRITY CHECKS
        -- --------------------------------------------------------------------
        
        -- Orders -> Locations: location_id must exist
        INSERT INTO data_quality_results (run_id, run_timestamp, check_category, check_name, layer, schema_name, table_name, column_name, status, records_checked, records_failed, failure_percentage, check_sql, error_details)
        SELECT 
            :v_run_id,
            :v_start_time,
            'referential_integrity',
            'Order location_id must exist in enriched_locations',
            'enriched',
            'SAMMYS_ENRICHED',
            'enriched_orders',
            'location_id',
            CASE 
                WHEN COUNT_IF(l.location_id IS NULL) = 0 THEN 'PASSED'
                WHEN COUNT_IF(l.location_id IS NULL) / NULLIF(COUNT(*), 0) < 0.01 THEN 'WARNING'
                ELSE 'FAILED'
            END,
            COUNT(*),
            COUNT_IF(l.location_id IS NULL AND o.location_id IS NOT NULL),
            ROUND(COUNT_IF(l.location_id IS NULL AND o.location_id IS NOT NULL) / NULLIF(COUNT(*), 0) * 100, 4),
            'SELECT o.order_id, o.location_id FROM SAMMYS_ENRICHED.enriched_orders o LEFT JOIN SAMMYS_ENRICHED.enriched_locations l ON o.location_id = l.location_id WHERE l.location_id IS NULL AND o.location_id IS NOT NULL',
            LISTAGG(DISTINCT CASE WHEN l.location_id IS NULL AND o.location_id IS NOT NULL THEN o.location_id END, ', ')
        FROM SAMMYS_ENRICHED.enriched_orders o
        LEFT JOIN SAMMYS_ENRICHED.enriched_locations l ON o.location_id = l.location_id;
        
        -- Orders -> Employees: employee_id must exist
        INSERT INTO data_quality_results (run_id, run_timestamp, check_category, check_name, layer, schema_name, table_name, column_name, status, records_checked, records_failed, failure_percentage, check_sql, error_details)
        SELECT 
            :v_run_id,
            :v_start_time,
            'referential_integrity',
            'Order employee_id must exist in enriched_employees',
            'enriched',
            'SAMMYS_ENRICHED',
            'enriched_orders',
            'employee_id',
            CASE 
                WHEN COUNT_IF(e.employee_id IS NULL AND o.employee_id IS NOT NULL) = 0 THEN 'PASSED'
                WHEN COUNT_IF(e.employee_id IS NULL AND o.employee_id IS NOT NULL) / NULLIF(COUNT(*), 0) < 0.01 THEN 'WARNING'
                ELSE 'FAILED'
            END,
            COUNT(*),
            COUNT_IF(e.employee_id IS NULL AND o.employee_id IS NOT NULL),
            ROUND(COUNT_IF(e.employee_id IS NULL AND o.employee_id IS NOT NULL) / NULLIF(COUNT(*), 0) * 100, 4),
            'SELECT o.order_id, o.employee_id FROM SAMMYS_ENRICHED.enriched_orders o LEFT JOIN SAMMYS_ENRICHED.enriched_employees e ON o.employee_id = e.employee_id WHERE e.employee_id IS NULL AND o.employee_id IS NOT NULL',
            LISTAGG(DISTINCT CASE WHEN e.employee_id IS NULL AND o.employee_id IS NOT NULL THEN o.employee_id END, ', ')
        FROM SAMMYS_ENRICHED.enriched_orders o
        LEFT JOIN SAMMYS_ENRICHED.enriched_employees e ON o.employee_id = e.employee_id;
        
        -- Order Items -> Orders: order_id must exist
        INSERT INTO data_quality_results (run_id, run_timestamp, check_category, check_name, layer, schema_name, table_name, column_name, status, records_checked, records_failed, failure_percentage, check_sql)
        SELECT 
            :v_run_id,
            :v_start_time,
            'referential_integrity',
            'Order item order_id must exist in enriched_orders',
            'enriched',
            'SAMMYS_ENRICHED',
            'enriched_order_items',
            'order_id',
            CASE 
                WHEN COUNT_IF(o.order_id IS NULL) = 0 THEN 'PASSED'
                WHEN COUNT_IF(o.order_id IS NULL) / NULLIF(COUNT(*), 0) < 0.01 THEN 'WARNING'
                ELSE 'FAILED'
            END,
            COUNT(*),
            COUNT_IF(o.order_id IS NULL),
            ROUND(COUNT_IF(o.order_id IS NULL) / NULLIF(COUNT(*), 0) * 100, 4),
            'SELECT oi.order_item_id, oi.order_id FROM SAMMYS_ENRICHED.enriched_order_items oi LEFT JOIN SAMMYS_ENRICHED.enriched_orders o ON oi.order_id = o.order_id WHERE o.order_id IS NULL'
        FROM SAMMYS_ENRICHED.enriched_order_items oi
        LEFT JOIN SAMMYS_ENRICHED.enriched_orders o ON oi.order_id = o.order_id;
        
        -- Order Items -> Menu Items: item_id must exist
        INSERT INTO data_quality_results (run_id, run_timestamp, check_category, check_name, layer, schema_name, table_name, column_name, status, records_checked, records_failed, failure_percentage, check_sql)
        SELECT 
            :v_run_id,
            :v_start_time,
            'referential_integrity',
            'Order item item_id must exist in enriched_menu_items',
            'enriched',
            'SAMMYS_ENRICHED',
            'enriched_order_items',
            'item_id',
            CASE 
                WHEN COUNT_IF(m.item_id IS NULL AND oi.item_id IS NOT NULL) = 0 THEN 'PASSED'
                WHEN COUNT_IF(m.item_id IS NULL AND oi.item_id IS NOT NULL) / NULLIF(COUNT(*), 0) < 0.01 THEN 'WARNING'
                ELSE 'FAILED'
            END,
            COUNT(*),
            COUNT_IF(m.item_id IS NULL AND oi.item_id IS NOT NULL),
            ROUND(COUNT_IF(m.item_id IS NULL AND oi.item_id IS NOT NULL) / NULLIF(COUNT(*), 0) * 100, 4),
            'SELECT oi.order_item_id, oi.item_id FROM SAMMYS_ENRICHED.enriched_order_items oi LEFT JOIN SAMMYS_ENRICHED.enriched_menu_items m ON oi.item_id = m.item_id WHERE m.item_id IS NULL AND oi.item_id IS NOT NULL'
        FROM SAMMYS_ENRICHED.enriched_order_items oi
        LEFT JOIN SAMMYS_ENRICHED.enriched_menu_items m ON oi.item_id = m.item_id;
        
        -- Ingredients -> Suppliers: supplier_id must exist
        INSERT INTO data_quality_results (run_id, run_timestamp, check_category, check_name, layer, schema_name, table_name, column_name, status, records_checked, records_failed, failure_percentage, check_sql)
        SELECT 
            :v_run_id,
            :v_start_time,
            'referential_integrity',
            'Ingredient supplier_id must exist in enriched_suppliers',
            'enriched',
            'SAMMYS_ENRICHED',
            'enriched_ingredients',
            'supplier_id',
            CASE 
                WHEN COUNT_IF(s.supplier_id IS NULL AND i.supplier_id IS NOT NULL) = 0 THEN 'PASSED'
                WHEN COUNT_IF(s.supplier_id IS NULL AND i.supplier_id IS NOT NULL) / NULLIF(COUNT(*), 0) < 0.01 THEN 'WARNING'
                ELSE 'FAILED'
            END,
            COUNT(*),
            COUNT_IF(s.supplier_id IS NULL AND i.supplier_id IS NOT NULL),
            ROUND(COUNT_IF(s.supplier_id IS NULL AND i.supplier_id IS NOT NULL) / NULLIF(COUNT(*), 0) * 100, 4),
            'SELECT i.ingredient_id, i.supplier_id FROM SAMMYS_ENRICHED.enriched_ingredients i LEFT JOIN SAMMYS_ENRICHED.enriched_suppliers s ON i.supplier_id = s.supplier_id WHERE s.supplier_id IS NULL AND i.supplier_id IS NOT NULL'
        FROM SAMMYS_ENRICHED.enriched_ingredients i
        LEFT JOIN SAMMYS_ENRICHED.enriched_suppliers s ON i.supplier_id = s.supplier_id;
        
        -- --------------------------------------------------------------------
        -- RANGE/VALUE CHECKS
        -- --------------------------------------------------------------------
        
        -- Orders: total_amount must be positive
        INSERT INTO data_quality_results (run_id, run_timestamp, check_category, check_name, layer, schema_name, table_name, column_name, status, records_checked, records_failed, failure_percentage, check_sql)
        SELECT 
            :v_run_id,
            :v_start_time,
            'range_check',
            'Order total_amount must be >= 0',
            'enriched',
            'SAMMYS_ENRICHED',
            'enriched_orders',
            'total_amount',
            CASE 
                WHEN COUNT_IF(total_amount < 0) = 0 THEN 'PASSED'
                ELSE 'FAILED'
            END,
            COUNT(*),
            COUNT_IF(total_amount < 0),
            ROUND(COUNT_IF(total_amount < 0) / NULLIF(COUNT(*), 0) * 100, 4),
            'SELECT order_id, total_amount FROM SAMMYS_ENRICHED.enriched_orders WHERE total_amount < 0'
        FROM SAMMYS_ENRICHED.enriched_orders;
        
        -- Orders: subtotal should equal or exceed discount
        INSERT INTO data_quality_results (run_id, run_timestamp, check_category, check_name, layer, schema_name, table_name, column_name, status, records_checked, records_failed, failure_percentage, check_sql)
        SELECT 
            :v_run_id,
            :v_start_time,
            'range_check',
            'Order discount_amount should not exceed subtotal',
            'enriched',
            'SAMMYS_ENRICHED',
            'enriched_orders',
            'discount_amount',
            CASE 
                WHEN COUNT_IF(discount_amount > subtotal) = 0 THEN 'PASSED'
                WHEN COUNT_IF(discount_amount > subtotal) / NULLIF(COUNT(*), 0) < 0.01 THEN 'WARNING'
                ELSE 'FAILED'
            END,
            COUNT(*),
            COUNT_IF(discount_amount > subtotal),
            ROUND(COUNT_IF(discount_amount > subtotal) / NULLIF(COUNT(*), 0) * 100, 4),
            'SELECT order_id, subtotal, discount_amount FROM SAMMYS_ENRICHED.enriched_orders WHERE discount_amount > subtotal'
        FROM SAMMYS_ENRICHED.enriched_orders;
        
        -- Menu Items: base_price must be positive
        INSERT INTO data_quality_results (run_id, run_timestamp, check_category, check_name, layer, schema_name, table_name, column_name, status, records_checked, records_failed, failure_percentage, check_sql)
        SELECT 
            :v_run_id,
            :v_start_time,
            'range_check',
            'Menu item base_price must be > 0',
            'enriched',
            'SAMMYS_ENRICHED',
            'enriched_menu_items',
            'base_price',
            CASE 
                WHEN COUNT_IF(base_price <= 0 OR base_price IS NULL) = 0 THEN 'PASSED'
                ELSE 'FAILED'
            END,
            COUNT(*),
            COUNT_IF(base_price <= 0 OR base_price IS NULL),
            ROUND(COUNT_IF(base_price <= 0 OR base_price IS NULL) / NULLIF(COUNT(*), 0) * 100, 4),
            'SELECT item_id, item_name, base_price FROM SAMMYS_ENRICHED.enriched_menu_items WHERE base_price <= 0 OR base_price IS NULL'
        FROM SAMMYS_ENRICHED.enriched_menu_items;
        
        -- Employees: hourly_rate must be reasonable (e.g., $7.25 to $100)
        INSERT INTO data_quality_results (run_id, run_timestamp, check_category, check_name, layer, schema_name, table_name, column_name, status, records_checked, records_failed, failure_percentage, check_sql)
        SELECT 
            :v_run_id,
            :v_start_time,
            'range_check',
            'Employee hourly_rate should be between $7.25 and $100',
            'enriched',
            'SAMMYS_ENRICHED',
            'enriched_employees',
            'hourly_rate',
            CASE 
                WHEN COUNT_IF(hourly_rate < 7.25 OR hourly_rate > 100) = 0 THEN 'PASSED'
                WHEN COUNT_IF(hourly_rate < 7.25 OR hourly_rate > 100) / NULLIF(COUNT(*), 0) < 0.05 THEN 'WARNING'
                ELSE 'FAILED'
            END,
            COUNT(*),
            COUNT_IF(hourly_rate < 7.25 OR hourly_rate > 100),
            ROUND(COUNT_IF(hourly_rate < 7.25 OR hourly_rate > 100) / NULLIF(COUNT(*), 0) * 100, 4),
            'SELECT employee_id, full_name, hourly_rate FROM SAMMYS_ENRICHED.enriched_employees WHERE hourly_rate < 7.25 OR hourly_rate > 100'
        FROM SAMMYS_ENRICHED.enriched_employees;
        
        -- Inventory: quantity_on_hand must be non-negative
        INSERT INTO data_quality_results (run_id, run_timestamp, check_category, check_name, layer, schema_name, table_name, column_name, status, records_checked, records_failed, failure_percentage, check_sql)
        SELECT 
            :v_run_id,
            :v_start_time,
            'range_check',
            'Inventory quantity_on_hand must be >= 0',
            'enriched',
            'SAMMYS_ENRICHED',
            'enriched_inventory',
            'quantity_on_hand',
            CASE 
                WHEN COUNT_IF(quantity_on_hand < 0) = 0 THEN 'PASSED'
                ELSE 'FAILED'
            END,
            COUNT(*),
            COUNT_IF(quantity_on_hand < 0),
            ROUND(COUNT_IF(quantity_on_hand < 0) / NULLIF(COUNT(*), 0) * 100, 4),
            'SELECT inventory_id, quantity_on_hand FROM SAMMYS_ENRICHED.enriched_inventory WHERE quantity_on_hand < 0'
        FROM SAMMYS_ENRICHED.enriched_inventory;
        
        -- --------------------------------------------------------------------
        -- ROW COUNT CHECKS (Raw vs Enriched)
        -- --------------------------------------------------------------------
        
        -- Orders row count comparison
        INSERT INTO data_quality_results (run_id, run_timestamp, check_category, check_name, layer, schema_name, table_name, column_name, status, records_checked, records_failed, failure_percentage, check_sql, error_details)
        SELECT 
            :v_run_id,
            :v_start_time,
            'row_count_check',
            'Enriched orders count should match raw orders (minus invalid)',
            'enriched',
            'SAMMYS_ENRICHED',
            'enriched_orders',
            NULL,
            CASE 
                WHEN ABS(enriched_count - raw_valid_count) / NULLIF(raw_valid_count, 0) < 0.01 THEN 'PASSED'
                WHEN ABS(enriched_count - raw_valid_count) / NULLIF(raw_valid_count, 0) < 0.05 THEN 'WARNING'
                ELSE 'FAILED'
            END,
            raw_valid_count,
            ABS(enriched_count - raw_valid_count),
            ROUND(ABS(enriched_count - raw_valid_count) / NULLIF(raw_valid_count, 0) * 100, 4),
            'SELECT (SELECT COUNT(*) FROM SAMMYS_RAW.orders WHERE order_id IS NOT NULL) as raw_count, (SELECT COUNT(*) FROM SAMMYS_ENRICHED.enriched_orders) as enriched_count',
            'Raw (valid): ' || raw_valid_count || ', Enriched: ' || enriched_count
        FROM (
            SELECT 
                (SELECT COUNT(*) FROM SAMMYS_RAW.orders WHERE order_id IS NOT NULL) AS raw_valid_count,
                (SELECT COUNT(*) FROM SAMMYS_ENRICHED.enriched_orders) AS enriched_count
        );
        
        -- Customers row count comparison
        INSERT INTO data_quality_results (run_id, run_timestamp, check_category, check_name, layer, schema_name, table_name, column_name, status, records_checked, records_failed, failure_percentage, check_sql, error_details)
        SELECT 
            :v_run_id,
            :v_start_time,
            'row_count_check',
            'Enriched customers count should match raw customers',
            'enriched',
            'SAMMYS_ENRICHED',
            'enriched_customers',
            NULL,
            CASE 
                WHEN ABS(enriched_count - raw_valid_count) / NULLIF(raw_valid_count, 0) < 0.01 THEN 'PASSED'
                WHEN ABS(enriched_count - raw_valid_count) / NULLIF(raw_valid_count, 0) < 0.05 THEN 'WARNING'
                ELSE 'FAILED'
            END,
            raw_valid_count,
            ABS(enriched_count - raw_valid_count),
            ROUND(ABS(enriched_count - raw_valid_count) / NULLIF(raw_valid_count, 0) * 100, 4),
            'SELECT (SELECT COUNT(*) FROM SAMMYS_RAW.customers WHERE customer_id IS NOT NULL) as raw_count, (SELECT COUNT(*) FROM SAMMYS_ENRICHED.enriched_customers) as enriched_count',
            'Raw (valid): ' || raw_valid_count || ', Enriched: ' || enriched_count
        FROM (
            SELECT 
                (SELECT COUNT(*) FROM SAMMYS_RAW.customers WHERE customer_id IS NOT NULL) AS raw_valid_count,
                (SELECT COUNT(*) FROM SAMMYS_ENRICHED.enriched_customers) AS enriched_count
        );
        
    END IF;
    
    -- ========================================================================
    -- READY LAYER CHECKS
    -- ========================================================================
    IF (p_layer = 'all' OR p_layer = 'ready') THEN
        
        -- --------------------------------------------------------------------
        -- REFERENTIAL INTEGRITY CHECKS (Fact -> Dim)
        -- --------------------------------------------------------------------
        
        -- Fact Sales -> Dim Customer
        INSERT INTO data_quality_results (run_id, run_timestamp, check_category, check_name, layer, schema_name, table_name, column_name, status, records_checked, records_failed, failure_percentage, check_sql)
        SELECT 
            :v_run_id,
            :v_start_time,
            'referential_integrity',
            'Fact sales customer_sk must exist in dim_customer',
            'ready',
            'SAMMYS_READY',
            'fact_sales',
            'customer_sk',
            CASE 
                WHEN COUNT_IF(dc.customer_sk IS NULL AND fs.customer_sk IS NOT NULL) = 0 THEN 'PASSED'
                WHEN COUNT_IF(dc.customer_sk IS NULL AND fs.customer_sk IS NOT NULL) / NULLIF(COUNT(*), 0) < 0.01 THEN 'WARNING'
                ELSE 'FAILED'
            END,
            COUNT(*),
            COUNT_IF(dc.customer_sk IS NULL AND fs.customer_sk IS NOT NULL),
            ROUND(COUNT_IF(dc.customer_sk IS NULL AND fs.customer_sk IS NOT NULL) / NULLIF(COUNT(*), 0) * 100, 4),
            'SELECT fs.order_id, fs.customer_sk FROM SAMMYS_READY.fact_sales fs LEFT JOIN SAMMYS_READY.dim_customer dc ON fs.customer_sk = dc.customer_sk WHERE dc.customer_sk IS NULL AND fs.customer_sk IS NOT NULL'
        FROM SAMMYS_READY.fact_sales fs
        LEFT JOIN SAMMYS_READY.dim_customer dc ON fs.customer_sk = dc.customer_sk;
        
        -- Fact Sales -> Dim Location
        INSERT INTO data_quality_results (run_id, run_timestamp, check_category, check_name, layer, schema_name, table_name, column_name, status, records_checked, records_failed, failure_percentage, check_sql)
        SELECT 
            :v_run_id,
            :v_start_time,
            'referential_integrity',
            'Fact sales location_sk must exist in dim_location',
            'ready',
            'SAMMYS_READY',
            'fact_sales',
            'location_sk',
            CASE 
                WHEN COUNT_IF(dl.location_sk IS NULL AND fs.location_sk IS NOT NULL) = 0 THEN 'PASSED'
                WHEN COUNT_IF(dl.location_sk IS NULL AND fs.location_sk IS NOT NULL) / NULLIF(COUNT(*), 0) < 0.01 THEN 'WARNING'
                ELSE 'FAILED'
            END,
            COUNT(*),
            COUNT_IF(dl.location_sk IS NULL AND fs.location_sk IS NOT NULL),
            ROUND(COUNT_IF(dl.location_sk IS NULL AND fs.location_sk IS NOT NULL) / NULLIF(COUNT(*), 0) * 100, 4),
            'SELECT fs.order_id, fs.location_sk FROM SAMMYS_READY.fact_sales fs LEFT JOIN SAMMYS_READY.dim_location dl ON fs.location_sk = dl.location_sk WHERE dl.location_sk IS NULL AND fs.location_sk IS NOT NULL'
        FROM SAMMYS_READY.fact_sales fs
        LEFT JOIN SAMMYS_READY.dim_location dl ON fs.location_sk = dl.location_sk;
        
        -- Fact Sales -> Dim Date
        INSERT INTO data_quality_results (run_id, run_timestamp, check_category, check_name, layer, schema_name, table_name, column_name, status, records_checked, records_failed, failure_percentage, check_sql)
        SELECT 
            :v_run_id,
            :v_start_time,
            'referential_integrity',
            'Fact sales date_key must exist in dim_date',
            'ready',
            'SAMMYS_READY',
            'fact_sales',
            'date_key',
            CASE 
                WHEN COUNT_IF(dd.date_key IS NULL) = 0 THEN 'PASSED'
                WHEN COUNT_IF(dd.date_key IS NULL) / NULLIF(COUNT(*), 0) < 0.01 THEN 'WARNING'
                ELSE 'FAILED'
            END,
            COUNT(*),
            COUNT_IF(dd.date_key IS NULL),
            ROUND(COUNT_IF(dd.date_key IS NULL) / NULLIF(COUNT(*), 0) * 100, 4),
            'SELECT fs.order_id, fs.date_key FROM SAMMYS_READY.fact_sales fs LEFT JOIN SAMMYS_READY.dim_date dd ON fs.date_key = dd.date_key WHERE dd.date_key IS NULL'
        FROM SAMMYS_READY.fact_sales fs
        LEFT JOIN SAMMYS_READY.dim_date dd ON fs.date_key = dd.date_key;
        
        -- Fact Sales Line Item -> Dim Menu Item
        INSERT INTO data_quality_results (run_id, run_timestamp, check_category, check_name, layer, schema_name, table_name, column_name, status, records_checked, records_failed, failure_percentage, check_sql)
        SELECT 
            :v_run_id,
            :v_start_time,
            'referential_integrity',
            'Fact sales line item menu_item_sk must exist in dim_menu_item',
            'ready',
            'SAMMYS_READY',
            'fact_sales_line_item',
            'menu_item_sk',
            CASE 
                WHEN COUNT_IF(dm.menu_item_sk IS NULL AND fsl.menu_item_sk IS NOT NULL) = 0 THEN 'PASSED'
                WHEN COUNT_IF(dm.menu_item_sk IS NULL AND fsl.menu_item_sk IS NOT NULL) / NULLIF(COUNT(*), 0) < 0.01 THEN 'WARNING'
                ELSE 'FAILED'
            END,
            COUNT(*),
            COUNT_IF(dm.menu_item_sk IS NULL AND fsl.menu_item_sk IS NOT NULL),
            ROUND(COUNT_IF(dm.menu_item_sk IS NULL AND fsl.menu_item_sk IS NOT NULL) / NULLIF(COUNT(*), 0) * 100, 4),
            'SELECT fsl.order_item_id, fsl.menu_item_sk FROM SAMMYS_READY.fact_sales_line_item fsl LEFT JOIN SAMMYS_READY.dim_menu_item dm ON fsl.menu_item_sk = dm.menu_item_sk WHERE dm.menu_item_sk IS NULL AND fsl.menu_item_sk IS NOT NULL'
        FROM SAMMYS_READY.fact_sales_line_item fsl
        LEFT JOIN SAMMYS_READY.dim_menu_item dm ON fsl.menu_item_sk = dm.menu_item_sk;
        
        -- --------------------------------------------------------------------
        -- AGGREGATE CONSISTENCY CHECKS
        -- --------------------------------------------------------------------
        
        -- Fact Daily Summary totals should match Fact Sales
        INSERT INTO data_quality_results (run_id, run_timestamp, check_category, check_name, layer, schema_name, table_name, column_name, status, records_checked, records_failed, failure_percentage, check_sql, error_details)
        SELECT 
            :v_run_id,
            :v_start_time,
            'aggregate_check',
            'Fact daily summary order count should match fact sales',
            'ready',
            'SAMMYS_READY',
            'fact_daily_summary',
            'total_orders',
            CASE 
                WHEN ABS(summary_orders - actual_orders) / NULLIF(actual_orders, 0) < 0.01 THEN 'PASSED'
                WHEN ABS(summary_orders - actual_orders) / NULLIF(actual_orders, 0) < 0.05 THEN 'WARNING'
                ELSE 'FAILED'
            END,
            actual_orders,
            ABS(summary_orders - actual_orders),
            ROUND(ABS(summary_orders - actual_orders) / NULLIF(actual_orders, 0) * 100, 4),
            'SELECT SUM(total_orders) FROM SAMMYS_READY.fact_daily_summary vs SELECT COUNT(*) FROM SAMMYS_READY.fact_sales',
            'Summary total: ' || summary_orders || ', Actual: ' || actual_orders
        FROM (
            SELECT 
                COALESCE((SELECT SUM(total_orders) FROM SAMMYS_READY.fact_daily_summary), 0) AS summary_orders,
                COALESCE((SELECT COUNT(*) FROM SAMMYS_READY.fact_sales), 0) AS actual_orders
        );
        
    END IF;
    
    -- ========================================================================
    -- CALCULATE SUMMARY STATISTICS
    -- ========================================================================
    
    SELECT 
        COUNT(*),
        COUNT_IF(status = 'PASSED'),
        COUNT_IF(status = 'WARNING'),
        COUNT_IF(status = 'FAILED')
    INTO v_total_checks, v_passed, v_warned, v_failed
    FROM data_quality_results
    WHERE run_id = :v_run_id;
    
    -- Determine overall status
    IF (v_failed > 0) THEN
        v_overall_status := 'FAILED';
    ELSEIF (v_warned > 0) THEN
        v_overall_status := 'WARNING';
    ELSE
        v_overall_status := 'PASSED';
    END IF;
    
    v_end_time := CURRENT_TIMESTAMP();
    v_duration_seconds := TIMESTAMPDIFF(SECOND, v_start_time, v_end_time);
    
    -- Insert run summary
    INSERT INTO data_quality_runs (
        run_id,
        run_timestamp,
        run_type,
        total_checks,
        checks_passed,
        checks_warned,
        checks_failed,
        overall_status,
        duration_seconds,
        triggered_by
    ) VALUES (
        :v_run_id,
        :v_start_time,
        :p_layer,
        :v_total_checks,
        :v_passed,
        :v_warned,
        :v_failed,
        :v_overall_status,
        :v_duration_seconds,
        'stored_procedure'
    );
    
    RETURN v_overall_status || ': ' || v_total_checks || ' checks completed - ' || 
           v_passed || ' passed, ' || v_warned || ' warnings, ' || v_failed || ' failed. Run ID: ' || v_run_id;
END;
$$;

-- Grant execute permissions
-- GRANT USAGE ON PROCEDURE sp_run_data_quality_checks(VARCHAR) TO ROLE <your_role>;
