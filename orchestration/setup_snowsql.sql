/*
================================================================================
  SAMMY'S SANDWICH SHOP - SNOWSQL DATA LOADING
================================================================================
  
  Best for: Command-line users, scripted deployments
  
  PREREQUISITES:
  1. Install SnowSQL: https://docs.snowflake.com/en/user-guide/snowsql-install-config
  2. Run 1_raw/ddl/00_setup.sql (creates database/schemas)
  3. Run 1_raw/ddl/01_raw_tables.sql (creates tables)
  
  USAGE:
  ------
  Step 1: Connect to Snowflake via terminal (from project root directory):
  
      snowsql -a <account> -u <username> -d SAMMYS_SANDWICH_SHOP -s SAMMYS_RAW
  
  Step 2: Upload CSV files to stage (run in SnowSQL):
  
      PUT file://1_raw/seed_data/locations.csv @seed_data_stage AUTO_COMPRESS=FALSE;
      PUT file://1_raw/seed_data/customers.csv @seed_data_stage AUTO_COMPRESS=FALSE;
      PUT file://1_raw/seed_data/employees.csv @seed_data_stage AUTO_COMPRESS=FALSE;
      PUT file://1_raw/seed_data/menu_items.csv @seed_data_stage AUTO_COMPRESS=FALSE;
      PUT file://1_raw/seed_data/ingredients.csv @seed_data_stage AUTO_COMPRESS=FALSE;
      PUT file://1_raw/seed_data/menu_item_ingredients.csv @seed_data_stage AUTO_COMPRESS=FALSE;
      PUT file://1_raw/seed_data/orders.csv @seed_data_stage AUTO_COMPRESS=FALSE;
      PUT file://1_raw/seed_data/order_items.csv @seed_data_stage AUTO_COMPRESS=FALSE;
      PUT file://1_raw/seed_data/suppliers.csv @seed_data_stage AUTO_COMPRESS=FALSE;
      PUT file://1_raw/seed_data/inventory.csv @seed_data_stage AUTO_COMPRESS=FALSE;
  
  Step 3: Run this file to copy data from stage to tables:
  
      !source orchestration/setup_snowsql.sql
  
================================================================================
*/

-- Set context
USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_RAW;
USE WAREHOUSE SAMMYS_WH;

-- Create internal stage for CSV files
CREATE OR REPLACE STAGE seed_data_stage
    FILE_FORMAT = (
        TYPE = 'CSV'
        FIELD_DELIMITER = ','
        SKIP_HEADER = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        NULL_IF = ('NULL', 'null', '')
        EMPTY_FIELD_AS_NULL = TRUE
    )
    COMMENT = 'Stage for loading seed data CSV files';

-- Verify files are staged
LIST @seed_data_stage;

-- Load locations
COPY INTO locations 
    (location_id, location_name, address, city, state, zip_code, phone, 
     manager_employee_id, open_date, seating_capacity, has_drive_thru)
FROM @seed_data_stage/locations.csv
ON_ERROR = 'CONTINUE';

-- Load customers
COPY INTO customers 
    (customer_id, first_name, last_name, email, phone, address, city, state, 
     zip_code, birth_date, signup_date, loyalty_tier, loyalty_points, 
     preferred_location, marketing_opt_in)
FROM @seed_data_stage/customers.csv
ON_ERROR = 'CONTINUE';

-- Load employees
COPY INTO employees 
    (employee_id, first_name, last_name, email, phone, hire_date, job_title,
     department, hourly_rate, location_id, manager_id, employment_status)
FROM @seed_data_stage/employees.csv
ON_ERROR = 'CONTINUE';

-- Load menu items
COPY INTO menu_items 
    (item_id, item_name, category, subcategory, description, base_price,
     is_active, is_seasonal, calories, prep_time_minutes, introduced_date)
FROM @seed_data_stage/menu_items.csv
ON_ERROR = 'CONTINUE';

-- Load ingredients
COPY INTO ingredients 
    (ingredient_id, ingredient_name, category, unit_of_measure, cost_per_unit,
     supplier_id, is_allergen, allergen_type, shelf_life_days, storage_type)
FROM @seed_data_stage/ingredients.csv
ON_ERROR = 'CONTINUE';

-- Load menu item ingredients
COPY INTO menu_item_ingredients 
    (item_id, ingredient_id, quantity_required, is_optional, extra_charge)
FROM @seed_data_stage/menu_item_ingredients.csv
ON_ERROR = 'CONTINUE';

-- Load orders
COPY INTO orders 
    (order_id, customer_id, employee_id, location_id, order_date, order_time,
     order_type, order_status, subtotal, tax_amount, discount_amount, tip_amount,
     total_amount, payment_method)
FROM @seed_data_stage/orders.csv
ON_ERROR = 'CONTINUE';

-- Load order items
COPY INTO order_items 
    (order_item_id, order_id, item_id, quantity, unit_price, customizations, line_total)
FROM @seed_data_stage/order_items.csv
ON_ERROR = 'CONTINUE';

-- Load suppliers
COPY INTO suppliers 
    (supplier_id, supplier_name, contact_name, email, phone, address, city,
     state, zip_code, payment_terms, lead_time_days, is_active)
FROM @seed_data_stage/suppliers.csv
ON_ERROR = 'CONTINUE';

-- Load inventory
COPY INTO inventory 
    (inventory_id, location_id, ingredient_id, snapshot_date, quantity_on_hand,
     quantity_reserved, reorder_point, reorder_quantity, last_restock_date, expiration_date)
FROM @seed_data_stage/inventory.csv
ON_ERROR = 'CONTINUE';

-- Verify data loaded
SELECT 'Data Loading Results' AS status;
SELECT 
    table_name,
    row_count,
    CASE WHEN row_count > 0 THEN 'OK' ELSE 'EMPTY' END AS status
FROM (
    SELECT 'locations' AS table_name, COUNT(*) AS row_count FROM locations
    UNION ALL SELECT 'customers', COUNT(*) FROM customers
    UNION ALL SELECT 'employees', COUNT(*) FROM employees
    UNION ALL SELECT 'menu_items', COUNT(*) FROM menu_items
    UNION ALL SELECT 'ingredients', COUNT(*) FROM ingredients
    UNION ALL SELECT 'menu_item_ingredients', COUNT(*) FROM menu_item_ingredients
    UNION ALL SELECT 'orders', COUNT(*) FROM orders
    UNION ALL SELECT 'order_items', COUNT(*) FROM order_items
    UNION ALL SELECT 'suppliers', COUNT(*) FROM suppliers
    UNION ALL SELECT 'inventory', COUNT(*) FROM inventory
)
ORDER BY table_name;

SELECT 'Seed data loading complete. Run orchestration/run_full_pipeline.sql next.' AS next_step;
