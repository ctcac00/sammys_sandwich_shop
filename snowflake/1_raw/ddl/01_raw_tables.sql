/*
================================================================================
  SAMMY'S SANDWICH SHOP - RAW LAYER TABLE DEFINITIONS
================================================================================
  These tables store source data exactly as received.
  All tables include metadata columns for tracking data lineage.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_RAW;

-- ============================================================================
-- LOCATIONS - Store locations
-- ============================================================================
CREATE OR REPLACE TABLE locations (
    location_id         VARCHAR(10),
    location_name       VARCHAR(100),
    address             VARCHAR(200),
    city                VARCHAR(50),
    state               VARCHAR(2),
    zip_code            VARCHAR(10),
    phone               VARCHAR(20),
    manager_employee_id VARCHAR(10),
    open_date           VARCHAR(20),
    seating_capacity    VARCHAR(10),
    has_drive_thru      VARCHAR(5),
    -- Metadata columns
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(255)
);

-- ============================================================================
-- CUSTOMERS - Customer master data
-- ============================================================================
CREATE OR REPLACE TABLE customers (
    customer_id         VARCHAR(10),
    first_name          VARCHAR(50),
    last_name           VARCHAR(50),
    email               VARCHAR(100),
    phone               VARCHAR(20),
    address             VARCHAR(200),
    city                VARCHAR(50),
    state               VARCHAR(2),
    zip_code            VARCHAR(10),
    birth_date          VARCHAR(20),
    signup_date         VARCHAR(20),
    loyalty_tier        VARCHAR(20),
    loyalty_points      VARCHAR(10),
    preferred_location  VARCHAR(10),
    marketing_opt_in    VARCHAR(5),
    -- Metadata columns
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(255)
);

-- ============================================================================
-- EMPLOYEES - Employee data
-- ============================================================================
CREATE OR REPLACE TABLE employees (
    employee_id         VARCHAR(10),
    first_name          VARCHAR(50),
    last_name           VARCHAR(50),
    email               VARCHAR(100),
    phone               VARCHAR(20),
    hire_date           VARCHAR(20),
    job_title           VARCHAR(50),
    department          VARCHAR(50),
    hourly_rate         VARCHAR(10),
    location_id         VARCHAR(10),
    manager_id          VARCHAR(10),
    employment_status   VARCHAR(20),
    -- Metadata columns
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(255)
);

-- ============================================================================
-- MENU_ITEMS - Products offered
-- ============================================================================
CREATE OR REPLACE TABLE menu_items (
    item_id             VARCHAR(10),
    item_name           VARCHAR(100),
    category            VARCHAR(50),
    subcategory         VARCHAR(50),
    description         VARCHAR(500),
    base_price          VARCHAR(10),
    is_active           VARCHAR(5),
    is_seasonal         VARCHAR(5),
    calories            VARCHAR(10),
    prep_time_minutes   VARCHAR(10),
    introduced_date     VARCHAR(20),
    -- Metadata columns
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(255)
);

-- ============================================================================
-- INGREDIENTS - Raw ingredients
-- ============================================================================
CREATE OR REPLACE TABLE ingredients (
    ingredient_id       VARCHAR(10),
    ingredient_name     VARCHAR(100),
    category            VARCHAR(50),
    unit_of_measure     VARCHAR(20),
    cost_per_unit       VARCHAR(10),
    supplier_id         VARCHAR(10),
    is_allergen         VARCHAR(5),
    allergen_type       VARCHAR(50),
    shelf_life_days     VARCHAR(10),
    storage_type        VARCHAR(20),
    -- Metadata columns
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(255)
);

-- ============================================================================
-- MENU_ITEM_INGREDIENTS - Recipe/BOM junction table
-- ============================================================================
CREATE OR REPLACE TABLE menu_item_ingredients (
    item_id             VARCHAR(10),
    ingredient_id       VARCHAR(10),
    quantity_required   VARCHAR(10),
    is_optional         VARCHAR(5),
    extra_charge        VARCHAR(10),
    -- Metadata columns
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(255)
);

-- ============================================================================
-- ORDERS - Order headers
-- ============================================================================
CREATE OR REPLACE TABLE orders (
    order_id            VARCHAR(20),
    customer_id         VARCHAR(10),
    employee_id         VARCHAR(10),
    location_id         VARCHAR(10),
    order_date          VARCHAR(30),
    order_time          VARCHAR(20),
    order_type          VARCHAR(20),
    order_status        VARCHAR(20),
    subtotal            VARCHAR(10),
    tax_amount          VARCHAR(10),
    discount_amount     VARCHAR(10),
    tip_amount          VARCHAR(10),
    total_amount        VARCHAR(10),
    payment_method      VARCHAR(20),
    -- Metadata columns
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(255)
);

-- ============================================================================
-- ORDER_ITEMS - Order line items
-- ============================================================================
CREATE OR REPLACE TABLE order_items (
    order_item_id       VARCHAR(20),
    order_id            VARCHAR(20),
    item_id             VARCHAR(10),
    quantity            VARCHAR(10),
    unit_price          VARCHAR(10),
    customizations      VARCHAR(500),
    line_total          VARCHAR(10),
    -- Metadata columns
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(255)
);

-- ============================================================================
-- SUPPLIERS - Vendor information
-- ============================================================================
CREATE OR REPLACE TABLE suppliers (
    supplier_id         VARCHAR(10),
    supplier_name       VARCHAR(100),
    contact_name        VARCHAR(100),
    email               VARCHAR(100),
    phone               VARCHAR(20),
    address             VARCHAR(200),
    city                VARCHAR(50),
    state               VARCHAR(2),
    zip_code            VARCHAR(10),
    payment_terms       VARCHAR(50),
    lead_time_days      VARCHAR(10),
    is_active           VARCHAR(5),
    -- Metadata columns
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(255)
);

-- ============================================================================
-- INVENTORY - Daily inventory snapshots
-- ============================================================================
CREATE OR REPLACE TABLE inventory (
    inventory_id        VARCHAR(20),
    location_id         VARCHAR(10),
    ingredient_id       VARCHAR(10),
    snapshot_date       VARCHAR(20),
    quantity_on_hand    VARCHAR(10),
    quantity_reserved   VARCHAR(10),
    reorder_point       VARCHAR(10),
    reorder_quantity    VARCHAR(10),
    last_restock_date   VARCHAR(20),
    expiration_date     VARCHAR(20),
    -- Metadata columns
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(255)
);

-- ============================================================================
-- Create file format for CSV loading
-- ============================================================================
CREATE OR REPLACE FILE FORMAT csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE;

-- ============================================================================
-- Create internal stage for loading seed data
-- ============================================================================
CREATE OR REPLACE STAGE seed_data_stage
    FILE_FORMAT = csv_format;

-- ============================================================================
-- COPY INTO commands (run after uploading CSVs to stage)
-- ============================================================================
/*
-- Upload files to stage first:
PUT file:///path/to/seed_data/locations.csv @seed_data_stage;
PUT file:///path/to/seed_data/customers.csv @seed_data_stage;
-- ... etc for all files

-- Then run COPY INTO:
COPY INTO locations FROM @seed_data_stage/locations.csv FILE_FORMAT = csv_format;
COPY INTO customers FROM @seed_data_stage/customers.csv FILE_FORMAT = csv_format;
COPY INTO employees FROM @seed_data_stage/employees.csv FILE_FORMAT = csv_format;
COPY INTO menu_items FROM @seed_data_stage/menu_items.csv FILE_FORMAT = csv_format;
COPY INTO ingredients FROM @seed_data_stage/ingredients.csv FILE_FORMAT = csv_format;
COPY INTO menu_item_ingredients FROM @seed_data_stage/menu_item_ingredients.csv FILE_FORMAT = csv_format;
COPY INTO orders FROM @seed_data_stage/orders.csv FILE_FORMAT = csv_format;
COPY INTO order_items FROM @seed_data_stage/order_items.csv FILE_FORMAT = csv_format;
COPY INTO suppliers FROM @seed_data_stage/suppliers.csv FILE_FORMAT = csv_format;
COPY INTO inventory FROM @seed_data_stage/inventory.csv FILE_FORMAT = csv_format;
*/

-- Verify tables created
SHOW TABLES IN SCHEMA SAMMYS_RAW;
