/*
================================================================================
  SAMMY'S SANDWICH SHOP - ENRICHED LAYER TABLE DEFINITIONS
================================================================================
  These tables contain cleaned, validated, and standardized data.
  Data types are properly cast, nulls are handled, and business rules applied.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_ENRICHED;

-- ============================================================================
-- ENRICHED_LOCATIONS - Cleaned location data
-- ============================================================================
CREATE OR REPLACE TABLE enriched_locations (
    location_id             VARCHAR(10) NOT NULL,
    location_name           VARCHAR(100) NOT NULL,
    address                 VARCHAR(200),
    city                    VARCHAR(50),
    state                   VARCHAR(2),
    zip_code                VARCHAR(10),
    phone                   VARCHAR(20),
    manager_employee_id     VARCHAR(10),
    open_date               DATE,
    seating_capacity        INTEGER,
    has_drive_thru          BOOLEAN,
    years_in_operation      NUMBER(5,2),
    -- Metadata
    _enriched_at            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_loaded_at       TIMESTAMP_NTZ,
    PRIMARY KEY (location_id)
);

-- ============================================================================
-- ENRICHED_CUSTOMERS - Cleaned and enriched customer data
-- ============================================================================
CREATE OR REPLACE TABLE enriched_customers (
    customer_id             VARCHAR(10) NOT NULL,
    first_name              VARCHAR(50),
    last_name               VARCHAR(50),
    full_name               VARCHAR(100),
    email                   VARCHAR(100),
    phone                   VARCHAR(20),
    address                 VARCHAR(200),
    city                    VARCHAR(50),
    state                   VARCHAR(2),
    zip_code                VARCHAR(10),
    birth_date              DATE,
    age                     INTEGER,
    signup_date             DATE,
    loyalty_tier            VARCHAR(20),
    loyalty_tier_rank       INTEGER,
    loyalty_points          INTEGER,
    preferred_location_id   VARCHAR(10),
    marketing_opt_in        BOOLEAN,
    customer_tenure_days    INTEGER,
    -- Metadata
    _enriched_at            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_loaded_at       TIMESTAMP_NTZ,
    PRIMARY KEY (customer_id)
);

-- ============================================================================
-- ENRICHED_EMPLOYEES - Cleaned employee data with derived fields
-- ============================================================================
CREATE OR REPLACE TABLE enriched_employees (
    employee_id             VARCHAR(10) NOT NULL,
    first_name              VARCHAR(50),
    last_name               VARCHAR(50),
    full_name               VARCHAR(100),
    email                   VARCHAR(100),
    phone                   VARCHAR(20),
    hire_date               DATE,
    job_title               VARCHAR(50),
    department              VARCHAR(50),
    hourly_rate             NUMBER(10,2),
    location_id             VARCHAR(10),
    manager_id              VARCHAR(10),
    employment_status       VARCHAR(20),
    tenure_days             INTEGER,
    is_manager              BOOLEAN,
    -- Metadata
    _enriched_at            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_loaded_at       TIMESTAMP_NTZ,
    PRIMARY KEY (employee_id)
);

-- ============================================================================
-- ENRICHED_MENU_ITEMS - Cleaned menu item data
-- ============================================================================
CREATE OR REPLACE TABLE enriched_menu_items (
    item_id                 VARCHAR(10) NOT NULL,
    item_name               VARCHAR(100) NOT NULL,
    category                VARCHAR(50),
    subcategory             VARCHAR(50),
    description             VARCHAR(500),
    base_price              NUMBER(10,2),
    is_active               BOOLEAN,
    is_seasonal             BOOLEAN,
    calories                INTEGER,
    prep_time_minutes       INTEGER,
    introduced_date         DATE,
    days_on_menu            INTEGER,
    price_tier              VARCHAR(20),
    calorie_category        VARCHAR(20),
    -- Metadata
    _enriched_at            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_loaded_at       TIMESTAMP_NTZ,
    PRIMARY KEY (item_id)
);

-- ============================================================================
-- ENRICHED_INGREDIENTS - Cleaned ingredient data
-- ============================================================================
CREATE OR REPLACE TABLE enriched_ingredients (
    ingredient_id           VARCHAR(10) NOT NULL,
    ingredient_name         VARCHAR(100) NOT NULL,
    category                VARCHAR(50),
    unit_of_measure         VARCHAR(20),
    cost_per_unit           NUMBER(10,2),
    supplier_id             VARCHAR(10),
    is_allergen             BOOLEAN,
    allergen_type           VARCHAR(50),
    shelf_life_days         INTEGER,
    storage_type            VARCHAR(20),
    cost_tier               VARCHAR(20),
    -- Metadata
    _enriched_at            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_loaded_at       TIMESTAMP_NTZ,
    PRIMARY KEY (ingredient_id)
);

-- ============================================================================
-- ENRICHED_MENU_ITEM_INGREDIENTS - Recipe BOM with costs
-- ============================================================================
CREATE OR REPLACE TABLE enriched_menu_item_ingredients (
    item_id                 VARCHAR(10) NOT NULL,
    ingredient_id           VARCHAR(10) NOT NULL,
    quantity_required       NUMBER(10,4),
    is_optional             BOOLEAN,
    extra_charge            NUMBER(10,2),
    ingredient_cost         NUMBER(10,4),
    -- Metadata
    _enriched_at            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (item_id, ingredient_id)
);

-- ============================================================================
-- ENRICHED_ORDERS - Cleaned and enriched order data
-- ============================================================================
CREATE OR REPLACE TABLE enriched_orders (
    order_id                VARCHAR(20) NOT NULL,
    customer_id             VARCHAR(10),
    employee_id             VARCHAR(10),
    location_id             VARCHAR(10),
    order_date              DATE,
    order_time              TIME,
    order_datetime          TIMESTAMP_NTZ,
    order_type              VARCHAR(20),
    order_status            VARCHAR(20),
    subtotal                NUMBER(10,2),
    tax_amount              NUMBER(10,2),
    discount_amount         NUMBER(10,2),
    tip_amount              NUMBER(10,2),
    total_amount            NUMBER(10,2),
    payment_method          VARCHAR(20),
    -- Derived fields
    order_day_of_week       VARCHAR(10),
    order_hour              INTEGER,
    order_period            VARCHAR(20),
    is_weekend              BOOLEAN,
    has_discount            BOOLEAN,
    has_tip                 BOOLEAN,
    is_guest_order          BOOLEAN,
    item_count              INTEGER,
    -- Metadata
    _enriched_at            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_loaded_at       TIMESTAMP_NTZ,
    PRIMARY KEY (order_id)
);

-- ============================================================================
-- ENRICHED_ORDER_ITEMS - Cleaned order line items with costs
-- ============================================================================
CREATE OR REPLACE TABLE enriched_order_items (
    order_item_id           VARCHAR(20) NOT NULL,
    order_id                VARCHAR(20) NOT NULL,
    item_id                 VARCHAR(10),
    quantity                INTEGER,
    unit_price              NUMBER(10,2),
    customizations          VARCHAR(500),
    line_total              NUMBER(10,2),
    has_customization       BOOLEAN,
    -- Metadata
    _enriched_at            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_loaded_at       TIMESTAMP_NTZ,
    PRIMARY KEY (order_item_id)
);

-- ============================================================================
-- ENRICHED_SUPPLIERS - Cleaned supplier data
-- ============================================================================
CREATE OR REPLACE TABLE enriched_suppliers (
    supplier_id             VARCHAR(10) NOT NULL,
    supplier_name           VARCHAR(100) NOT NULL,
    contact_name            VARCHAR(100),
    email                   VARCHAR(100),
    phone                   VARCHAR(20),
    address                 VARCHAR(200),
    city                    VARCHAR(50),
    state                   VARCHAR(2),
    zip_code                VARCHAR(10),
    payment_terms           VARCHAR(50),
    payment_days            INTEGER,
    lead_time_days          INTEGER,
    is_active               BOOLEAN,
    -- Metadata
    _enriched_at            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_loaded_at       TIMESTAMP_NTZ,
    PRIMARY KEY (supplier_id)
);

-- ============================================================================
-- ENRICHED_INVENTORY - Cleaned inventory with status calculations
-- ============================================================================
CREATE OR REPLACE TABLE enriched_inventory (
    inventory_id            VARCHAR(20) NOT NULL,
    location_id             VARCHAR(10),
    ingredient_id           VARCHAR(10),
    snapshot_date           DATE,
    quantity_on_hand        INTEGER,
    quantity_reserved       INTEGER,
    quantity_available      INTEGER,
    reorder_point           INTEGER,
    reorder_quantity        INTEGER,
    last_restock_date       DATE,
    expiration_date         DATE,
    days_until_expiration   INTEGER,
    needs_reorder           BOOLEAN,
    stock_status            VARCHAR(20),
    -- Metadata
    _enriched_at            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_loaded_at       TIMESTAMP_NTZ,
    PRIMARY KEY (inventory_id)
);

-- Verify tables created
SHOW TABLES IN SCHEMA SAMMYS_ENRICHED;
