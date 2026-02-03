/*
================================================================================
  SAMMY'S SANDWICH SHOP - READY LAYER DIMENSION TABLES
================================================================================
  Star schema dimension tables for analytics and reporting.
  Includes surrogate keys and SCD Type 2 support where applicable.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

-- ============================================================================
-- DIM_DATE - Date dimension (conformed dimension)
-- ============================================================================
CREATE OR REPLACE TABLE dim_date (
    date_key                INTEGER NOT NULL,
    full_date               DATE NOT NULL,
    year                    INTEGER,
    quarter                 INTEGER,
    quarter_name            VARCHAR(10),
    month                   INTEGER,
    month_name              VARCHAR(20),
    month_abbr              VARCHAR(3),
    week_of_year            INTEGER,
    day_of_month            INTEGER,
    day_of_week             INTEGER,
    day_name                VARCHAR(10),
    day_abbr                VARCHAR(3),
    is_weekend              BOOLEAN,
    is_holiday              BOOLEAN,
    holiday_name            VARCHAR(50),
    fiscal_year             INTEGER,
    fiscal_quarter          INTEGER,
    -- Metadata
    _created_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (date_key)
);

-- ============================================================================
-- DIM_TIME - Time dimension for time-of-day analysis
-- ============================================================================
CREATE OR REPLACE TABLE dim_time (
    time_key                INTEGER NOT NULL,
    full_time               TIME NOT NULL,
    hour_24                 INTEGER,
    hour_12                 INTEGER,
    minute                  INTEGER,
    am_pm                   VARCHAR(2),
    time_period             VARCHAR(20),
    is_peak_hour            BOOLEAN,
    -- Metadata
    _created_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (time_key)
);

-- ============================================================================
-- DIM_CUSTOMER - Customer dimension with SCD Type 2 support
-- ============================================================================
CREATE OR REPLACE TABLE dim_customer (
    customer_sk             INTEGER AUTOINCREMENT NOT NULL,
    customer_id             VARCHAR(10),
    first_name              VARCHAR(50),
    last_name               VARCHAR(50),
    full_name               VARCHAR(100),
    email                   VARCHAR(100),
    phone                   VARCHAR(20),
    city                    VARCHAR(50),
    state                   VARCHAR(2),
    zip_code                VARCHAR(10),
    age_group               VARCHAR(20),
    loyalty_tier            VARCHAR(20),
    loyalty_tier_rank       INTEGER,
    signup_date             DATE,
    tenure_months           INTEGER,
    tenure_group            VARCHAR(20),
    marketing_opt_in        BOOLEAN,
    preferred_location_id   VARCHAR(10),
    -- SCD Type 2 columns
    effective_date          DATE NOT NULL,
    expiration_date         DATE,
    is_current              BOOLEAN DEFAULT TRUE,
    -- Metadata
    _created_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _updated_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (customer_sk)
);

-- Unknown/Guest customer record
-- INSERT INTO dim_customer (customer_id, first_name, last_name, full_name, loyalty_tier, loyalty_tier_rank, effective_date, is_current)
-- VALUES ('UNKNOWN', 'Guest', 'Customer', 'Guest Customer', 'None', 0, '1900-01-01', TRUE);

-- ============================================================================
-- DIM_EMPLOYEE - Employee dimension
-- ============================================================================
CREATE OR REPLACE TABLE dim_employee (
    employee_sk             INTEGER AUTOINCREMENT NOT NULL,
    employee_id             VARCHAR(10),
    first_name              VARCHAR(50),
    last_name               VARCHAR(50),
    full_name               VARCHAR(100),
    job_title               VARCHAR(50),
    department              VARCHAR(50),
    hourly_rate             NUMBER(10,2),
    rate_band               VARCHAR(20),
    location_id             VARCHAR(10),
    is_manager              BOOLEAN,
    hire_date               DATE,
    tenure_months           INTEGER,
    employment_status       VARCHAR(20),
    -- SCD Type 2 columns
    effective_date          DATE NOT NULL,
    expiration_date         DATE,
    is_current              BOOLEAN DEFAULT TRUE,
    -- Metadata
    _created_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _updated_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (employee_sk)
);

-- ============================================================================
-- DIM_LOCATION - Location/Store dimension
-- ============================================================================
CREATE OR REPLACE TABLE dim_location (
    location_sk             INTEGER AUTOINCREMENT NOT NULL,
    location_id             VARCHAR(10) NOT NULL,
    location_name           VARCHAR(100),
    address                 VARCHAR(200),
    city                    VARCHAR(50),
    state                   VARCHAR(2),
    zip_code                VARCHAR(10),
    region                  VARCHAR(50),
    open_date               DATE,
    years_in_operation      NUMBER(5,2),
    seating_capacity        INTEGER,
    capacity_tier           VARCHAR(20),
    has_drive_thru          BOOLEAN,
    manager_name            VARCHAR(100),
    -- SCD Type 2 columns
    effective_date          DATE NOT NULL,
    expiration_date         DATE,
    is_current              BOOLEAN DEFAULT TRUE,
    -- Metadata
    _created_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _updated_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (location_sk)
);

-- ============================================================================
-- DIM_MENU_ITEM - Menu item/Product dimension
-- ============================================================================
CREATE OR REPLACE TABLE dim_menu_item (
    menu_item_sk            INTEGER AUTOINCREMENT NOT NULL,
    item_id                 VARCHAR(10) NOT NULL,
    item_name               VARCHAR(100),
    category                VARCHAR(50),
    subcategory             VARCHAR(50),
    description             VARCHAR(500),
    base_price              NUMBER(10,2),
    price_tier              VARCHAR(20),
    calories                INTEGER,
    calorie_category        VARCHAR(20),
    prep_time_minutes       INTEGER,
    is_active               BOOLEAN,
    is_seasonal             BOOLEAN,
    introduced_date         DATE,
    food_cost               NUMBER(10,2),
    food_cost_pct           NUMBER(5,2),
    gross_margin            NUMBER(10,2),
    gross_margin_pct        NUMBER(5,2),
    -- SCD Type 2 columns
    effective_date          DATE NOT NULL,
    expiration_date         DATE,
    is_current              BOOLEAN DEFAULT TRUE,
    -- Metadata
    _created_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _updated_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (menu_item_sk)
);

-- ============================================================================
-- DIM_INGREDIENT - Ingredient dimension for inventory analysis
-- ============================================================================
CREATE OR REPLACE TABLE dim_ingredient (
    ingredient_sk           INTEGER AUTOINCREMENT NOT NULL,
    ingredient_id           VARCHAR(10) NOT NULL,
    ingredient_name         VARCHAR(100),
    category                VARCHAR(50),
    unit_of_measure         VARCHAR(20),
    cost_per_unit           NUMBER(10,2),
    cost_tier               VARCHAR(20),
    supplier_id             VARCHAR(10),
    supplier_name           VARCHAR(100),
    is_allergen             BOOLEAN,
    allergen_type           VARCHAR(50),
    shelf_life_days         INTEGER,
    storage_type            VARCHAR(20),
    -- SCD Type 2 columns
    effective_date          DATE NOT NULL,
    expiration_date         DATE,
    is_current              BOOLEAN DEFAULT TRUE,
    -- Metadata
    _created_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _updated_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (ingredient_sk)
);

-- ============================================================================
-- DIM_PAYMENT_METHOD - Payment method dimension (static)
-- ============================================================================
CREATE OR REPLACE TABLE dim_payment_method (
    payment_method_sk       INTEGER AUTOINCREMENT NOT NULL,
    payment_method          VARCHAR(50) NOT NULL,
    payment_type            VARCHAR(20),
    is_digital              BOOLEAN,
    processing_fee_pct      NUMBER(5,4),
    -- Metadata
    _created_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (payment_method_sk)
);

-- Seed payment methods
INSERT INTO dim_payment_method (payment_method, payment_type, is_digital, processing_fee_pct) VALUES
    ('Credit Card', 'Card', TRUE, 0.0295),
    ('Debit Card', 'Card', TRUE, 0.0150),
    ('Cash', 'Cash', FALSE, 0.0000),
    ('Mobile Pay', 'Digital', TRUE, 0.0250),
    ('Gift Card', 'Prepaid', FALSE, 0.0000);

-- ============================================================================
-- DIM_ORDER_TYPE - Order type dimension (static)
-- ============================================================================
CREATE OR REPLACE TABLE dim_order_type (
    order_type_sk           INTEGER AUTOINCREMENT NOT NULL,
    order_type              VARCHAR(50) NOT NULL,
    is_in_store             BOOLEAN,
    avg_service_minutes     INTEGER,
    -- Metadata
    _created_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (order_type_sk)
);

-- Seed order types
INSERT INTO dim_order_type (order_type, is_in_store, avg_service_minutes) VALUES
    ('Dine-In', TRUE, 5),
    ('Takeout', TRUE, 3),
    ('Drive-Thru', FALSE, 4),
    ('Delivery', FALSE, 30),
    ('Catering', FALSE, 60);

-- Verify tables created
SHOW TABLES IN SCHEMA SAMMYS_READY;
