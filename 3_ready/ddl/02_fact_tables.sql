/*
================================================================================
  SAMMY'S SANDWICH SHOP - READY LAYER FACT TABLES
================================================================================
  Fact tables for analytics and reporting.
  Contains measures and foreign keys to dimension tables.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

-- ============================================================================
-- FACT_SALES - Order-level sales transactions (grain: one row per order)
-- ============================================================================
CREATE OR REPLACE TABLE fact_sales (
    -- Surrogate keys to dimensions
    order_id                VARCHAR(20) NOT NULL,
    date_key                INTEGER NOT NULL,
    time_key                INTEGER NOT NULL,
    customer_sk             INTEGER,
    employee_sk             INTEGER,
    location_sk             INTEGER,
    payment_method_sk       INTEGER,
    order_type_sk           INTEGER,
    -- Degenerate dimensions
    order_status            VARCHAR(20),
    -- Measures
    item_count              INTEGER,
    subtotal                NUMBER(12,2),
    tax_amount              NUMBER(12,2),
    discount_amount         NUMBER(12,2),
    tip_amount              NUMBER(12,2),
    total_amount            NUMBER(12,2),
    net_sales               NUMBER(12,2),
    -- Derived measures
    discount_pct            NUMBER(5,2),
    tip_pct                 NUMBER(5,2),
    avg_item_price          NUMBER(10,2),
    -- Flags for analysis
    has_discount            BOOLEAN,
    has_tip                 BOOLEAN,
    is_guest_order          BOOLEAN,
    is_weekend              BOOLEAN,
    -- Metadata
    _created_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (order_id)
);

-- ============================================================================
-- FACT_SALES_LINE_ITEM - Line-item level sales (grain: one row per order item)
-- ============================================================================
CREATE OR REPLACE TABLE fact_sales_line_item (
    -- Keys
    order_item_id           VARCHAR(20) NOT NULL,
    order_id                VARCHAR(20) NOT NULL,
    date_key                INTEGER NOT NULL,
    customer_sk             INTEGER,
    employee_sk             INTEGER,
    location_sk             INTEGER,
    menu_item_sk            INTEGER,
    -- Measures
    quantity                INTEGER,
    unit_price              NUMBER(10,2),
    line_total              NUMBER(12,2),
    food_cost               NUMBER(10,2),
    gross_profit            NUMBER(10,2),
    gross_margin_pct        NUMBER(5,2),
    -- Flags
    has_customization       BOOLEAN,
    -- Metadata
    _created_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (order_item_id)
);

-- ============================================================================
-- FACT_INVENTORY_SNAPSHOT - Daily inventory snapshot (grain: location/ingredient/day)
-- ============================================================================
CREATE OR REPLACE TABLE fact_inventory_snapshot (
    -- Keys
    inventory_snapshot_id   VARCHAR(50) NOT NULL,
    date_key                INTEGER NOT NULL,
    location_sk             INTEGER,
    ingredient_sk           INTEGER,
    -- Measures
    quantity_on_hand        INTEGER,
    quantity_reserved       INTEGER,
    quantity_available      INTEGER,
    reorder_point           INTEGER,
    days_until_expiration   INTEGER,
    -- Calculated measures
    stock_value             NUMBER(12,2),
    days_of_supply          NUMBER(10,2),
    -- Status indicators
    needs_reorder           BOOLEAN,
    stock_status            VARCHAR(20),
    expiration_risk         VARCHAR(20),
    -- Metadata
    _created_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (inventory_snapshot_id)
);

-- ============================================================================
-- FACT_DAILY_SUMMARY - Aggregated daily metrics (grain: location/day)
-- ============================================================================
CREATE OR REPLACE TABLE fact_daily_summary (
    -- Keys
    daily_summary_id        VARCHAR(50) NOT NULL,
    date_key                INTEGER NOT NULL,
    location_sk             INTEGER,
    -- Order metrics
    total_orders            INTEGER,
    total_items_sold        INTEGER,
    unique_customers        INTEGER,
    guest_orders            INTEGER,
    -- Revenue metrics
    gross_sales             NUMBER(12,2),
    discounts_given         NUMBER(12,2),
    net_sales               NUMBER(12,2),
    tax_collected           NUMBER(12,2),
    tips_received           NUMBER(12,2),
    total_revenue           NUMBER(12,2),
    -- Calculated metrics
    avg_order_value         NUMBER(10,2),
    avg_items_per_order     NUMBER(10,2),
    discount_rate           NUMBER(5,2),
    -- Order type breakdown
    dine_in_orders          INTEGER,
    takeout_orders          INTEGER,
    drive_thru_orders       INTEGER,
    -- Period breakdown
    breakfast_orders        INTEGER,
    lunch_orders            INTEGER,
    dinner_orders           INTEGER,
    -- Metadata
    _created_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (daily_summary_id)
);

-- ============================================================================
-- FACT_CUSTOMER_ACTIVITY - Customer activity accumulating snapshot
-- ============================================================================
CREATE OR REPLACE TABLE fact_customer_activity (
    -- Keys
    customer_sk             INTEGER NOT NULL,
    customer_id             VARCHAR(10) NOT NULL,
    -- Activity metrics (lifetime)
    first_order_date_key    INTEGER,
    last_order_date_key     INTEGER,
    total_orders            INTEGER,
    total_items_purchased   INTEGER,
    total_spend             NUMBER(12,2),
    total_discounts         NUMBER(12,2),
    total_tips              NUMBER(12,2),
    -- Calculated metrics
    avg_order_value         NUMBER(10,2),
    avg_days_between_orders NUMBER(10,2),
    customer_lifetime_days  INTEGER,
    -- Favorite analysis
    favorite_location_sk    INTEGER,
    favorite_menu_item_sk   INTEGER,
    favorite_order_type     VARCHAR(20),
    -- Recency metrics
    days_since_last_order   INTEGER,
    -- RFM scores
    recency_score           INTEGER,
    frequency_score         INTEGER,
    monetary_score          INTEGER,
    rfm_segment             VARCHAR(50),
    -- Metadata
    _created_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _updated_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (customer_sk)
);

-- ============================================================================
-- FACT_MENU_ITEM_PERFORMANCE - Menu item metrics
-- ============================================================================
CREATE OR REPLACE TABLE fact_menu_item_performance (
    -- Keys
    menu_item_sk            INTEGER NOT NULL,
    item_id                 VARCHAR(10) NOT NULL,
    -- Volume metrics
    total_quantity_sold     INTEGER,
    total_orders            INTEGER,
    -- Revenue metrics  
    total_revenue           NUMBER(12,2),
    total_food_cost         NUMBER(12,2),
    total_gross_profit      NUMBER(12,2),
    -- Calculated metrics
    avg_daily_quantity      NUMBER(10,2),
    revenue_per_unit        NUMBER(10,2),
    gross_margin_pct        NUMBER(5,2),
    -- Ranking
    revenue_rank            INTEGER,
    quantity_rank           INTEGER,
    profit_rank             INTEGER,
    -- Metadata
    _created_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _updated_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (menu_item_sk)
);

-- Verify fact tables created
SHOW TABLES IN SCHEMA SAMMYS_READY LIKE 'FACT%';
