/*
================================================================================
  SAMMY'S SANDWICH SHOP - DATA QUALITY FRAMEWORK
================================================================================
  Tables to store and track data quality validation results.
  Run checks after each pipeline execution to monitor data health.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;

-- Create a schema for data quality objects
CREATE SCHEMA IF NOT EXISTS SAMMYS_DATA_QUALITY;
USE SCHEMA SAMMYS_DATA_QUALITY;

-- ============================================================================
-- DATA_QUALITY_RESULTS - Stores individual check results
-- ============================================================================
CREATE OR REPLACE TABLE data_quality_results (
    result_id               INTEGER AUTOINCREMENT NOT NULL,
    run_id                  VARCHAR(50) NOT NULL,          -- Groups checks from same run
    run_timestamp           TIMESTAMP_NTZ NOT NULL,
    
    -- Check identification
    check_category          VARCHAR(50) NOT NULL,           -- null_check, duplicate_check, etc.
    check_name              VARCHAR(200) NOT NULL,          -- Descriptive name
    layer                   VARCHAR(20) NOT NULL,           -- raw, enriched, ready
    schema_name             VARCHAR(100) NOT NULL,
    table_name              VARCHAR(100) NOT NULL,
    column_name             VARCHAR(100),
    
    -- Results
    status                  VARCHAR(20) NOT NULL,           -- PASSED, FAILED, WARNING
    records_checked         INTEGER,
    records_failed          INTEGER,
    failure_percentage      NUMBER(10,4),
    
    -- Thresholds
    warning_threshold       NUMBER(10,4),                   -- Percentage that triggers warning
    failure_threshold       NUMBER(10,4),                   -- Percentage that triggers failure
    
    -- Details
    check_sql               VARCHAR(10000),                 -- SQL used for the check
    error_details           VARCHAR(5000),                  -- Sample of failed records
    
    -- Metadata
    _created_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (result_id)
);

-- ============================================================================
-- DATA_QUALITY_RUNS - Summary of each validation run
-- ============================================================================
CREATE OR REPLACE TABLE data_quality_runs (
    run_id                  VARCHAR(50) NOT NULL,
    run_timestamp           TIMESTAMP_NTZ NOT NULL,
    run_type                VARCHAR(50),                    -- full, incremental, layer-specific
    
    -- Summary counts
    total_checks            INTEGER,
    checks_passed           INTEGER,
    checks_warned           INTEGER,
    checks_failed           INTEGER,
    
    -- Status
    overall_status          VARCHAR(20),                    -- PASSED, WARNING, FAILED
    duration_seconds        NUMBER(10,2),
    
    -- Metadata
    triggered_by            VARCHAR(100),                   -- manual, scheduled, pipeline
    _created_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (run_id)
);

-- ============================================================================
-- DATA_QUALITY_THRESHOLDS - Configurable thresholds per check
-- ============================================================================
CREATE OR REPLACE TABLE data_quality_thresholds (
    threshold_id            INTEGER AUTOINCREMENT NOT NULL,
    check_category          VARCHAR(50) NOT NULL,
    layer                   VARCHAR(20),                    -- NULL means applies to all layers
    table_name              VARCHAR(100),                   -- NULL means applies to all tables
    column_name             VARCHAR(100),                   -- NULL means applies to all columns
    
    -- Thresholds (percentage of records that can fail)
    warning_threshold       NUMBER(10,4) DEFAULT 0.01,      -- 1% triggers warning
    failure_threshold       NUMBER(10,4) DEFAULT 0.05,      -- 5% triggers failure
    
    is_active               BOOLEAN DEFAULT TRUE,
    
    -- Metadata
    _created_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _updated_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (threshold_id)
);

-- Insert default thresholds
INSERT INTO data_quality_thresholds (check_category, warning_threshold, failure_threshold) VALUES
    ('null_check', 0.01, 0.05),
    ('duplicate_check', 0.00, 0.01),
    ('referential_integrity', 0.01, 0.05),
    ('range_check', 0.01, 0.05),
    ('freshness_check', 0.00, 0.00),
    ('row_count_check', 0.05, 0.20);

-- Verify tables created
SHOW TABLES IN SCHEMA SAMMYS_DATA_QUALITY;
