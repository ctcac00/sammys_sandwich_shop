/*
================================================================================
  SAMMY'S SANDWICH SHOP - DATABASE AND SCHEMA SETUP
================================================================================
  This script creates the database and all three layer schemas.
  Run this first before any other scripts.
================================================================================
*/

-- Create the database
CREATE DATABASE IF NOT EXISTS SAMMYS_SANDWICH_SHOP;

USE DATABASE SAMMYS_SANDWICH_SHOP;

-- Create warehouse for processing
CREATE WAREHOUSE IF NOT EXISTS SAMMYS_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- Create schemas for each layer
CREATE SCHEMA IF NOT EXISTS SAMMYS_RAW
    COMMENT = 'Raw layer - Source data as-is from operational systems';

CREATE SCHEMA IF NOT EXISTS SAMMYS_ENRICHED
    COMMENT = 'Enriched layer - Cleaned, validated, and standardized data';

CREATE SCHEMA IF NOT EXISTS SAMMYS_READY
    COMMENT = 'Ready layer - Dimensional model for analytics and reporting';

-- Grant usage (adjust roles as needed for your environment)
-- GRANT USAGE ON DATABASE SAMMYS_SANDWICH_SHOP TO ROLE <your_role>;
-- GRANT USAGE ON ALL SCHEMAS IN DATABASE SAMMYS_SANDWICH_SHOP TO ROLE <your_role>;

-- Set default schema for this session
USE SCHEMA SAMMYS_RAW;

-- Verify setup
SHOW SCHEMAS IN DATABASE SAMMYS_SANDWICH_SHOP;
