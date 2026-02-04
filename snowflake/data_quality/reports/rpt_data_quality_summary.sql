/*
================================================================================
  DATA QUALITY SUMMARY REPORT
================================================================================
  Provides an overview of data quality check results.
  Run after executing sp_run_data_quality_checks().
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_DATA_QUALITY;

-- ============================================================================
-- LATEST RUN SUMMARY
-- ============================================================================
-- Quick overview of the most recent data quality run
SELECT 
    run_id,
    run_timestamp,
    run_type AS layer_checked,
    overall_status,
    total_checks,
    checks_passed,
    checks_warned,
    checks_failed,
    ROUND(checks_passed / NULLIF(total_checks, 0) * 100, 1) AS pass_rate_pct,
    duration_seconds || 's' AS duration
FROM data_quality_runs
ORDER BY run_timestamp DESC
LIMIT 1;

-- ============================================================================
-- FAILED CHECKS (Most Recent Run)
-- ============================================================================
-- Details on all failed checks - these need immediate attention
SELECT 
    layer,
    table_name,
    column_name,
    check_category,
    check_name,
    records_checked,
    records_failed,
    failure_percentage || '%' AS failure_pct,
    error_details
FROM data_quality_results
WHERE run_id = (SELECT run_id FROM data_quality_runs ORDER BY run_timestamp DESC LIMIT 1)
  AND status = 'FAILED'
ORDER BY failure_percentage DESC, layer, table_name;

-- ============================================================================
-- WARNING CHECKS (Most Recent Run)
-- ============================================================================
-- Checks that passed but are approaching failure threshold
SELECT 
    layer,
    table_name,
    column_name,
    check_category,
    check_name,
    records_checked,
    records_failed,
    failure_percentage || '%' AS failure_pct
FROM data_quality_results
WHERE run_id = (SELECT run_id FROM data_quality_runs ORDER BY run_timestamp DESC LIMIT 1)
  AND status = 'WARNING'
ORDER BY failure_percentage DESC, layer, table_name;

-- ============================================================================
-- CHECKS BY CATEGORY (Most Recent Run)
-- ============================================================================
-- Summary of check results grouped by category
SELECT 
    check_category,
    COUNT(*) AS total_checks,
    COUNT_IF(status = 'PASSED') AS passed,
    COUNT_IF(status = 'WARNING') AS warned,
    COUNT_IF(status = 'FAILED') AS failed,
    ROUND(COUNT_IF(status = 'PASSED') / COUNT(*) * 100, 1) AS pass_rate_pct
FROM data_quality_results
WHERE run_id = (SELECT run_id FROM data_quality_runs ORDER BY run_timestamp DESC LIMIT 1)
GROUP BY check_category
ORDER BY failed DESC, warned DESC;

-- ============================================================================
-- CHECKS BY LAYER (Most Recent Run)
-- ============================================================================
-- Summary of check results grouped by data layer
SELECT 
    layer,
    COUNT(*) AS total_checks,
    COUNT_IF(status = 'PASSED') AS passed,
    COUNT_IF(status = 'WARNING') AS warned,
    COUNT_IF(status = 'FAILED') AS failed,
    ROUND(COUNT_IF(status = 'PASSED') / COUNT(*) * 100, 1) AS pass_rate_pct
FROM data_quality_results
WHERE run_id = (SELECT run_id FROM data_quality_runs ORDER BY run_timestamp DESC LIMIT 1)
GROUP BY layer
ORDER BY 
    CASE layer 
        WHEN 'raw' THEN 1 
        WHEN 'enriched' THEN 2 
        WHEN 'ready' THEN 3 
    END;

-- ============================================================================
-- ALL CHECK RESULTS (Most Recent Run)
-- ============================================================================
-- Complete list of all check results
SELECT 
    layer,
    table_name,
    column_name,
    check_category,
    check_name,
    status,
    records_checked,
    records_failed,
    failure_percentage || '%' AS failure_pct
FROM data_quality_results
WHERE run_id = (SELECT run_id FROM data_quality_runs ORDER BY run_timestamp DESC LIMIT 1)
ORDER BY 
    CASE status WHEN 'FAILED' THEN 1 WHEN 'WARNING' THEN 2 ELSE 3 END,
    layer, 
    table_name;

-- ============================================================================
-- TREND ANALYSIS (Last 7 Runs)
-- ============================================================================
-- Track data quality trends over time
SELECT 
    DATE(run_timestamp) AS run_date,
    run_id,
    overall_status,
    total_checks,
    checks_passed,
    checks_warned,
    checks_failed,
    ROUND(checks_passed / NULLIF(total_checks, 0) * 100, 1) AS pass_rate_pct
FROM data_quality_runs
ORDER BY run_timestamp DESC
LIMIT 7;

-- ============================================================================
-- PROBLEMATIC TABLES (Last 7 Days)
-- ============================================================================
-- Tables with recurring data quality issues
SELECT 
    table_name,
    check_category,
    COUNT(*) AS times_checked,
    COUNT_IF(status = 'FAILED') AS times_failed,
    COUNT_IF(status = 'WARNING') AS times_warned,
    ROUND(AVG(failure_percentage), 4) AS avg_failure_pct
FROM data_quality_results
WHERE run_timestamp >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
GROUP BY table_name, check_category
HAVING COUNT_IF(status IN ('FAILED', 'WARNING')) > 0
ORDER BY times_failed DESC, times_warned DESC, avg_failure_pct DESC;
