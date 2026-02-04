# Data Quality Framework

This folder contains SQL-based data quality validation for Sammy's Sandwich Shop data warehouse.

## Overview

The data quality framework provides:
- Automated validation checks across all data layers (raw, enriched, ready)
- Persistent storage of check results for trend analysis
- Configurable thresholds for warnings and failures
- Pre-built reports for monitoring data health
- Python script for easy execution and reporting

## Quick Start (Python - Recommended)

The easiest way to run data quality checks is using the Python script:

```bash
# Set credentials
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_username"
export SNOWFLAKE_PASSWORD="your_password"

# Run all data quality checks (will auto-setup schema if needed)
python data_quality/run_data_quality.py

# Run checks for a specific layer only
python data_quality/run_data_quality.py --layer enriched

# Show verbose output (including passed checks)
python data_quality/run_data_quality.py --verbose

# View run history
python data_quality/run_data_quality.py --history

# Setup schema only (don't run checks)
python data_quality/run_data_quality.py --setup-only

# Skip setup (faster if schema already exists)
python data_quality/run_data_quality.py --skip-setup
```

## Quick Start (SQL)

Alternatively, run directly in Snowflake:

### 1. Create the Data Quality Schema and Tables

```sql
-- Run the DDL script to create the schema and tables
@data_quality/ddl/01_data_quality_tables.sql
```

### 2. Run Data Quality Checks

```sql
-- Run all checks across all layers
CALL SAMMYS_DATA_QUALITY.sp_run_data_quality_checks('all');

-- Or run checks for a specific layer
CALL SAMMYS_DATA_QUALITY.sp_run_data_quality_checks('raw');
CALL SAMMYS_DATA_QUALITY.sp_run_data_quality_checks('enriched');
CALL SAMMYS_DATA_QUALITY.sp_run_data_quality_checks('ready');
```

### 3. Review Results

```sql
-- Run the summary report queries
@data_quality/reports/rpt_data_quality_summary.sql
```

## Check Categories

| Category | Description |
|----------|-------------|
| `null_check` | Validates required fields are not null |
| `duplicate_check` | Ensures primary keys are unique |
| `referential_integrity` | Validates foreign key relationships |
| `range_check` | Checks values are within expected ranges |
| `row_count_check` | Compares record counts between layers |
| `aggregate_check` | Validates aggregated values match source |

## Check Status

| Status | Meaning |
|--------|---------|
| `PASSED` | Check passed with no issues |
| `WARNING` | Check passed but approaching threshold |
| `FAILED` | Check failed, exceeds failure threshold |

## Configuring Thresholds

Default thresholds are stored in `data_quality_thresholds`. Modify them to adjust sensitivity:

```sql
-- View current thresholds
SELECT * FROM SAMMYS_DATA_QUALITY.data_quality_thresholds;

-- Update a threshold (e.g., allow up to 2% nulls before warning)
UPDATE SAMMYS_DATA_QUALITY.data_quality_thresholds
SET warning_threshold = 0.02
WHERE check_category = 'null_check';
```

## Scheduling (Optional)

To run data quality checks automatically after your pipeline:

```sql
-- Create a task to run checks daily
CREATE OR REPLACE TASK run_daily_dq_checks
  WAREHOUSE = <your_warehouse>
  SCHEDULE = 'USING CRON 0 6 * * * America/New_York'
AS
  CALL SAMMYS_DATA_QUALITY.sp_run_data_quality_checks('all');

-- Enable the task
ALTER TASK run_daily_dq_checks RESUME;
```

## Adding Custom Checks

To add your own checks, insert additional check logic into `sp_run_data_quality_checks.sql` following the existing pattern:

```sql
INSERT INTO data_quality_results (
    run_id, run_timestamp, check_category, check_name, 
    layer, schema_name, table_name, column_name, 
    status, records_checked, records_failed, failure_percentage, check_sql
)
SELECT 
    :v_run_id,
    :v_start_time,
    'custom_check',
    'Your check description',
    'enriched',
    'SAMMYS_ENRICHED',
    'your_table',
    'your_column',
    CASE WHEN <condition> THEN 'PASSED' ELSE 'FAILED' END,
    COUNT(*),
    COUNT_IF(<failure_condition>),
    ROUND(COUNT_IF(<failure_condition>) / NULLIF(COUNT(*), 0) * 100, 4),
    'SELECT query for investigating failures'
FROM your_table;
```

## File Structure

```
data_quality/
├── README.md                           # This file
├── run_data_quality.py                # Python script for running checks
├── ddl/
│   └── 01_data_quality_tables.sql     # Schema and table definitions
├── stored_procedures/
│   └── sp_run_data_quality_checks.sql # Main validation procedure
└── reports/
    └── rpt_data_quality_summary.sql   # Result reporting queries
```

## Best Practices

1. **Run after each pipeline execution** - Add a call to `sp_run_data_quality_checks` at the end of your pipeline
2. **Monitor trends** - Review the trend analysis to catch degrading data quality early
3. **Investigate failures promptly** - Use the `check_sql` column to investigate failed checks
4. **Adjust thresholds** - Tune thresholds based on your business requirements
5. **Add business-specific checks** - Extend the framework with domain-specific validations
