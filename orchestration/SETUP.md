# Sammy's Sandwich Shop - Setup Guide

This guide walks you through setting up the complete data warehouse in Snowflake.

## Prerequisites

- Snowflake account with appropriate permissions (ACCOUNTADMIN or equivalent)
- One of the following for data loading:
  - Snowflake Web UI access (easiest)
  - [SnowSQL CLI](https://docs.snowflake.com/en/user-guide/snowsql-install-config) installed
  - Python 3.8+ with dependencies from `requirements.txt`

## Quick Start

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          SETUP SEQUENCE                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Step 1: Create database, schemas, warehouse                                │
│          → Run: 1_raw/ddl/00_setup.sql                                      │
│                                                                             │
│  Step 2: Create all tables                                                  │
│          → Run: 1_raw/ddl/01_raw_tables.sql                                 │
│          → Run: 2_enriched/ddl/01_enriched_tables.sql                       │
│          → Run: 3_ready/ddl/01_dim_tables.sql                               │
│          → Run: 3_ready/ddl/02_fact_tables.sql                              │
│                                                                             │
│  Step 3: Create stored procedures                                           │
│          → Run all files in: 2_enriched/stored_procedures/                  │
│          → Run all files in: 3_ready/stored_procedures/                     │
│                                                                             │
│  Step 4: Load seed data (choose one method)                                 │
│          → Option A: setup_webui.md        (manual, no tools needed)        │
│          → Option B: setup_snowsql.sql     (CLI-based)                      │
│          → Option C: setup_python.py       (automated, recommended)         │
│                                                                             │
│  Step 5: Run the data pipeline                                              │
│          → Run: orchestration/run_full_pipeline.sql                         │
│          → Includes data quality checks automatically                       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Data Loading Options

Choose the method that best fits your workflow:

| Method | File | Best For |
|--------|------|----------|
| **A. Web UI** | [setup_webui.md](setup_webui.md) | Quick manual setup, learning Snowflake |
| **B. SnowSQL** | [setup_snowsql.sql](setup_snowsql.sql) | Command-line users, scripted deployments |
| **C. Python** | [setup_python.py](setup_python.py) | Automation, CI/CD pipelines, one-command setup |

### Recommended: Python Setup

For the fastest setup experience, use the Python script:

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_username"
export SNOWFLAKE_PASSWORD="your_password"

# Run setup (from project root)
python orchestration/setup_python.py
```

This will:
1. Create the database, schemas, and warehouse
2. Create all tables
3. Create all stored procedures
4. Load all seed data
5. Run the full pipeline

## Teardown

To completely remove the project from Snowflake:

```sql
-- Run this in Snowflake
DROP DATABASE IF EXISTS SAMMYS_SANDWICH_SHOP CASCADE;
DROP WAREHOUSE IF EXISTS SAMMYS_WH;
```

Or use the teardown script:

```bash
python orchestration/setup_python.py --teardown
```

## Verifying Setup

After setup completes, verify everything is working:

```sql
-- Check row counts
USE DATABASE SAMMYS_SANDWICH_SHOP;

SELECT 'Raw Layer' AS layer, COUNT(*) AS tables 
FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'SAMMYS_RAW';

SELECT 'Enriched Layer' AS layer, COUNT(*) AS tables 
FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'SAMMYS_ENRICHED';

SELECT 'Ready Layer' AS layer, COUNT(*) AS tables 
FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'SAMMYS_READY';

-- Sample report
SELECT * FROM SAMMYS_READY.V_RPT_LOCATION_RANKING LIMIT 5;
```

## Troubleshooting

### "Object does not exist" error
Ensure you've run the DDL scripts in order (Step 1, then Step 2).

### "Warehouse does not exist" error
Run `1_raw/ddl/00_setup.sql` first to create the warehouse.

### Python connection errors
- Verify your account identifier format (e.g., `xy12345.us-east-1`)
- Check that your user has the necessary permissions
- Ensure your IP is allowlisted if using network policies

### Partial data loaded
Check the COPY history for errors:
```sql
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'CUSTOMERS', 
    START_TIME => DATEADD(hour, -1, CURRENT_TIMESTAMP())
));
```

## Data Quality

The pipeline includes automated data quality checks that run after each execution.

### Running Data Quality Checks

Data quality checks are automatically included when you run the pipeline. To run them separately:

**Using Python (recommended):**
```bash
python data_quality/run_data_quality.py
```

**Using SQL:**
```sql
CALL SAMMYS_DATA_QUALITY.sp_run_data_quality_checks('all');
```

### What Gets Checked

| Category | Description |
|----------|-------------|
| Null checks | Required fields are not null |
| Duplicate checks | Primary keys are unique |
| Referential integrity | Foreign key relationships are valid |
| Range checks | Values are within expected ranges |
| Row count checks | Record counts match between layers |
| Aggregate checks | Aggregated values match source |

### Viewing Results

```sql
-- Latest run summary
SELECT * FROM SAMMYS_DATA_QUALITY.data_quality_runs 
ORDER BY run_timestamp DESC LIMIT 1;

-- Failed checks from latest run
SELECT layer, table_name, check_name, records_failed, failure_percentage
FROM SAMMYS_DATA_QUALITY.data_quality_results
WHERE run_id = (SELECT run_id FROM SAMMYS_DATA_QUALITY.data_quality_runs ORDER BY run_timestamp DESC LIMIT 1)
  AND status = 'FAILED';
```

### Python Options

```bash
# Run checks for specific layer
python data_quality/run_data_quality.py --layer enriched

# Show verbose output (all checks)
python data_quality/run_data_quality.py --verbose

# View run history
python data_quality/run_data_quality.py --history
```

See [data_quality/README.md](../data_quality/README.md) for complete documentation.
