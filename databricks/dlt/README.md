# Sammy's Sandwich Shop - Delta Live Tables Pipeline

This directory contains a Delta Live Tables (DLT) implementation of the Sammy's Sandwich Shop data warehouse. DLT provides a declarative approach to building data pipelines with automatic dependency management, built-in data quality, and simplified operations.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Delta Live Tables Pipeline                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────────────┐  │
│  │    BRONZE    │    │    SILVER    │    │            GOLD              │  │
│  │              │    │              │    │                              │  │
│  │  Raw CSV     │───▶│  Staging     │───▶│  Dimensions (9 tables)       │  │
│  │  Ingestion   │    │  (cleaned)   │    │  Facts (5 tables)            │  │
│  │  (10 tables) │    │              │    │  Reports (15+ views)         │  │
│  │              │    │  Enriched    │    │                              │  │
│  │              │    │  (derived)   │    │                              │  │
│  └──────────────┘    └──────────────┘    └──────────────────────────────┘  │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                     Data Quality Expectations                        │   │
│  │  • Primary key validation (expect_or_drop)                          │   │
│  │  • Value range checks (expect)                                      │   │
│  │  • Referential integrity validation                                 │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## DLT vs Traditional Notebooks

| Feature | Traditional Notebooks | Delta Live Tables |
|---------|----------------------|-------------------|
| Dependency Management | Manual orchestration | Automatic based on `dlt.read()` |
| Data Quality | Separate test notebooks | Built-in `@dlt.expect` decorators |
| Execution Order | Explicit sequencing | DAG-based automatic ordering |
| Error Handling | Custom retry logic | Built-in retry and recovery |
| Monitoring | Manual logging | Pipeline UI with lineage |
| Schema Evolution | Manual handling | Automatic schema evolution |
| Incremental Processing | Custom CDC logic | Native streaming support |

## Directory Structure

```
dlt/
├── config.py                    # Shared configuration (catalog, paths, defaults)
├── README.md                    # This file
│
├── bronze/
│   └── ingest.py               # Raw data ingestion (10 tables)
│
├── silver/
│   ├── staging.py              # Type casting & cleaning (10 tables)
│   └── enriched.py             # Derived fields & joins (10 tables)
│
└── gold/
    ├── dimensions.py           # Dimension tables (9 tables)
    ├── facts.py                # Fact tables (5 tables)
    └── reports.py              # Analytical views (15+ views)
```

## Tables and Views

### Bronze Layer (10 tables)
Raw data ingestion with metadata tracking:
- `bronze_customers`, `bronze_employees`, `bronze_locations`
- `bronze_menu_items`, `bronze_ingredients`, `bronze_menu_item_ingredients`
- `bronze_orders`, `bronze_order_items`
- `bronze_suppliers`, `bronze_inventory`

### Silver Layer (20 tables)

**Staging Tables (10):** Type casting, cleaning, basic validation
- `stg_customers`, `stg_employees`, `stg_locations`, `stg_menu_items`
- `stg_ingredients`, `stg_menu_item_ingredients`, `stg_orders`, `stg_order_items`
- `stg_suppliers`, `stg_inventory`

**Enriched Tables (10):** Derived fields, joins, business logic
- `int_customers`, `int_employees`, `int_locations`, `int_menu_items`
- `int_ingredients`, `int_menu_item_ingredients`, `int_orders`, `int_order_items`
- `int_suppliers`, `int_inventory`

### Gold Layer

**Dimension Tables (9):**
- `dim_date` - Calendar dimension (2020-2030)
- `dim_time` - Time of day dimension
- `dim_customer` - Customer demographics and loyalty
- `dim_employee` - Employee details and tenure
- `dim_location` - Store locations and geography
- `dim_menu_item` - Menu items with profitability
- `dim_ingredient` - Ingredients with supplier info
- `dim_payment_method` - Payment method reference
- `dim_order_type` - Order type reference

**Fact Tables (5):**
- `fct_sales` - Order-level transactions
- `fct_sales_line_item` - Line item detail with profitability
- `fct_daily_summary` - Daily aggregated metrics per location
- `fct_inventory_snapshot` - Current inventory status
- `fct_customer_activity` - Customer lifetime metrics & RFM
- `fct_menu_item_performance` - Menu item performance rankings

**Report Views (15+):**
- Sales: `rpt_daily_sales_summary`, `rpt_weekly_sales_summary`, `rpt_company_daily_totals`
- Customers: `rpt_customer_overview`, `rpt_rfm_segment_summary`, `rpt_top_customers`, `rpt_customers_at_risk`, `rpt_loyalty_tier_analysis`
- Menu: `rpt_top_selling_items`, `rpt_category_performance`, `rpt_menu_item_profitability`
- Locations: `rpt_location_performance`, `rpt_location_ranking`
- Inventory: `rpt_inventory_status`, `rpt_inventory_alerts`, `rpt_inventory_value_by_location`

## Data Quality Expectations

DLT provides three types of expectations:

```python
# Warn only - log violations but keep rows
@dlt.expect("valid_email", "email LIKE '%@%.%'")

# Drop rows that fail validation
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")

# Fail pipeline if expectation fails
@dlt.expect_or_fail("critical_check", "total_amount >= 0")
```

### Implemented Expectations

| Table | Expectation | Type |
|-------|-------------|------|
| `stg_customers` | `customer_id IS NOT NULL` | drop |
| `stg_customers` | Valid email format | warn |
| `stg_employees` | `employee_id IS NOT NULL` | drop |
| `stg_orders` | `order_id IS NOT NULL` | drop |
| `stg_orders` | `total_amount >= 0` | warn |
| `stg_order_items` | `quantity > 0` | warn |
| `stg_menu_items` | `price > 0` | warn |
| `stg_inventory` | `quantity_on_hand >= 0` | warn |

## Pipeline Configuration

### Creating the Pipeline

1. Go to **Workflows > Delta Live Tables > Create Pipeline**

2. Configure settings:
   ```
   Pipeline name: sammys_sandwich_shop_dlt
   Product edition: Advanced (recommended) or Pro
   Pipeline mode: Triggered (batch) or Continuous (streaming)
   
   Source code:
   - /Workspace/path/to/dlt/bronze/ingest.py
   - /Workspace/path/to/dlt/silver/staging.py
   - /Workspace/path/to/dlt/silver/enriched.py
   - /Workspace/path/to/dlt/gold/dimensions.py
   - /Workspace/path/to/dlt/gold/facts.py
   - /Workspace/path/to/dlt/gold/reports.py
   
   Target catalog: sammys_sandwich_shop
   Target schema: dlt
   ```

3. Configure cluster:
   ```
   Cluster mode: Enhanced autoscaling
   Min workers: 1
   Max workers: 4
   ```

### Pipeline JSON Configuration

You can also create the pipeline using the API with this configuration:

```json
{
  "name": "sammys_sandwich_shop_dlt",
  "catalog": "sammys_sandwich_shop",
  "target": "dlt",
  "libraries": [
    {"notebook": {"path": "/Workspace/path/to/dlt/config"}},
    {"notebook": {"path": "/Workspace/path/to/dlt/bronze/ingest"}},
    {"notebook": {"path": "/Workspace/path/to/dlt/silver/staging"}},
    {"notebook": {"path": "/Workspace/path/to/dlt/silver/enriched"}},
    {"notebook": {"path": "/Workspace/path/to/dlt/gold/dimensions"}},
    {"notebook": {"path": "/Workspace/path/to/dlt/gold/facts"}},
    {"notebook": {"path": "/Workspace/path/to/dlt/gold/reports"}}
  ],
  "configuration": {
    "pipelines.enableTrackHistory": "true"
  },
  "clusters": [
    {
      "label": "default",
      "autoscale": {
        "min_workers": 1,
        "max_workers": 4,
        "mode": "ENHANCED"
      }
    }
  ],
  "development": true,
  "continuous": false,
  "channel": "CURRENT",
  "edition": "ADVANCED",
  "photon": true
}
```

## Running the Pipeline

### Development Mode
```python
# In development mode, the pipeline:
# - Uses less resources
# - Retains cluster for faster iteration
# - Does not publish to target schema

# Start development run from UI or API
```

### Production Mode
```python
# In production mode, the pipeline:
# - Publishes tables to target schema
# - Applies full data quality checks
# - Records full lineage

# Set "development": false in configuration
```

### Scheduled Runs
```python
# Create a job to run the pipeline on a schedule
# Workflows > Jobs > Create Job
# Task type: Delta Live Tables pipeline
# Schedule: Daily at 2:00 AM (or as needed)
```

## Monitoring

### Pipeline UI
The DLT pipeline UI provides:
- **DAG visualization** - See all table dependencies
- **Data quality metrics** - Pass/fail rates for expectations
- **Performance metrics** - Processing time, rows processed
- **Lineage tracking** - Full data lineage from source to target

### Event Log
Query the event log for detailed metrics:
```sql
SELECT * FROM event_log(TABLE(sammys_sandwich_shop.dlt.bronze_customers))
WHERE event_type = 'flow_progress'
ORDER BY timestamp DESC
```

## Comparison with Notebooks Version

### Advantages of DLT
1. **Simpler code** - No manual orchestration or error handling
2. **Built-in quality** - Data quality as first-class feature
3. **Better observability** - Pipeline UI with full lineage
4. **Automatic retries** - Handles transient failures
5. **Schema evolution** - Automatic schema management

### When to Use Notebooks Instead
1. Complex custom logic that doesn't fit DLT patterns
2. Need for interactive development and debugging
3. Integration with external systems in complex ways
4. One-off data exploration or ad-hoc analysis

## Prerequisites

1. **Databricks Workspace** with Unity Catalog enabled
2. **Catalog created**: `sammys_sandwich_shop`
3. **Volume created** for CSV files: `/Volumes/sammys_sandwich_shop/raw/csv_files`
4. **CSV files uploaded** to the volume

## Setup Instructions

1. Upload all notebooks to your Databricks workspace
2. Update `config.py` with your data path if different
3. Create the DLT pipeline in the Databricks UI
4. Add all source notebooks to the pipeline
5. Run the pipeline in development mode first
6. Validate results and promote to production

## Troubleshooting

### Common Issues

**"Table not found" errors:**
- Ensure all notebooks are included in the pipeline
- Check that `dlt.read()` references match table names

**Data quality failures:**
- Check the event log for specific violation details
- Adjust expectations or fix source data

**Performance issues:**
- Increase max workers for larger datasets
- Enable Photon for faster processing
- Consider partitioning large tables

### Support

For issues specific to this implementation, check:
1. Pipeline event log for detailed errors
2. Cluster logs for infrastructure issues
3. Data quality tab for expectation failures
