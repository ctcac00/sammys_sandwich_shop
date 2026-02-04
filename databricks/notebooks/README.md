# Sammy's Sandwich Shop - Databricks Notebooks

This project implements a data warehouse for Sammy's Sandwich Shop using Databricks notebooks and the medallion architecture (Bronze/Silver/Gold). It mirrors the functionality of the dbt/Snowflake implementation.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DATA ARCHITECTURE                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   CSV Files (data/)                                                         │
│        │                                                                    │
│        ▼                                                                    │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │  BRONZE LAYER (bronze schema)                                       │   │
│   │  Raw data loaded from CSV files with metadata columns               │   │
│   │  Tables: customers, employees, locations, menu_items, etc. (10)     │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│        │                                                                    │
│        ▼                                                                    │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │  SILVER LAYER (silver schema)                                       │   │
│   │  ├── Staging (stg_*): Type casting, cleaning, null handling (10)    │   │
│   │  └── Enriched (int_*): Derived fields, business logic (10)          │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│        │                                                                    │
│        ▼                                                                    │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │  GOLD LAYER (gold schema)                                           │   │
│   │  ├── Dimensions (dim_*): SCD Type 1 with surrogate keys (9)         │   │
│   │  ├── Facts (fct_*): Transactional and aggregated facts (6)          │   │
│   │  └── Reports (rpt_*): Analytical views (26)                         │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Project Structure

```
databricks/notebooks/
├── config.py                    # Shared configuration (catalog, schema names)
├── utils/
│   ├── __init__.py
│   └── helpers.py               # Shared helper functions
├── 00_setup/
│   └── setup_catalog.py         # Create catalog and schemas
├── 01_bronze/
│   ├── _run_all_bronze.py       # Orchestrator
│   └── load_*.py                # 10 loaders (one per CSV)
├── 02_silver/
│   ├── _run_all_silver.py       # Orchestrator
│   ├── staging/
│   │   └── stg_*.py             # 10 staging notebooks
│   └── enriched/
│       └── int_*.py             # 10 intermediate notebooks
├── 03_gold/
│   ├── _run_all_gold.py         # Orchestrator
│   ├── dimensions/
│   │   └── dim_*.py             # 9 dimension notebooks
│   ├── facts/
│   │   └── fct_*.py             # 6 fact notebooks
│   └── reports/
│       └── rpt_*.py             # 26 report notebooks
├── 04_tests/
│   ├── _run_all_tests.py        # Test orchestrator
│   ├── test_row_counts.py       # Source vs staging row counts
│   ├── test_data_quality.py     # Column value validations
│   └── test_aggregates.py       # Cross-table consistency
└── orchestration/
    └── run_full_pipeline.py     # Main orchestrator
```

## Getting Started

### Prerequisites

1. **Databricks Workspace** with Unity Catalog enabled
2. **CSV data files** uploaded to a Volume or cloud storage location
3. **Cluster** with Databricks Runtime 13.0+ (or DBR with Unity Catalog support)

### Setup Instructions

1. **Configure Data Path**
   
   Edit `config.py` and update `DATA_PATH` to point to your CSV files location:
   ```python
   # Option 1: Unity Catalog Volume (recommended)
   DATA_PATH = f"/Volumes/{CATALOG}/raw/csv_files"
   
   # Option 2: External cloud storage
   DATA_PATH = "abfss://container@storage.dfs.core.windows.net/sammys/csv_files"
   ```

2. **Upload CSV Files**
   
   Upload the following CSV files from `data/` to your configured location:
   - customers.csv
   - employees.csv
   - ingredients.csv
   - inventory.csv
   - locations.csv
   - menu_item_ingredients.csv
   - menu_items.csv
   - order_items.csv
   - orders.csv
   - suppliers.csv

3. **Run Setup**
   
   Execute `00_setup/setup_catalog.py` to create the catalog and schemas.

4. **Run Full Pipeline**
   
   Execute `orchestration/run_full_pipeline.py` to run the entire ETL pipeline.

## Layer Details

### Bronze Layer (01_bronze/)

Loads raw CSV data into Delta tables with metadata columns:
- `_loaded_at`: Timestamp when data was loaded
- `_source_file`: Name of the source CSV file

### Silver Layer (02_silver/)

**Staging (staging/):**
- Type casting (dates, numbers, booleans)
- String cleaning (trim, case normalization)
- Null handling with defaults
- Basic filtering (remove null primary keys)

**Enriched (enriched/):**
- Derived fields (age, tenure, full names)
- Business logic (tier ranks, categorizations)
- Cross-table enrichments

### Gold Layer (03_gold/)

**Dimensions (dimensions/):**
- Surrogate keys using MD5 hash
- SCD Type 1 implementation
- Unknown/default records for null handling

**Facts (facts/):**
- Transactional facts (fct_sales, fct_sales_line_item)
- Periodic snapshots (fct_inventory_snapshot)
- Accumulating snapshots (fct_customer_activity)
- Aggregated facts (fct_daily_summary, fct_menu_item_performance)

**Reports (reports/):**
- Pre-built analytical views
- Sales, Customer, Menu, Location, and Inventory reports
- Comparisons (day-over-day, week-over-week)

### Tests (04_tests/)

- **Row Count Tests**: Verify staging row counts match source
- **Data Quality Tests**: Validate value ranges, not null, unique constraints
- **Aggregate Consistency**: Cross-table validation (e.g., daily summary vs fact sales)

## Data Model

### Source Tables (10)

| Table | Description |
|-------|-------------|
| customers | Customer master data with loyalty program |
| employees | Employee records and assignments |
| locations | Store location master data |
| menu_items | Products available for sale |
| ingredients | Raw ingredients used in menu items |
| menu_item_ingredients | Recipe/BOM linking items to ingredients |
| orders | Order transaction headers |
| order_items | Order line items (detail rows) |
| suppliers | Vendor/supplier information |
| inventory | Daily inventory snapshots |

### Dimension Tables (9)

| Table | Description |
|-------|-------------|
| dim_customer | Customer dimension with demographics, loyalty |
| dim_employee | Employee dimension with job, tenure |
| dim_location | Location dimension with geography |
| dim_menu_item | Menu item dimension with pricing, nutrition |
| dim_ingredient | Ingredient dimension with cost, allergens |
| dim_payment_method | Payment method reference |
| dim_order_type | Order type reference |
| dim_date | Date dimension (2020-2030) |
| dim_time | Time dimension with time-of-day attributes |

### Fact Tables (6)

| Table | Grain | Description |
|-------|-------|-------------|
| fct_sales | Order | Order-level sales metrics |
| fct_sales_line_item | Line item | Line item detail with profitability |
| fct_daily_summary | Location/Day | Aggregated daily metrics |
| fct_inventory_snapshot | Location/Ingredient/Day | Inventory status |
| fct_customer_activity | Customer | Lifetime metrics, RFM scores |
| fct_menu_item_performance | Menu item | Item performance rankings |

### Report Views (26)

- **Sales (3)**: Daily summary, weekly summary, company totals
- **Customer (6)**: Overview, RFM segments, loyalty analysis, top customers, at-risk, cohorts
- **Menu (5)**: Top sellers, category performance, profitability analysis
- **Location (6)**: Performance, rankings, comparisons, peak hours, drive-thru
- **Inventory (6)**: Status, alerts, value, cost breakdown, usage, supplier spend

## Comparison with dbt Project

| Aspect | dbt | Databricks Notebooks |
|--------|-----|---------------------|
| Transformation Language | SQL (Jinja) | PySpark + SQL |
| Orchestration | dbt run | dbutils.notebook.run |
| Testing | dbt test | Custom Python assertions |
| Materialization | table/view/incremental | Delta tables/views |
| Surrogate Keys | dbt_utils.generate_surrogate_key | MD5 hash |
| Date Spine | dbt_date.get_date_dimension | Custom generation |
