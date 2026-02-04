# Sammy's Sandwich Shop - Snowflake Data Pipeline

A complete Snowflake data warehouse project demonstrating the transformation of data from raw sources through to analytics-ready reports using the **three-layer architecture**: Raw, Enriched, and Ready.

## Business Context

Sammy's Sandwich Shop is a fictional sandwich business with multiple locations. This project models their operational data including:
- Customer information and loyalty program
- Menu items and ingredients
- Daily orders and transactions
- Inventory management
- Employee data

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                            DATA WAREHOUSE LAYERS                             │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────┐    ┌────────────────┐    ┌────────────────────────────┐  │
│  │   RAW LAYER    │    │ ENRICHED LAYER │    │        READY LAYER         │  │
│  │                │    │                │    │                            │  │
│  │ • customers    │    │ • enriched_    │    │ Dimensions:                │  │
│  │ • employees    │    │   customers    │    │ • dim_customer             │  │
│  │ • menu_items   │    │ • enriched_    │    │ • dim_employee             │  │
│  │ • ingredients  │    │   orders       │    │ • dim_menu_item            │  │
│  │ • orders       │    │ • enriched_    │    │ • dim_date                 │  │
│  │ • order_items  │───►│   menu         │───►│ • dim_location             │  │
│  │ • inventory    │    │ • enriched_    │    │                            │  │
│  │ • suppliers    │    │   inventory    │    │ Facts:                     │  │
│  │ • locations    │    │                │    │ • fact_sales               │  │
│  │                │    │                │    │ • fact_inventory_snapshot  │  │
│  └───────┬────────┘    └───────┬────────┘    └─────────────┬──────────────┘  │
│          │                     │                           │                 │
│          └─────────────────────┴───────────────────────────┘                 │
│                                    │                                         │
│                      ┌─────────────▼─────────────┐                           │
│                      │    DATA QUALITY LAYER     │                           │
│                      │  • Null checks            │                           │
│                      │  • Duplicate checks       │                           │
│                      │  • Referential integrity  │                           │
│                      │  • Range validation       │                           │
│                      └───────────────────────────┘                           │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Project Structure

```
snowflake-stored-procs/
├── 1_raw/
│   ├── ddl/                    # Table definitions for raw data
│   │   ├── 00_setup.sql        # Database and schema creation
│   │   └── 01_raw_tables.sql   # All raw table DDL
│   └── seed_data/              # CSV files to populate raw tables
│       ├── customers.csv
│       ├── employees.csv
│       ├── menu_items.csv
│       ├── ingredients.csv
│       ├── menu_item_ingredients.csv
│       ├── orders.csv
│       ├── order_items.csv
│       ├── inventory.csv
│       ├── suppliers.csv
│       └── locations.csv
│
├── 2_enriched/
│   ├── ddl/
│   │   └── 01_enriched_tables.sql
│   └── stored_procedures/
│       ├── sp_enrich_customers.sql
│       ├── sp_enrich_orders.sql
│       ├── sp_enrich_menu.sql
│       └── sp_enrich_inventory.sql
│
├── 3_ready/
│   ├── ddl/
│   │   ├── 01_dim_tables.sql
│   │   └── 02_fact_tables.sql
│   ├── stored_procedures/
│   │   ├── sp_load_dim_customer.sql
│   │   ├── sp_load_dim_employee.sql
│   │   ├── sp_load_dim_menu_item.sql
│   │   ├── sp_load_dim_date.sql
│   │   ├── sp_load_dim_location.sql
│   │   ├── sp_load_fact_sales.sql
│   │   └── sp_load_fact_inventory.sql
│   └── reports/
│       ├── rpt_daily_sales_summary.sql
│       ├── rpt_top_selling_items.sql
│       ├── rpt_customer_loyalty_analysis.sql
│       ├── rpt_ingredient_usage.sql
│       └── rpt_location_performance.sql
│
├── orchestration/
│   ├── SETUP.md                # Setup guide and instructions
│   ├── setup_python.py         # Automated setup/teardown script
│   ├── setup_snowsql.sql       # SnowSQL CLI setup commands
│   ├── setup_webui.md          # Web UI setup instructions
│   └── run_full_pipeline.sql   # Execute all transformations in order
│
└── data_quality/
    ├── README.md               # Data quality documentation
    ├── run_data_quality.py     # Python script for running checks
    ├── ddl/
    │   └── 01_data_quality_tables.sql
    ├── stored_procedures/
    │   └── sp_run_data_quality_checks.sql
    └── reports/
        └── rpt_data_quality_summary.sql
```

## Layer Descriptions

### 1. Raw Layer (Bronze)
- **Purpose**: Land data exactly as received from source systems
- **Schema**: `SAMMYS_RAW`
- **Characteristics**: 
  - No transformations applied
  - Preserves original data types and values
  - Includes metadata columns (load timestamp, source)

### 2. Enriched Layer (Silver)
- **Purpose**: Clean, validate, and standardize data
- **Schema**: `SAMMYS_ENRICHED`
- **Transformations**:
  - Data type standardization
  - Null handling and default values
  - Business rule validation
  - Deduplication
  - Joining related entities

### 3. Ready Layer (Gold)
- **Purpose**: Business-ready dimensional model for analytics
- **Schema**: `SAMMYS_READY`
- **Components**:
  - **Dimension Tables**: Slowly changing dimensions (SCD Type 2 where applicable)
  - **Fact Tables**: Transactional and snapshot facts
  - **Reports**: Pre-built analytical views

### 4. Data Quality Layer
- **Purpose**: Automated data validation and monitoring
- **Schema**: `SAMMYS_DATA_QUALITY`
- **Checks**:
  - Null validation on required fields
  - Primary key uniqueness
  - Referential integrity between tables
  - Value range validation
  - Row count consistency across layers

## Getting Started

### Quick Setup (Recommended)

The fastest way to get started is using the Python setup script:

```bash
# Install dependencies
pip install -r requirements.txt

# Set credentials
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_username"
export SNOWFLAKE_PASSWORD="your_password"

# Run setup (from project root)
python orchestration/setup_python.py
```

This creates everything and loads sample data in one command.

### Teardown

To remove everything from Snowflake:

```bash
python orchestration/setup_python.py --teardown
```

### Alternative Setup Methods

See the **[Setup Guide](orchestration/SETUP.md)** for:
- Step-by-step manual setup via Snowflake Web UI
- SnowSQL CLI-based setup
- Detailed troubleshooting

## Data Quality

Data quality checks run automatically as part of the pipeline. To run them separately:

```bash
# Run all data quality checks
python data_quality/run_data_quality.py

# Run checks for a specific layer
python data_quality/run_data_quality.py --layer enriched

# View historical results
python data_quality/run_data_quality.py --history
```

See [data_quality/README.md](data_quality/README.md) for full documentation.

## Sample Reports

| Report | Description |
|--------|-------------|
| Daily Sales Summary | Revenue, order count, and avg ticket by day |
| Top Selling Items | Best performing menu items by revenue and quantity |
| Customer Loyalty Analysis | Customer segments, repeat rates, lifetime value |
| Ingredient Usage | Ingredient consumption and cost analysis |
| Location Performance | Comparative metrics across store locations |

## Key Business Metrics

- **Average Order Value (AOV)**: Revenue per transaction
- **Customer Lifetime Value (CLV)**: Total revenue per customer
- **Food Cost Percentage**: Ingredient cost vs. menu price
- **Items per Order**: Average sandwich/item count per order
- **Peak Hours Analysis**: Order distribution by time of day

## License

This is a demonstration project for educational purposes.
