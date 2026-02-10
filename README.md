# Sammy's Sandwich Shop - Multi-Platform Data Warehouse

A complete data warehouse project demonstrating the transformation of data from raw sources through to analytics-ready reports using the **three-layer architecture** (Raw, Enriched, Ready) across multiple platforms.

## Business Context

Sammy's Sandwich Shop is a fictional sandwich business with multiple locations. This project models their operational data including:
- Customer information and loyalty program
- Menu items and ingredients
- Daily orders and transactions
- Inventory management
- Employee data

## Project Structure

```
├── data/                    # Shared seed data (CSV files)
│   ├── customers.csv
│   ├── employees.csv
│   ├── ingredients.csv
│   ├── inventory.csv
│   ├── locations.csv
│   ├── menu_item_ingredients.csv
│   ├── menu_items.csv
│   ├── order_items.csv
│   ├── orders.csv
│   └── suppliers.csv
│
├── snowflake/               # Snowflake stored procedures implementation
│   ├── 1_raw/               # Raw layer DDL
│   ├── 2_enriched/          # Enriched layer with stored procedures
│   ├── 3_ready/             # Ready layer with dims, facts, reports
│   ├── data_quality/        # Data quality framework
│   ├── orchestration/       # Setup and pipeline scripts
│   └── README.md
│
├── dbt/                     # dbt + Snowflake implementation
│   ├── models/              # Staging, intermediate, marts
│   ├── tests/               # Data quality tests
│   └── README.md
│
└── databricks/              # Databricks implementations
    ├── notebooks/           # PySpark notebooks (manual orchestration)
    │   ├── 00_setup/        # Catalog setup
    │   ├── 01_bronze/       # Raw data loading
    │   ├── 02_silver/       # Staging and enriched transforms
    │   ├── 03_gold/         # Dimensions, facts, reports
    │   ├── 04_tests/        # Data quality tests
    │   └── orchestration/   # Pipeline runner
    │
    └── dlt/                 # Delta Live Tables (declarative)
        ├── bronze/          # Raw ingestion tables
        ├── silver/          # Staging and enriched tables
        └── gold/            # Dimensions, facts, report views
```

## Platform Implementations

### Snowflake (Stored Procedures)

Full implementation with stored procedures, dimensional model, and data quality framework.

See [snowflake/README.md](snowflake/README.md) for setup and documentation.

```bash
cd snowflake
pip install -r requirements.txt
python orchestration/setup_python.py
```

### dbt + Snowflake

dbt implementation using the same Snowflake RAW schema as the source. Recreates all transformations and reports using dbt's declarative approach.

See [dbt/README.md](dbt/README.md) for setup and documentation.

```bash
cd dbt
dbt deps
dbt run
```

**Note**: The dbt project uses the existing Snowflake RAW schema as its source. Run the Snowflake setup first to create and populate the raw tables before running dbt.

### Databricks Notebooks

PySpark implementation using traditional Databricks notebooks with manual orchestration. Demonstrates the medallion architecture (Bronze/Silver/Gold) with explicit pipeline control.

See [databricks/notebooks/README.md](databricks/notebooks/README.md) for setup and documentation.

**Features:**
- 65+ notebooks organized by layer
- Custom orchestration scripts (`_run_all_*.py`)
- Separate data quality test notebooks
- Unity Catalog integration

```bash
# Upload notebooks to Databricks workspace
# Run orchestration/run_full_pipeline.py
```

### Databricks Delta Live Tables (DLT)

Declarative pipeline implementation using Delta Live Tables. Same transformations as the notebooks version but with automatic dependency management and built-in data quality.

See [databricks/dlt/README.md](databricks/dlt/README.md) for setup and documentation.

**Features:**
- 7 notebooks (vs 65+ in traditional approach)
- Automatic DAG-based execution
- Built-in `@dlt.expect` data quality expectations
- Pipeline UI with lineage visualization

```bash
# Create DLT pipeline in Databricks UI
# Add all notebooks from databricks/dlt/
# Set target catalog and schema
```

### Implementation Comparison

| Aspect | Snowflake | dbt | Databricks Notebooks | Databricks DLT |
|--------|-----------|-----|---------------------|----------------|
| Language | SQL + JS | SQL (Jinja) | PySpark | PySpark |
| Orchestration | Stored procs | dbt CLI | Manual scripts | Automatic |
| Data Quality | Custom framework | dbt tests | Test notebooks | `@dlt.expect` |
| Execution | Snowflake engine | Snowflake engine | Spark cluster | Spark cluster |
| SCD Strategy | Type 2 ready (surrogate keys via AUTOINCREMENT) | Type 1 (hash-based surrogate keys) | Type 1 (hash-based surrogate keys) | Type 1 (hash-based surrogate keys) |
| Best For | SQL-centric teams | Analytics engineers | Custom Spark logic | Managed pipelines |

### Cross-Platform Design Notes

All platforms produce the **same** dimensional model (dimensions, facts, and reports) from the same source data. There are intentional per-platform differences:

- **Surrogate keys**: Snowflake uses `AUTOINCREMENT` integers with SCD Type 2 columns (`expiration_date`), making it ready for historical tracking. The other platforms use deterministic `md5()` hashes with SCD Type 1 (current state only).
- **Dimension columns**: The Databricks DLT implementation exposes additional enriched columns on some entity dimensions (e.g., `dim_location` includes `city_state`, `district`; `dim_employee` includes `tenure_group`, `tenure_years`) that go beyond the dbt/Snowflake schemas. This reflects DLT's design as a self-contained analytical pipeline. The core join keys and business keys remain identical.
- **Raw layer metadata**: The Snowflake raw tables include `_loaded_at` and `_source_file` audit columns added on ingestion. The Databricks bronze layers do not include these columns since data is loaded directly from CSV files.

## Data Model

All platforms share the same source data model:

| Table | Description |
|-------|-------------|
| customers | Customer information and loyalty data |
| employees | Staff records and assignments |
| locations | Store locations and details |
| menu_items | Products available for sale |
| ingredients | Raw materials for menu items |
| menu_item_ingredients | Recipe composition |
| orders | Transaction headers |
| order_items | Transaction line items |
| inventory | Stock levels by location |
| suppliers | Vendor information |

## Architecture

Each platform implements the same three-layer architecture:

1. **Raw Layer (Bronze)** - Land data exactly as received
2. **Enriched Layer (Silver)** - Clean, validate, and standardize
3. **Ready Layer (Gold)** - Business-ready dimensional model

## License

This is a demonstration project for educational purposes.
