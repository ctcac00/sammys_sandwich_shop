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
├── snowflake/               # Snowflake implementation
│   ├── 1_raw/               # Raw layer DDL
│   ├── 2_enriched/          # Enriched layer with stored procedures
│   ├── 3_ready/             # Ready layer with dims, facts, reports
│   ├── data_quality/        # Data quality framework
│   ├── orchestration/       # Setup and pipeline scripts
│   └── README.md            # Snowflake-specific documentation
│
├── databricks/              # Databricks implementation (planned)
│
└── dbt/                     # dbt implementation (planned)
```

## Platform Implementations

### Snowflake

Full implementation with stored procedures, dimensional model, and data quality framework.

See [snowflake/README.md](snowflake/README.md) for setup and documentation.

```bash
cd snowflake
pip install -r requirements.txt
python orchestration/setup_python.py
```

### Databricks

Coming soon.

### dbt

Coming soon.

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
