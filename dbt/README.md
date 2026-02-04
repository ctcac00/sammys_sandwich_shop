# Sammy's Sandwich Shop - dbt Project

dbt implementation of the Sammy's Sandwich Shop data warehouse, recreating the same transformations and reports as the Snowflake stored procedures project using dbt's declarative approach.

## Prerequisites

**Important**: This dbt project uses the **existing Snowflake RAW schema** as its source. Before running this dbt project, you must first set up the Snowflake project to create and populate the raw tables.

See the [Snowflake project README](../snowflake/README.md) for instructions on:
- Creating the `SAMMYS_SANDWICH_SHOP` database
- Creating the `SAMMYS_RAW` schema with source tables
- Loading seed data from CSV files

## Architecture Overview

This project implements the same three-layer architecture as the Snowflake project:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           dbt MODEL LAYERS                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────────────┐  │
│  │   STAGING       │    │  INTERMEDIATE   │    │        MARTS            │  │
│  │   (stg_*)       │    │    (int_*)      │    │                         │  │
│  │                 │    │                 │    │  Dimensions (dim_*)     │  │
│  │ • stg_customers │    │ • int_customers │    │  • dim_customer         │  │
│  │ • stg_employees │    │ • int_employees │    │  • dim_employee         │  │
│  │ • stg_orders    │───►│ • int_orders    │───►│  • dim_menu_item        │  │
│  │ • stg_menu_items│    │ • int_menu_items│    │  • dim_date             │  │
│  │ • stg_inventory │    │ • int_inventory │    │  • dim_location         │  │
│  │ • ...           │    │ • ...           │    │                         │  │
│  │                 │    │                 │    │  Facts (fct_*)          │  │
│  │                 │    │                 │    │  • fct_sales            │  │
│  │                 │    │                 │    │  • fct_daily_summary    │  │
│  │                 │    │                 │    │  • fct_inventory        │  │
│  │                 │    │                 │    │                         │  │
│  │                 │    │                 │    │  Reports (rpt_*)        │  │
│  │                 │    │                 │    │  • rpt_daily_sales      │  │
│  │                 │    │                 │    │  • rpt_top_customers    │  │
│  │                 │    │                 │    │  • rpt_location_perf    │  │
│  └────────┬────────┘    └────────┬────────┘    └────────────┬────────────┘  │
│           │                      │                          │               │
│           └──────────────────────┴──────────────────────────┘               │
│                                  │                                          │
│               ┌──────────────────▼──────────────────┐                       │
│               │     SNOWFLAKE RAW SCHEMA            │                       │
│               │     (Source - see Snowflake project)│                       │
│               └─────────────────────────────────────┘                       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Project Structure

```
dbt/
├── dbt_project.yml         # Project configuration
├── packages.yml            # dbt package dependencies
├── profiles.yml.example    # Example connection profile
├── README.md               # This file
│
├── models/
│   ├── staging/            # Stage raw data with type casting
│   │   ├── _sources.yml    # Source definitions (points to SAMMYS_RAW)
│   │   ├── _staging.yml    # Model documentation & tests
│   │   ├── stg_customers.sql
│   │   ├── stg_employees.sql
│   │   ├── stg_locations.sql
│   │   ├── stg_menu_items.sql
│   │   ├── stg_ingredients.sql
│   │   ├── stg_menu_item_ingredients.sql
│   │   ├── stg_orders.sql
│   │   ├── stg_order_items.sql
│   │   ├── stg_suppliers.sql
│   │   └── stg_inventory.sql
│   │
│   ├── intermediate/       # Business logic & enrichment
│   │   ├── _intermediate.yml
│   │   ├── int_customers.sql
│   │   ├── int_employees.sql
│   │   ├── int_locations.sql
│   │   ├── int_menu_items.sql
│   │   ├── int_ingredients.sql
│   │   ├── int_menu_item_ingredients.sql
│   │   ├── int_orders.sql
│   │   ├── int_order_items.sql
│   │   ├── int_suppliers.sql
│   │   └── int_inventory.sql
│   │
│   └── marts/              # Analytics-ready models
│       ├── dimensions/     # Dimension tables
│       │   ├── _dimensions.yml
│       │   ├── dim_date.sql
│       │   ├── dim_time.sql
│       │   ├── dim_customer.sql
│       │   ├── dim_employee.sql
│       │   ├── dim_location.sql
│       │   ├── dim_menu_item.sql
│       │   ├── dim_ingredient.sql
│       │   ├── dim_payment_method.sql
│       │   └── dim_order_type.sql
│       │
│       ├── facts/          # Fact tables
│       │   ├── _facts.yml
│       │   ├── fct_sales.sql
│       │   ├── fct_sales_line_item.sql
│       │   ├── fct_inventory_snapshot.sql
│       │   ├── fct_daily_summary.sql
│       │   ├── fct_customer_activity.sql
│       │   └── fct_menu_item_performance.sql
│       │
│       └── reports/        # Report views
│           ├── _reports.yml
│           ├── rpt_daily_sales_summary.sql
│           ├── rpt_weekly_sales_summary.sql
│           ├── rpt_company_daily_totals.sql
│           ├── rpt_customer_overview.sql
│           ├── rpt_rfm_segment_summary.sql
│           ├── rpt_loyalty_tier_analysis.sql
│           ├── rpt_top_customers.sql
│           ├── rpt_customers_at_risk.sql
│           ├── rpt_customer_cohort_analysis.sql
│           ├── rpt_top_selling_items.sql
│           ├── rpt_top_items_by_category.sql
│           ├── rpt_category_performance.sql
│           ├── rpt_menu_item_profitability.sql
│           ├── rpt_daily_item_sales.sql
│           ├── rpt_location_performance.sql
│           ├── rpt_location_ranking.sql
│           ├── rpt_location_daily_comparison.sql
│           ├── rpt_location_peak_hours.sql
│           ├── rpt_location_employee_performance.sql
│           ├── rpt_drive_thru_analysis.sql
│           ├── rpt_inventory_status.sql
│           ├── rpt_inventory_alerts.sql
│           ├── rpt_inventory_value_by_location.sql
│           ├── rpt_ingredient_cost_breakdown.sql
│           ├── rpt_estimated_ingredient_usage.sql
│           └── rpt_supplier_spend.sql
│
├── macros/                 # Custom macros (empty - uses dbt_utils)
├── tests/                  # Custom tests
├── seeds/                  # Static seed data (not used - using Snowflake RAW)
├── snapshots/              # SCD snapshots (not implemented)
└── analyses/               # Ad-hoc analyses
```

## Getting Started

### 1. Install dbt

```bash
# Using pip
pip install dbt-snowflake

# Or using homebrew (macOS)
brew install dbt-snowflake
```

### 2. Configure Connection

Copy the example profile and configure your Snowflake credentials:

```bash
# Copy to your dbt profiles directory
cp profiles.yml.example ~/.dbt/profiles.yml

# Edit with your credentials
# Or use environment variables:
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_username"
export SNOWFLAKE_PASSWORD="your_password"
```

### 3. Install Dependencies

```bash
cd dbt
dbt deps
```

### 4. Verify Connection

```bash
dbt debug
```

### 5. Run Models

```bash
# Run all models
dbt run

# Run specific layer
dbt run --select staging
dbt run --select intermediate
dbt run --select marts

# Run specific model type
dbt run --select tag:dimension
dbt run --select tag:fact
dbt run --select tag:report

# Run specific model
dbt run --select dim_customer
```

### 6. Run Tests

```bash
# Run all tests
dbt test

# Run tests for specific model
dbt test --select dim_customer
```

### 7. Generate Documentation

```bash
dbt docs generate
dbt docs serve
```

## Model Layers

### Staging (stg_*)
- **Purpose**: 1:1 with source tables
- **Transforms**: Data type casting, null handling, basic cleaning
- **Materialization**: View

### Intermediate (int_*)
- **Purpose**: Business logic and enrichment
- **Transforms**: Derived fields, calculations, joins
- **Materialization**: View

### Dimensions (dim_*)
- **Purpose**: Descriptive attributes for analysis
- **Features**: Surrogate keys, SCD Type 1
- **Materialization**: Table

### Facts (fct_*)
- **Purpose**: Business metrics and measures
- **Features**: Foreign keys to dimensions, calculated measures
- **Materialization**: Table

### Reports (rpt_*)
- **Purpose**: Pre-built analytical views
- **Features**: Joins facts with dimensions, aggregations, rankings
- **Materialization**: View

## Report Inventory

| Category | Report | Description |
|----------|--------|-------------|
| **Sales** | rpt_daily_sales_summary | Daily sales by location with comparisons |
| | rpt_weekly_sales_summary | Weekly aggregated sales |
| | rpt_company_daily_totals | Company-wide daily totals |
| **Customer** | rpt_customer_overview | Customer lifetime metrics & RFM |
| | rpt_rfm_segment_summary | RFM segment analysis |
| | rpt_loyalty_tier_analysis | Loyalty tier performance |
| | rpt_top_customers | Top 100 customers by spend |
| | rpt_customers_at_risk | Churn risk identification |
| | rpt_customer_cohort_analysis | Cohort retention analysis |
| **Menu** | rpt_top_selling_items | Best sellers with Pareto |
| | rpt_top_items_by_category | Top 5 per category |
| | rpt_category_performance | Category summary |
| | rpt_menu_item_profitability | Profitability quadrants |
| | rpt_daily_item_sales | Daily item breakdown |
| **Location** | rpt_location_performance | Location metrics |
| | rpt_location_ranking | Location rankings |
| | rpt_location_daily_comparison | Daily comparisons |
| | rpt_location_peak_hours | Peak hour analysis |
| | rpt_location_employee_performance | Employee metrics |
| | rpt_drive_thru_analysis | Drive-thru analysis |
| **Inventory** | rpt_inventory_status | Current stock status |
| | rpt_inventory_alerts | Low stock/expiring alerts |
| | rpt_inventory_value_by_location | Value summary |
| | rpt_ingredient_cost_breakdown | Ingredient costs per item |
| | rpt_estimated_ingredient_usage | Usage estimates |
| | rpt_supplier_spend | Supplier spend summary |

## Comparison: dbt vs Snowflake Stored Procedures

| Aspect | Snowflake Stored Procedures | dbt |
|--------|----------------------------|-----|
| **Approach** | Imperative (SQL scripts) | Declarative (models) |
| **Dependencies** | Manual ordering | Automatic via ref() |
| **Testing** | Custom scripts | Built-in testing framework |
| **Documentation** | README files | Auto-generated docs |
| **Version Control** | SQL files | SQL + YAML |
| **Incremental** | Manual implementation | Built-in strategies |
| **Modularity** | Stored procedures | Composable models |

## Resources

- [dbt Documentation](https://docs.getdbt.com/)
- [dbt-utils Package](https://hub.getdbt.com/dbt-labs/dbt_utils/latest/)
- [dbt-date Package](https://hub.getdbt.com/calogica/dbt_date/latest/)
- [Snowflake Project](../snowflake/README.md)
