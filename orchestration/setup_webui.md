# Setup via Snowflake Web UI

This guide walks you through loading seed data using the Snowflake web interface.

**Best for:** Quick manual setup, users new to Snowflake, one-time loads

## Prerequisites

Before loading data, you must have already run these scripts in Snowflake:

1. `1_raw/ddl/00_setup.sql` - Creates database and schemas
2. `1_raw/ddl/01_raw_tables.sql` - Creates raw layer tables

## Step-by-Step Instructions

### 1. Log into Snowflake

Go to [https://app.snowflake.com](https://app.snowflake.com) and sign in.

### 2. Navigate to the Raw Schema

1. Click **Data** in the left sidebar
2. Click **Databases**
3. Expand **SAMMYS_SANDWICH_SHOP**
4. Click on **SAMMYS_RAW**

### 3. Load Each CSV File

For each table listed below, repeat these steps:

1. Click on the **table name** in the left panel
2. Click the **Load Data** button (top right)
3. Select warehouse: **SAMMYS_WH**
4. Click **Browse** and select the corresponding CSV file from `1_raw/seed_data/`
5. Configure the file format:
   - **File type:** CSV
   - **Header lines to skip:** 1
   - **Field delimiter:** `,`
   - **Field optionally enclosed by:** `"`
6. Click **Load**
7. Wait for "Load completed successfully"

### Files to Load

| Table Name | CSV File |
|------------|----------|
| LOCATIONS | `1_raw/seed_data/locations.csv` |
| CUSTOMERS | `1_raw/seed_data/customers.csv` |
| EMPLOYEES | `1_raw/seed_data/employees.csv` |
| MENU_ITEMS | `1_raw/seed_data/menu_items.csv` |
| INGREDIENTS | `1_raw/seed_data/ingredients.csv` |
| MENU_ITEM_INGREDIENTS | `1_raw/seed_data/menu_item_ingredients.csv` |
| ORDERS | `1_raw/seed_data/orders.csv` |
| ORDER_ITEMS | `1_raw/seed_data/order_items.csv` |
| SUPPLIERS | `1_raw/seed_data/suppliers.csv` |
| INVENTORY | `1_raw/seed_data/inventory.csv` |

### 4. Verify Data Loaded

Open a new worksheet and run:

```sql
USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_RAW;

SELECT 'locations' AS table_name, COUNT(*) AS rows FROM locations
UNION ALL SELECT 'customers', COUNT(*) FROM customers
UNION ALL SELECT 'employees', COUNT(*) FROM employees
UNION ALL SELECT 'menu_items', COUNT(*) FROM menu_items
UNION ALL SELECT 'ingredients', COUNT(*) FROM ingredients
UNION ALL SELECT 'menu_item_ingredients', COUNT(*) FROM menu_item_ingredients
UNION ALL SELECT 'orders', COUNT(*) FROM orders
UNION ALL SELECT 'order_items', COUNT(*) FROM order_items
UNION ALL SELECT 'suppliers', COUNT(*) FROM suppliers
UNION ALL SELECT 'inventory', COUNT(*) FROM inventory
ORDER BY table_name;
```

All tables should show row counts greater than 0.

## Next Steps

After loading data successfully:

1. Run all stored procedure scripts in `2_enriched/stored_procedures/`
2. Run all stored procedure scripts in `3_ready/stored_procedures/`
3. Run `orchestration/run_full_pipeline.sql` to transform data through all layers

## Troubleshooting

### "Table not found" error
Run `1_raw/ddl/01_raw_tables.sql` to create the tables first.

### Load shows 0 rows
- Check that you selected "Header lines to skip: 1"
- Verify the CSV file is not empty
- Check for encoding issues (files should be UTF-8)

### "Warehouse suspended" error
The warehouse auto-suspends after 60 seconds of inactivity. It will auto-resume when you run the load.
