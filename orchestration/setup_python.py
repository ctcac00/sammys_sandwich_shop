#!/usr/bin/env python3
"""
Sammy's Sandwich Shop - Automated Snowflake Setup

This script automates the complete setup (or teardown) of the data warehouse.

Usage:
    # Set credentials via environment variables
    export SNOWFLAKE_ACCOUNT="your_account"
    export SNOWFLAKE_USER="your_username"
    export SNOWFLAKE_PASSWORD="your_password"
    
    # Full setup (creates everything and loads data)
    python orchestration/setup_python.py
    
    # Teardown (removes everything)
    python orchestration/setup_python.py --teardown
    
    # Load seed data only (assumes tables exist)
    python orchestration/setup_python.py --seed-only

Requirements:
    pip install -r requirements.txt
"""

import argparse
import os
import sys
from pathlib import Path
from typing import Optional

try:
    import snowflake.connector
    import pandas as pd
    from snowflake.connector.pandas_tools import write_pandas
except ImportError:
    print("Error: Required packages not installed.")
    print("Run: pip install snowflake-connector-python pandas")
    sys.exit(1)


# Configuration
DATABASE = "SAMMYS_SANDWICH_SHOP"
WAREHOUSE = "SAMMYS_WH"
SCHEMAS = {
    "raw": "SAMMYS_RAW",
    "enriched": "SAMMYS_ENRICHED", 
    "ready": "SAMMYS_READY"
}

# Table definitions with column mappings for seed data
SEED_TABLES = {
    "locations": [
        "location_id", "location_name", "address", "city", "state", "zip_code",
        "phone", "manager_employee_id", "open_date", "seating_capacity", "has_drive_thru"
    ],
    "customers": [
        "customer_id", "first_name", "last_name", "email", "phone", "address",
        "city", "state", "zip_code", "birth_date", "signup_date", "loyalty_tier",
        "loyalty_points", "preferred_location", "marketing_opt_in"
    ],
    "employees": [
        "employee_id", "first_name", "last_name", "email", "phone", "hire_date",
        "job_title", "department", "hourly_rate", "location_id", "manager_id",
        "employment_status"
    ],
    "menu_items": [
        "item_id", "item_name", "category", "subcategory", "description",
        "base_price", "is_active", "is_seasonal", "calories", "prep_time_minutes",
        "introduced_date"
    ],
    "ingredients": [
        "ingredient_id", "ingredient_name", "category", "unit_of_measure",
        "cost_per_unit", "supplier_id", "is_allergen", "allergen_type",
        "shelf_life_days", "storage_type"
    ],
    "menu_item_ingredients": [
        "item_id", "ingredient_id", "quantity_required", "is_optional", "extra_charge"
    ],
    "orders": [
        "order_id", "customer_id", "employee_id", "location_id", "order_date",
        "order_time", "order_type", "order_status", "subtotal", "tax_amount",
        "discount_amount", "tip_amount", "total_amount", "payment_method"
    ],
    "order_items": [
        "order_item_id", "order_id", "item_id", "quantity", "unit_price",
        "customizations", "line_total"
    ],
    "suppliers": [
        "supplier_id", "supplier_name", "contact_name", "email", "phone",
        "address", "city", "state", "zip_code", "payment_terms", "lead_time_days",
        "is_active"
    ],
    "inventory": [
        "inventory_id", "location_id", "ingredient_id", "snapshot_date",
        "quantity_on_hand", "quantity_reserved", "reorder_point", "reorder_quantity",
        "last_restock_date", "expiration_date"
    ]
}


def get_project_root() -> Path:
    """Find the project root directory."""
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / "1_raw").exists() and (current / "README.md").exists():
            return current
        current = current.parent
    # Fallback to script's parent's parent
    return Path(__file__).resolve().parent.parent


def get_connection(database: Optional[str] = None) -> snowflake.connector.SnowflakeConnection:
    """Create a Snowflake connection using environment variables."""
    account = os.environ.get("SNOWFLAKE_ACCOUNT")
    user = os.environ.get("SNOWFLAKE_USER")
    password = os.environ.get("SNOWFLAKE_PASSWORD")
    
    if not all([account, user, password]):
        print("Error: Missing Snowflake credentials.")
        print("Set these environment variables:")
        print("  export SNOWFLAKE_ACCOUNT='your_account'")
        print("  export SNOWFLAKE_USER='your_username'")
        print("  export SNOWFLAKE_PASSWORD='your_password'")
        sys.exit(1)
    
    conn_params = {
        "account": account,
        "user": user,
        "password": password,
    }
    
    if database:
        conn_params["database"] = database
        conn_params["warehouse"] = WAREHOUSE
    
    return snowflake.connector.connect(**conn_params)


def run_sql_file(conn: snowflake.connector.SnowflakeConnection, filepath: Path, description: str):
    """Execute a SQL file."""
    print(f"  Running: {filepath.name} ({description})")
    
    if not filepath.exists():
        print(f"    Warning: File not found - {filepath}")
        return
    
    sql_content = filepath.read_text()
    
    # Split on semicolons but handle edge cases
    statements = []
    current = []
    in_string = False
    string_char = None
    
    for char in sql_content:
        if char in ("'", '"') and not in_string:
            in_string = True
            string_char = char
        elif char == string_char and in_string:
            in_string = False
            string_char = None
        
        if char == ';' and not in_string:
            stmt = ''.join(current).strip()
            if stmt and not stmt.startswith('--'):
                statements.append(stmt)
            current = []
        else:
            current.append(char)
    
    # Don't forget the last statement
    stmt = ''.join(current).strip()
    if stmt and not stmt.startswith('--'):
        statements.append(stmt)
    
    cursor = conn.cursor()
    for stmt in statements:
        # Skip empty statements and comments
        clean_stmt = '\n'.join(
            line for line in stmt.split('\n') 
            if line.strip() and not line.strip().startswith('--')
        )
        if clean_stmt and not clean_stmt.startswith('/*'):
            try:
                cursor.execute(stmt)
            except Exception as e:
                # Ignore certain errors (like "already exists")
                if "already exists" not in str(e).lower():
                    print(f"    Warning: {e}")
    cursor.close()


def run_ddl_scripts(conn: snowflake.connector.SnowflakeConnection, project_root: Path):
    """Run all DDL scripts to create database objects."""
    print("\n[1/4] Creating database and schemas...")
    run_sql_file(conn, project_root / "1_raw/ddl/00_setup.sql", "database setup")
    
    # Reconnect with database context
    conn.close()
    conn = get_connection(DATABASE)
    
    print("\n[2/4] Creating tables...")
    run_sql_file(conn, project_root / "1_raw/ddl/01_raw_tables.sql", "raw tables")
    run_sql_file(conn, project_root / "2_enriched/ddl/01_enriched_tables.sql", "enriched tables")
    run_sql_file(conn, project_root / "3_ready/ddl/01_dim_tables.sql", "dimension tables")
    run_sql_file(conn, project_root / "3_ready/ddl/02_fact_tables.sql", "fact tables")
    
    print("\n[3/4] Creating stored procedures...")
    
    # Enriched layer procedures
    enriched_sp_dir = project_root / "2_enriched/stored_procedures"
    if enriched_sp_dir.exists():
        for sp_file in sorted(enriched_sp_dir.glob("*.sql")):
            run_sql_file(conn, sp_file, "enriched procedure")
    
    # Ready layer procedures
    ready_sp_dir = project_root / "3_ready/stored_procedures"
    if ready_sp_dir.exists():
        for sp_file in sorted(ready_sp_dir.glob("*.sql")):
            run_sql_file(conn, sp_file, "ready procedure")
    
    # Ready layer report views
    reports_dir = project_root / "3_ready/reports"
    if reports_dir.exists():
        for report_file in sorted(reports_dir.glob("*.sql")):
            run_sql_file(conn, report_file, "report view")
    
    return conn


def load_seed_data(conn: snowflake.connector.SnowflakeConnection, project_root: Path):
    """Load CSV seed data into raw tables."""
    print("\n[4/4] Loading seed data...")
    
    seed_dir = project_root / "1_raw/seed_data"
    if not seed_dir.exists():
        print(f"  Error: Seed data directory not found: {seed_dir}")
        return
    
    cursor = conn.cursor()
    cursor.execute(f"USE SCHEMA {SCHEMAS['raw']}")
    
    for table_name, columns in SEED_TABLES.items():
        csv_path = seed_dir / f"{table_name}.csv"
        
        if not csv_path.exists():
            print(f"  Warning: {csv_path.name} not found, skipping")
            continue
        
        print(f"  Loading: {table_name}")
        
        try:
            # Read CSV with pandas
            df = pd.read_csv(csv_path)
            
            # Normalize column names to match expected columns
            df.columns = [col.lower().strip() for col in df.columns]
            
            # Select only the columns we need (in case CSV has extra columns)
            available_cols = [col for col in columns if col in df.columns]
            df = df[available_cols]
            
            # Rename columns to uppercase for Snowflake
            df.columns = [col.upper() for col in df.columns]
            
            # Write to Snowflake
            success, num_chunks, num_rows, _ = write_pandas(
                conn, 
                df, 
                table_name.upper(),
                auto_create_table=False,
                overwrite=False
            )
            
            if success:
                print(f"    Loaded {num_rows} rows")
            else:
                print(f"    Warning: Load may have failed")
                
        except Exception as e:
            print(f"    Error loading {table_name}: {e}")
    
    cursor.close()


def run_pipeline(conn: snowflake.connector.SnowflakeConnection, project_root: Path):
    """Run the data transformation pipeline."""
    print("\n[5/5] Running data pipeline...")
    run_sql_file(conn, project_root / "orchestration/run_full_pipeline.sql", "full pipeline")


def verify_setup(conn: snowflake.connector.SnowflakeConnection):
    """Verify the setup completed successfully."""
    print("\n" + "=" * 60)
    print("VERIFICATION")
    print("=" * 60)
    
    cursor = conn.cursor()
    
    # Check raw layer
    cursor.execute(f"USE SCHEMA {SCHEMAS['raw']}")
    print(f"\nRaw Layer ({SCHEMAS['raw']}):")
    for table in SEED_TABLES.keys():
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            status = "OK" if count > 0 else "EMPTY"
            print(f"  {table}: {count} rows [{status}]")
        except Exception as e:
            print(f"  {table}: ERROR - {e}")
    
    # Check ready layer
    cursor.execute(f"USE SCHEMA {SCHEMAS['ready']}")
    print(f"\nReady Layer ({SCHEMAS['ready']}):")
    ready_tables = ["dim_customer", "dim_location", "dim_menu_item", "fact_sales"]
    for table in ready_tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            status = "OK" if count > 0 else "EMPTY"
            print(f"  {table}: {count} rows [{status}]")
        except Exception as e:
            print(f"  {table}: ERROR - {e}")
    
    cursor.close()


def teardown(conn: snowflake.connector.SnowflakeConnection):
    """Remove all database objects."""
    print("\nTearing down Sammy's Sandwich Shop...")
    
    cursor = conn.cursor()
    
    print(f"  Dropping database: {DATABASE}")
    cursor.execute(f"DROP DATABASE IF EXISTS {DATABASE} CASCADE")
    
    print(f"  Dropping warehouse: {WAREHOUSE}")
    cursor.execute(f"DROP WAREHOUSE IF EXISTS {WAREHOUSE}")
    
    cursor.close()
    print("\nTeardown complete.")


def main():
    parser = argparse.ArgumentParser(
        description="Setup or teardown Sammy's Sandwich Shop data warehouse"
    )
    parser.add_argument(
        "--teardown", 
        action="store_true",
        help="Remove all database objects"
    )
    parser.add_argument(
        "--seed-only",
        action="store_true", 
        help="Only load seed data (assumes tables exist)"
    )
    parser.add_argument(
        "--no-pipeline",
        action="store_true",
        help="Skip running the transformation pipeline"
    )
    args = parser.parse_args()
    
    project_root = get_project_root()
    print(f"Project root: {project_root}")
    
    if args.teardown:
        conn = get_connection()
        teardown(conn)
        conn.close()
        return
    
    print("\n" + "=" * 60)
    print("SAMMY'S SANDWICH SHOP - SNOWFLAKE SETUP")
    print("=" * 60)
    
    if args.seed_only:
        conn = get_connection(DATABASE)
        load_seed_data(conn, project_root)
        verify_setup(conn)
        conn.close()
        return
    
    # Full setup
    conn = get_connection()
    conn = run_ddl_scripts(conn, project_root)
    
    # Reconnect with database context for data loading
    conn.close()
    conn = get_connection(DATABASE)
    
    load_seed_data(conn, project_root)
    
    if not args.no_pipeline:
        run_pipeline(conn, project_root)
    
    verify_setup(conn)
    
    conn.close()
    
    print("\n" + "=" * 60)
    print("SETUP COMPLETE")
    print("=" * 60)
    print(f"\nDatabase: {DATABASE}")
    print(f"Warehouse: {WAREHOUSE}")
    print("\nTo tear down:")
    print("  python orchestration/setup_python.py --teardown")


if __name__ == "__main__":
    main()
