#!/usr/bin/env python3
"""
Sammy's Sandwich Shop - Automated Snowflake Setup

This script automates the complete setup (or teardown) of the data warehouse.

Usage:
    # Set credentials via environment variables
    export SNOWFLAKE_ACCOUNT="your_account"
    export SNOWFLAKE_USER="your_username"
    export SNOWFLAKE_PASSWORD="your_password"
    
    # Authentication options (via SNOWFLAKE_AUTHENTICATOR):
    #   - Default (no setting): password + Duo/MFA push notification
    #   - externalbrowser: opens browser for SSO
    #   - snowflake: basic password auth (no MFA)
    
    # Interactive setup (prompts before each step)
    python orchestration/setup_python.py
    
    # Non-interactive setup (auto-accept all steps)
    python orchestration/setup_python.py -y
    
    # Teardown (removes everything)
    python orchestration/setup_python.py --teardown
    
    # Load seed data only (assumes tables exist)
    python orchestration/setup_python.py --seed-only

Requirements:
    pip install -r requirements.txt
"""

import argparse
import os
import re
import sys
from pathlib import Path
from typing import Optional

# Global flag for auto-accept
AUTO_ACCEPT = False


def confirm_step(step_name: str, description: str = "") -> bool:
    """Prompt user to confirm a step. Returns True if user wants to proceed."""
    if AUTO_ACCEPT:
        return True
    
    print()
    if description:
        print(f"  {description}")
    
    while True:
        response = input(f"  Run '{step_name}'? [y/n/q] (y=yes, n=skip, q=quit): ").strip().lower()
        if response in ('y', 'yes', ''):
            return True
        elif response in ('n', 'no', 's', 'skip'):
            print(f"  Skipping {step_name}...")
            return False
        elif response in ('q', 'quit', 'exit'):
            print("\nSetup cancelled by user.")
            sys.exit(0)
        else:
            print("  Please enter 'y' (yes), 'n' (skip), or 'q' (quit)")

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
    "ready": "SAMMYS_READY",
    "data_quality": "SAMMYS_DATA_QUALITY"
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
    """Create a Snowflake connection using environment variables.
    
    Supports multiple authentication methods:
    - username_password_mfa: Password + Duo/MFA push notification (default when password is set)
    - externalbrowser: Opens browser for SSO authentication
    - snowflake: Basic password auth (no MFA)
    """
    account = os.environ.get("SNOWFLAKE_ACCOUNT")
    user = os.environ.get("SNOWFLAKE_USER")
    password = os.environ.get("SNOWFLAKE_PASSWORD")
    authenticator = os.environ.get("SNOWFLAKE_AUTHENTICATOR", "").lower()
    
    if not account or not user:
        print("Error: Missing Snowflake credentials.")
        print("Set these environment variables:")
        print("  export SNOWFLAKE_ACCOUNT='your_account'")
        print("  export SNOWFLAKE_USER='your_username'")
        print("  export SNOWFLAKE_PASSWORD='your_password'")
        print("")
        print("Authentication options (SNOWFLAKE_AUTHENTICATOR):")
        print("  username_password_mfa  - Password + Duo push (default)")
        print("  externalbrowser        - Opens browser for SSO")
        print("  snowflake              - Basic password (no MFA)")
        sys.exit(1)
    
    if not password and authenticator != "externalbrowser":
        print("Error: SNOWFLAKE_PASSWORD is required for this auth method.")
        sys.exit(1)
    
    conn_params = {
        "account": account,
        "user": user,
    }
    
    if authenticator == "externalbrowser":
        conn_params["authenticator"] = "externalbrowser"
        print("  Using browser authentication (SSO)...")
    elif authenticator == "snowflake":
        conn_params["password"] = password
        print("  Using basic password authentication...")
    else:
        # Default: username_password_mfa for Duo/MFA push
        conn_params["password"] = password
        conn_params["authenticator"] = "username_password_mfa"
        print("  Using password + MFA (approve the Duo push on your device)...")
    
    if database:
        conn_params["database"] = database
        conn_params["warehouse"] = WAREHOUSE
    
    return snowflake.connector.connect(**conn_params)


def run_sql_file(conn: snowflake.connector.SnowflakeConnection, filepath: Path, description: str):
    """Execute a SQL file."""
    debug = os.environ.get("DEBUG_SQL", "").lower() in ("1", "true", "yes")
    print(f"  Running: {filepath.name} ({description})")
    
    if not filepath.exists():
        print(f"    Warning: File not found - {filepath}")
        return
    
    sql_content = filepath.read_text()
    
    # Split on semicolons but handle edge cases
    # Must track: string literals ('/""), block comments (/* */), and $$ delimited blocks (stored procedures)
    statements = []
    current = []
    in_string = False
    string_char = None
    in_block_comment = False
    in_dollar_block = False
    i = 0
    
    while i < len(sql_content):
        char = sql_content[i]
        
        # Check for block comment start: /*
        if not in_string and not in_dollar_block and not in_block_comment:
            if i + 1 < len(sql_content) and sql_content[i:i+2] == '/*':
                current.append('/*')
                in_block_comment = True
                i += 2
                continue
        
        # Check for block comment end: */
        if in_block_comment:
            if i + 1 < len(sql_content) and sql_content[i:i+2] == '*/':
                current.append('*/')
                in_block_comment = False
                i += 2
                continue
            # While in block comment, just append and continue
            current.append(char)
            i += 1
            continue
        
        # Check for $$ delimiter (used in stored procedures)
        if not in_string and i + 1 < len(sql_content) and sql_content[i:i+2] == '$$':
            current.append('$$')
            in_dollar_block = not in_dollar_block
            i += 2
            continue
        
        # Track string literals (only when not in $$ block)
        if not in_dollar_block:
            if char in ("'", '"') and not in_string:
                in_string = True
                string_char = char
            elif char == string_char and in_string:
                in_string = False
                string_char = None
        
        # Split on semicolons only when not inside a string or $$ block
        if char == ';' and not in_string and not in_dollar_block:
            stmt = ''.join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
        else:
            current.append(char)
        
        i += 1
    
    # Don't forget the last statement
    stmt = ''.join(current).strip()
    if stmt:
        statements.append(stmt)
    
    # Filter statements: skip empty ones and comment-only ones
    filtered_statements = []
    for stmt in statements:
        # Skip empty statements and single-line comments
        clean_stmt = '\n'.join(
            line for line in stmt.split('\n') 
            if line.strip() and not line.strip().startswith('--')
        )
        if not clean_stmt:
            continue
        
        # Remove block comments to check if there's actual SQL content
        # This handles cases like: /* comment */ USE DATABASE ...
        stmt_without_block_comments = re.sub(r'/\*.*?\*/', '', clean_stmt, flags=re.DOTALL).strip()
        
        if not stmt_without_block_comments:
            # Statement is only comments, skip it
            continue
        
        filtered_statements.append(stmt)
    
    if debug:
        print(f"    DEBUG: Found {len(filtered_statements)} statements to execute")
        for idx, s in enumerate(filtered_statements):
            preview = s[:60].replace('\n', ' ').strip()
            semicolon_count = s.count(';')
            warning = " [WARNING: contains semicolons!]" if semicolon_count > 0 else ""
            print(f"    DEBUG: [{idx+1}] ({len(s)} chars, {semicolon_count} semicolons{warning}) {preview}...")
    
    # Execute statements one at a time
    cursor = conn.cursor()
    for stmt in filtered_statements:
        try:
            # Use execute with num_statements=1 to ensure single statement execution
            # If Snowflake detects multiple statements, this will fail clearly
            cursor.execute(stmt, num_statements=1)
        except snowflake.connector.errors.ProgrammingError as e:
            # Handle multi-statement error by trying without the restriction
            if "statement count" in str(e).lower():
                # Statement might contain subqueries or complex syntax that looks like multiple statements
                try:
                    cursor.execute(stmt)
                except Exception as inner_e:
                    if "already exists" not in str(inner_e).lower():
                        print(f"    Warning: {inner_e}")
            elif "already exists" not in str(e).lower():
                print(f"    Warning: {e}")
        except Exception as e:
            # Ignore certain errors (like "already exists")
            if "already exists" not in str(e).lower():
                print(f"    Warning: {e}")
    cursor.close()


def run_ddl_scripts(conn: snowflake.connector.SnowflakeConnection, project_root: Path):
    """Run all DDL scripts to create database objects."""
    
    # Step 1: Create database and schemas
    print("\n" + "-" * 50)
    print("[Step 1/6] Create database, warehouse, and schemas")
    print("-" * 50)
    if confirm_step("Create database/schemas", "Creates SAMMYS_SANDWICH_SHOP database, warehouse, and 3 schemas"):
        run_sql_file(conn, project_root / "1_raw/ddl/00_setup.sql", "database setup")
        
        # Reconnect with database context
        conn.close()
        conn = get_connection(DATABASE)
        
        # Ensure warehouse and schemas exist (in case 00_setup.sql had context issues)
        cursor = conn.cursor()
        print("  Ensuring warehouse exists...")
        try:
            cursor.execute(f"""
                CREATE WAREHOUSE IF NOT EXISTS {WAREHOUSE}
                WAREHOUSE_SIZE = 'X-SMALL'
                AUTO_SUSPEND = 60
                AUTO_RESUME = TRUE
                INITIALLY_SUSPENDED = TRUE
            """)
            cursor.execute(f"USE WAREHOUSE {WAREHOUSE}")
        except Exception as e:
            print(f"    Warning creating warehouse: {e}")
        
        print("  Ensuring schemas exist...")
        for schema_name in SCHEMAS.values():
            try:
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            except Exception as e:
                print(f"    Warning creating schema {schema_name}: {e}")
        cursor.close()
    else:
        # Still need to reconnect with database context
        conn.close()
        conn = get_connection(DATABASE)
    
    # Step 2: Create tables
    print("\n" + "-" * 50)
    print("[Step 2/6] Create tables")
    print("-" * 50)
    if confirm_step("Create tables", "Creates raw, enriched, and ready layer tables"):
        cursor = conn.cursor()
        
        # Set context and run raw tables
        cursor.execute(f"USE SCHEMA {SCHEMAS['raw']}")
        run_sql_file(conn, project_root / "1_raw/ddl/01_raw_tables.sql", "raw tables")
        
        # Set context and run enriched tables
        cursor.execute(f"USE SCHEMA {SCHEMAS['enriched']}")
        run_sql_file(conn, project_root / "2_enriched/ddl/01_enriched_tables.sql", "enriched tables")
        
        # Set context and run ready layer tables
        cursor.execute(f"USE SCHEMA {SCHEMAS['ready']}")
        run_sql_file(conn, project_root / "3_ready/ddl/01_dim_tables.sql", "dimension tables")
        run_sql_file(conn, project_root / "3_ready/ddl/02_fact_tables.sql", "fact tables")
        
        cursor.close()
    
    # Step 3: Create stored procedures
    print("\n" + "-" * 50)
    print("[Step 3/6] Create stored procedures")
    print("-" * 50)
    if confirm_step("Create stored procedures", "Creates enrichment and dimension loading procedures"):
        cursor = conn.cursor()
        
        # Enriched layer procedures
        cursor.execute(f"USE SCHEMA {SCHEMAS['enriched']}")
        enriched_sp_dir = project_root / "2_enriched/stored_procedures"
        if enriched_sp_dir.exists():
            for sp_file in sorted(enriched_sp_dir.glob("*.sql")):
                run_sql_file(conn, sp_file, "enriched procedure")
        
        # Ready layer procedures
        cursor.execute(f"USE SCHEMA {SCHEMAS['ready']}")
        ready_sp_dir = project_root / "3_ready/stored_procedures"
        if ready_sp_dir.exists():
            for sp_file in sorted(ready_sp_dir.glob("*.sql")):
                run_sql_file(conn, sp_file, "ready procedure")
        
        cursor.close()
    
    # Step 4: Create report views
    print("\n" + "-" * 50)
    print("[Step 4/6] Create report views")
    print("-" * 50)
    if confirm_step("Create report views", "Creates analytics views (may show warnings if fact tables are empty)"):
        cursor = conn.cursor()
        cursor.execute(f"USE SCHEMA {SCHEMAS['ready']}")
        
        reports_dir = project_root / "3_ready/reports"
        if reports_dir.exists():
            for report_file in sorted(reports_dir.glob("*.sql")):
                run_sql_file(conn, report_file, "report view")
        
        cursor.close()
    
    # Step 5: Create data quality objects
    print("\n" + "-" * 50)
    print("[Step 5/6] Create data quality framework")
    print("-" * 50)
    if confirm_step("Create data quality", "Creates schema, tables, and procedures for data quality checks"):
        cursor = conn.cursor()
        
        # Create data quality schema
        try:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMAS['data_quality']}")
        except Exception as e:
            print(f"    Warning creating schema: {e}")
        
        cursor.execute(f"USE SCHEMA {SCHEMAS['data_quality']}")
        
        # Run data quality DDL
        dq_ddl_dir = project_root / "data_quality/ddl"
        if dq_ddl_dir.exists():
            for ddl_file in sorted(dq_ddl_dir.glob("*.sql")):
                run_sql_file(conn, ddl_file, "data quality tables")
        
        # Run data quality stored procedures
        dq_sp_dir = project_root / "data_quality/stored_procedures"
        if dq_sp_dir.exists():
            for sp_file in sorted(dq_sp_dir.glob("*.sql")):
                run_sql_file(conn, sp_file, "data quality procedure")
        
        cursor.close()
    
    return conn


def load_seed_data(conn: snowflake.connector.SnowflakeConnection, project_root: Path):
    """Load CSV seed data into raw tables."""
    print("  Loading seed data...")
    
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
    print("  Running data pipeline...")
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
    parser.add_argument(
        "-y", "--yes",
        action="store_true",
        help="Auto-accept all prompts (non-interactive mode)"
    )
    args = parser.parse_args()
    
    # Set global auto-accept flag
    global AUTO_ACCEPT
    AUTO_ACCEPT = args.yes
    
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
    
    # Step 6: Load seed data
    print("\n" + "-" * 50)
    print("[Step 6/8] Load seed data")
    print("-" * 50)
    if confirm_step("Load seed data", "Loads CSV data into raw tables"):
        load_seed_data(conn, project_root)
    
    # Step 7: Run pipeline (includes data quality checks)
    if not args.no_pipeline:
        print("\n" + "-" * 50)
        print("[Step 7/8] Run transformation pipeline")
        print("-" * 50)
        if confirm_step("Run pipeline", "Executes stored procedures to transform data + data quality checks"):
            run_pipeline(conn, project_root)
    
    # Step 8: Verify setup
    print("\n" + "-" * 50)
    print("[Step 8/8] Verify setup")
    print("-" * 50)
    if confirm_step("Verify setup", "Checks row counts in tables"):
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
