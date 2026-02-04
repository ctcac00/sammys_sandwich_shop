#!/usr/bin/env python3
"""
Sammy's Sandwich Shop - Data Quality Validation

This script runs data quality checks against the Snowflake data warehouse.

Usage:
    # Set credentials via environment variables
    export SNOWFLAKE_ACCOUNT="your_account"
    export SNOWFLAKE_USER="your_username"
    export SNOWFLAKE_PASSWORD="your_password"
    
    # Authentication options (via SNOWFLAKE_AUTHENTICATOR):
    #   - Default (no setting): password + Duo/MFA push notification
    #   - externalbrowser: opens browser for SSO
    #   - snowflake: basic password auth (no MFA)
    
    # Run all data quality checks
    python data_quality/run_data_quality.py
    
    # Run checks for a specific layer only
    python data_quality/run_data_quality.py --layer enriched
    
    # Setup only (create schema and tables, don't run checks)
    python data_quality/run_data_quality.py --setup-only
    
    # Show detailed results (including passed checks)
    python data_quality/run_data_quality.py --verbose

Requirements:
    pip install -r requirements.txt
"""

import argparse
import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional

try:
    import snowflake.connector
except ImportError:
    print("Error: Required packages not installed.")
    print("Run: pip install snowflake-connector-python")
    sys.exit(1)


# Configuration
DATABASE = "SAMMYS_SANDWICH_SHOP"
WAREHOUSE = "SAMMYS_WH"
DQ_SCHEMA = "SAMMYS_DATA_QUALITY"


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
    
    # Filter out comment-only statements
    import re
    filtered_statements = []
    for stmt in statements:
        clean_stmt = '\n'.join(
            line for line in stmt.split('\n') 
            if line.strip() and not line.strip().startswith('--')
        )
        if not clean_stmt:
            continue
        stmt_without_block_comments = re.sub(r'/\*.*?\*/', '', clean_stmt, flags=re.DOTALL).strip()
        if not stmt_without_block_comments:
            continue
        filtered_statements.append(stmt)
    
    # Execute statements
    cursor = conn.cursor()
    for stmt in filtered_statements:
        try:
            cursor.execute(stmt, num_statements=1)
        except snowflake.connector.errors.ProgrammingError as e:
            if "statement count" in str(e).lower():
                try:
                    cursor.execute(stmt)
                except Exception as inner_e:
                    if "already exists" not in str(inner_e).lower():
                        print(f"    Warning: {inner_e}")
            elif "already exists" not in str(e).lower():
                print(f"    Warning: {e}")
        except Exception as e:
            if "already exists" not in str(e).lower():
                print(f"    Warning: {e}")
    cursor.close()


def setup_data_quality_schema(conn: snowflake.connector.SnowflakeConnection, project_root: Path):
    """Create the data quality schema and tables."""
    print("\n" + "-" * 60)
    print("Setting up Data Quality Schema")
    print("-" * 60)
    
    cursor = conn.cursor()
    
    # Ensure we're using the right database and warehouse
    cursor.execute(f"USE DATABASE {DATABASE}")
    cursor.execute(f"USE WAREHOUSE {WAREHOUSE}")
    
    # Run the DDL script
    ddl_path = project_root / "data_quality/ddl/01_data_quality_tables.sql"
    run_sql_file(conn, ddl_path, "data quality tables")
    
    # Create the stored procedure
    sp_path = project_root / "data_quality/stored_procedures/sp_run_data_quality_checks.sql"
    run_sql_file(conn, sp_path, "data quality procedure")
    
    cursor.close()
    print("  Data quality schema setup complete.")


def run_data_quality_checks(conn: snowflake.connector.SnowflakeConnection, layer: str = "all") -> dict:
    """Run data quality checks and return the results."""
    print("\n" + "-" * 60)
    print(f"Running Data Quality Checks (layer: {layer})")
    print("-" * 60)
    
    cursor = conn.cursor()
    cursor.execute(f"USE DATABASE {DATABASE}")
    cursor.execute(f"USE SCHEMA {DQ_SCHEMA}")
    
    # Call the stored procedure
    print(f"  Executing sp_run_data_quality_checks('{layer}')...")
    cursor.execute(f"CALL sp_run_data_quality_checks('{layer}')")
    result = cursor.fetchone()[0]
    print(f"  Result: {result}")
    
    # Extract run_id from result
    run_id = result.split("Run ID: ")[-1] if "Run ID: " in result else None
    
    cursor.close()
    
    return {
        "result": result,
        "run_id": run_id
    }


def display_results(conn: snowflake.connector.SnowflakeConnection, run_id: str, verbose: bool = False):
    """Display the data quality check results."""
    print("\n" + "=" * 60)
    print("DATA QUALITY RESULTS")
    print("=" * 60)
    
    cursor = conn.cursor()
    cursor.execute(f"USE DATABASE {DATABASE}")
    cursor.execute(f"USE SCHEMA {DQ_SCHEMA}")
    
    # Get run summary
    cursor.execute(f"""
        SELECT 
            run_timestamp,
            run_type,
            overall_status,
            total_checks,
            checks_passed,
            checks_warned,
            checks_failed,
            duration_seconds
        FROM data_quality_runs
        WHERE run_id = '{run_id}'
    """)
    run = cursor.fetchone()
    
    if run:
        timestamp, run_type, status, total, passed, warned, failed, duration = run
        
        # Status colors (ANSI)
        status_color = {
            "PASSED": "\033[92m",   # Green
            "WARNING": "\033[93m",  # Yellow
            "FAILED": "\033[91m"    # Red
        }
        reset = "\033[0m"
        
        print(f"\nRun Summary:")
        print(f"  Timestamp:  {timestamp}")
        print(f"  Layer:      {run_type}")
        print(f"  Status:     {status_color.get(status, '')}{status}{reset}")
        print(f"  Duration:   {duration}s")
        print(f"\n  Checks:     {total} total")
        print(f"    Passed:   {passed} ({round(passed/total*100, 1) if total > 0 else 0}%)")
        print(f"    Warnings: {warned}")
        print(f"    Failed:   {failed}")
    
    # Get failed checks
    cursor.execute(f"""
        SELECT 
            layer,
            table_name,
            column_name,
            check_name,
            records_checked,
            records_failed,
            failure_percentage,
            error_details
        FROM data_quality_results
        WHERE run_id = '{run_id}' AND status = 'FAILED'
        ORDER BY failure_percentage DESC
    """)
    failed_checks = cursor.fetchall()
    
    if failed_checks:
        print("\n" + "-" * 60)
        print("FAILED CHECKS (require attention)")
        print("-" * 60)
        for check in failed_checks:
            layer, table, column, name, checked, failed_count, pct, details = check
            print(f"\n  [{layer.upper()}] {table}.{column or '*'}")
            print(f"    Check: {name}")
            print(f"    Failed: {failed_count:,} of {checked:,} records ({pct}%)")
            if details:
                print(f"    Details: {details[:100]}{'...' if len(str(details)) > 100 else ''}")
    
    # Get warnings
    cursor.execute(f"""
        SELECT 
            layer,
            table_name,
            column_name,
            check_name,
            records_failed,
            failure_percentage
        FROM data_quality_results
        WHERE run_id = '{run_id}' AND status = 'WARNING'
        ORDER BY failure_percentage DESC
    """)
    warnings = cursor.fetchall()
    
    if warnings:
        print("\n" + "-" * 60)
        print("WARNINGS (approaching threshold)")
        print("-" * 60)
        for warning in warnings:
            layer, table, column, name, failed_count, pct = warning
            print(f"  [{layer.upper()}] {table}.{column or '*'}: {name} ({failed_count} failed, {pct}%)")
    
    # Show passed checks if verbose
    if verbose:
        cursor.execute(f"""
            SELECT 
                layer,
                table_name,
                column_name,
                check_name,
                records_checked
            FROM data_quality_results
            WHERE run_id = '{run_id}' AND status = 'PASSED'
            ORDER BY layer, table_name
        """)
        passed_checks = cursor.fetchall()
        
        if passed_checks:
            print("\n" + "-" * 60)
            print("PASSED CHECKS")
            print("-" * 60)
            for check in passed_checks:
                layer, table, column, name, checked = check
                print(f"  [{layer.upper()}] {table}.{column or '*'}: {name} ({checked:,} records)")
    
    # Summary by layer
    cursor.execute(f"""
        SELECT 
            layer,
            COUNT(*) as total,
            COUNT_IF(status = 'PASSED') as passed,
            COUNT_IF(status = 'WARNING') as warned,
            COUNT_IF(status = 'FAILED') as failed
        FROM data_quality_results
        WHERE run_id = '{run_id}'
        GROUP BY layer
        ORDER BY CASE layer WHEN 'raw' THEN 1 WHEN 'enriched' THEN 2 WHEN 'ready' THEN 3 END
    """)
    by_layer = cursor.fetchall()
    
    if by_layer:
        print("\n" + "-" * 60)
        print("SUMMARY BY LAYER")
        print("-" * 60)
        print(f"  {'Layer':<12} {'Total':>8} {'Passed':>8} {'Warned':>8} {'Failed':>8}")
        print("  " + "-" * 48)
        for row in by_layer:
            layer, total, passed, warned, failed = row
            print(f"  {layer:<12} {total:>8} {passed:>8} {warned:>8} {failed:>8}")
    
    cursor.close()


def show_history(conn: snowflake.connector.SnowflakeConnection, limit: int = 10):
    """Show recent data quality run history."""
    print("\n" + "=" * 60)
    print("DATA QUALITY RUN HISTORY")
    print("=" * 60)
    
    cursor = conn.cursor()
    cursor.execute(f"USE DATABASE {DATABASE}")
    cursor.execute(f"USE SCHEMA {DQ_SCHEMA}")
    
    cursor.execute(f"""
        SELECT 
            run_timestamp,
            run_type,
            overall_status,
            total_checks,
            checks_passed,
            checks_warned,
            checks_failed,
            duration_seconds
        FROM data_quality_runs
        ORDER BY run_timestamp DESC
        LIMIT {limit}
    """)
    runs = cursor.fetchall()
    
    if runs:
        print(f"\n  {'Timestamp':<22} {'Layer':<10} {'Status':<10} {'Total':>6} {'Pass':>6} {'Warn':>6} {'Fail':>6}")
        print("  " + "-" * 74)
        for run in runs:
            timestamp, run_type, status, total, passed, warned, failed, duration = run
            ts_str = timestamp.strftime("%Y-%m-%d %H:%M:%S") if hasattr(timestamp, 'strftime') else str(timestamp)[:19]
            print(f"  {ts_str:<22} {run_type:<10} {status:<10} {total:>6} {passed:>6} {warned:>6} {failed:>6}")
    else:
        print("\n  No data quality runs found.")
    
    cursor.close()


def main():
    parser = argparse.ArgumentParser(
        description="Run data quality checks on Sammy's Sandwich Shop data warehouse"
    )
    parser.add_argument(
        "--layer",
        choices=["raw", "enriched", "ready", "all"],
        default="all",
        help="Which data layer to check (default: all)"
    )
    parser.add_argument(
        "--setup-only",
        action="store_true",
        help="Only create the data quality schema and tables, don't run checks"
    )
    parser.add_argument(
        "--skip-setup",
        action="store_true",
        help="Skip setup, assume schema already exists"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show all check results including passed checks"
    )
    parser.add_argument(
        "--history",
        action="store_true",
        help="Show recent data quality run history"
    )
    parser.add_argument(
        "--history-limit",
        type=int,
        default=10,
        help="Number of history records to show (default: 10)"
    )
    args = parser.parse_args()
    
    project_root = get_project_root()
    print(f"Project root: {project_root}")
    
    print("\n" + "=" * 60)
    print("SAMMY'S SANDWICH SHOP - DATA QUALITY VALIDATION")
    print("=" * 60)
    
    # Connect to Snowflake (without database first to ensure connection works)
    print("\nConnecting to Snowflake...")
    conn = get_connection()
    
    try:
        # Set database and warehouse context
        cursor = conn.cursor()
        print(f"  Using database: {DATABASE}")
        cursor.execute(f"USE DATABASE {DATABASE}")
        print(f"  Using warehouse: {WAREHOUSE}")
        cursor.execute(f"USE WAREHOUSE {WAREHOUSE}")
        cursor.close()
        
        # Show history if requested
        if args.history:
            show_history(conn, args.history_limit)
            conn.close()
            return
        
        # Setup schema if needed
        if not args.skip_setup:
            setup_data_quality_schema(conn, project_root)
        
        if args.setup_only:
            print("\n  Setup complete. Use --skip-setup to run checks without setup.")
            conn.close()
            return
        
        # Run checks
        result = run_data_quality_checks(conn, args.layer)
        
        # Display results
        if result["run_id"]:
            display_results(conn, result["run_id"], args.verbose)
        
        print("\n" + "=" * 60)
        print("DATA QUALITY CHECK COMPLETE")
        print("=" * 60)
        
        # Exit with appropriate code
        if "FAILED" in result["result"]:
            print("\nData quality checks FAILED. Review the issues above.")
            sys.exit(1)
        elif "WARNING" in result["result"]:
            print("\nData quality checks passed with WARNINGS.")
            sys.exit(0)
        else:
            print("\nAll data quality checks PASSED.")
            sys.exit(0)
            
    finally:
        conn.close()


if __name__ == "__main__":
    main()
