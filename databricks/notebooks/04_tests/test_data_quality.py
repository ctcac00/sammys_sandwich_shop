# Databricks notebook source
# MAGIC %md
# MAGIC # Test: Data Quality
# MAGIC Validates data quality constraints (unique, not null, value ranges).
# MAGIC 
# MAGIC Mirrors dbt schema tests and dbt_expectations tests.

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

from pyspark.sql.functions import col, count, sum as spark_sum, when

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Framework

# COMMAND ----------

class TestResult:
    def __init__(self, test_name, passed, message):
        self.test_name = test_name
        self.passed = passed
        self.message = message
    
    def __repr__(self):
        status = "✓ PASS" if self.passed else "✗ FAIL"
        return f"{status}: {self.test_name} - {self.message}"

test_results = []

def test_unique(table_name, column_name, schema="silver"):
    """Test that column values are unique."""
    if schema == "silver":
        full_table = silver_table(table_name)
    elif schema == "gold":
        full_table = gold_table(table_name)
    else:
        full_table = bronze_table(table_name)
    
    df = spark.table(full_table)
    total_count = df.count()
    distinct_count = df.select(column_name).distinct().count()
    
    passed = total_count == distinct_count
    message = f"Total: {total_count}, Distinct: {distinct_count}"
    
    result = TestResult(f"{table_name}.{column_name}_unique", passed, message)
    test_results.append(result)
    print(result)
    return passed

def test_not_null(table_name, column_name, schema="silver"):
    """Test that column has no null values."""
    if schema == "silver":
        full_table = silver_table(table_name)
    elif schema == "gold":
        full_table = gold_table(table_name)
    else:
        full_table = bronze_table(table_name)
    
    df = spark.table(full_table)
    null_count = df.filter(col(column_name).isNull()).count()
    
    passed = null_count == 0
    message = f"Null count: {null_count}"
    
    result = TestResult(f"{table_name}.{column_name}_not_null", passed, message)
    test_results.append(result)
    print(result)
    return passed

def test_value_range(table_name, column_name, min_val, max_val, schema="silver", severity="error"):
    """Test that column values are within expected range."""
    if schema == "silver":
        full_table = silver_table(table_name)
    elif schema == "gold":
        full_table = gold_table(table_name)
    else:
        full_table = bronze_table(table_name)
    
    df = spark.table(full_table)
    invalid_count = df.filter(
        (col(column_name) < min_val) | (col(column_name) > max_val)
    ).count()
    
    passed = invalid_count == 0
    message = f"Invalid count: {invalid_count} (expected range: {min_val}-{max_val})"
    
    result = TestResult(f"{table_name}.{column_name}_range", passed, message)
    test_results.append(result)
    
    if severity == "warn" and not passed:
        print(f"⚠ WARN: {result.test_name} - {result.message}")
    else:
        print(result)
    
    return passed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Primary Key Tests (Unique + Not Null)

# COMMAND ----------

print("=" * 60)
print("Primary Key Tests (Unique + Not Null)")
print("=" * 60)

pk_tests = [
    ("stg_customers", "customer_id"),
    ("stg_employees", "employee_id"),
    ("stg_locations", "location_id"),
    ("stg_menu_items", "item_id"),
    ("stg_ingredients", "ingredient_id"),
    ("stg_orders", "order_id"),
    ("stg_order_items", "order_item_id"),
    ("stg_suppliers", "supplier_id"),
    ("stg_inventory", "inventory_id"),
]

for table, column in pk_tests:
    test_unique(table, column)
    test_not_null(table, column)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Value Range Tests

# COMMAND ----------

print("\n" + "=" * 60)
print("Value Range Tests")
print("=" * 60)

# Employee hourly rate (warn severity)
test_value_range("stg_employees", "hourly_rate", 7.25, 100, severity="warn")

# Menu item price (must be positive)
test_value_range("stg_menu_items", "base_price", 0.01, 1000)

# Inventory quantities (non-negative)
test_value_range("stg_inventory", "quantity_on_hand", 0, 100000)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Gold Layer Tests

# COMMAND ----------

print("\n" + "=" * 60)
print("Gold Layer Dimension Tests")
print("=" * 60)

# Dimension surrogate key tests
dim_tests = [
    ("dim_customer", "customer_sk"),
    ("dim_employee", "employee_sk"),
    ("dim_location", "location_sk"),
    ("dim_menu_item", "menu_item_sk"),
    ("dim_ingredient", "ingredient_sk"),
    ("dim_date", "date_key"),
    ("dim_time", "time_key"),
    ("dim_payment_method", "payment_method_sk"),
    ("dim_order_type", "order_type_sk"),
]

for table, column in dim_tests:
    test_unique(table, column, schema="gold")
    test_not_null(table, column, schema="gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

passed_count = sum(1 for r in test_results if r.passed)
failed_count = len(test_results) - passed_count

print(f"\n{'='*60}")
print(f"Data Quality Tests Summary")
print(f"{'='*60}")
print(f"Total Tests: {len(test_results)}")
print(f"Passed: {passed_count}")
print(f"Failed: {failed_count}")

if failed_count > 0:
    print(f"\nFailed Tests:")
    for r in test_results:
        if not r.passed:
            print(f"  {r}")
    raise Exception(f"{failed_count} data quality tests failed")
else:
    print("\n✓ All data quality tests passed!")
