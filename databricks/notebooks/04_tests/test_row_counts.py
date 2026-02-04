# Databricks notebook source
# MAGIC %md
# MAGIC # Test: Row Counts
# MAGIC Validates that staging row counts match source bronze tables.
# MAGIC 
# MAGIC Mirrors: `dbt/tests/assert_stg_customers_row_count_matches_source.sql`

# COMMAND ----------

# MAGIC %run ../config

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

def assert_row_count_match(bronze_name, silver_name, tolerance_pct=0):
    """Compare row counts between bronze and silver tables."""
    bronze_count = spark.table(bronze_table(bronze_name)).count()
    silver_count = spark.table(silver_table(silver_name)).count()
    
    diff = abs(bronze_count - silver_count)
    diff_pct = (diff / bronze_count * 100) if bronze_count > 0 else 0
    
    passed = diff_pct <= tolerance_pct
    message = f"Bronze: {bronze_count}, Silver: {silver_count}, Diff: {diff} ({diff_pct:.2f}%)"
    
    result = TestResult(f"{silver_name}_row_count", passed, message)
    test_results.append(result)
    print(result)
    return passed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Row Count Tests

# COMMAND ----------

print("=" * 60)
print("Row Count Tests: Bronze vs Silver Staging")
print("=" * 60)

# Test all staging tables
test_tables = [
    ("customers", "stg_customers"),
    ("employees", "stg_employees"),
    ("locations", "stg_locations"),
    ("menu_items", "stg_menu_items"),
    ("ingredients", "stg_ingredients"),
    ("menu_item_ingredients", "stg_menu_item_ingredients"),
    ("orders", "stg_orders"),
    ("order_items", "stg_order_items"),
    ("suppliers", "stg_suppliers"),
    ("inventory", "stg_inventory"),
]

for bronze_name, silver_name in test_tables:
    assert_row_count_match(bronze_name, silver_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

passed_count = sum(1 for r in test_results if r.passed)
failed_count = len(test_results) - passed_count

print(f"\n{'='*60}")
print(f"Row Count Tests Summary")
print(f"{'='*60}")
print(f"Total Tests: {len(test_results)}")
print(f"Passed: {passed_count}")
print(f"Failed: {failed_count}")

if failed_count > 0:
    print(f"\nFailed Tests:")
    for r in test_results:
        if not r.passed:
            print(f"  {r}")
    raise Exception(f"{failed_count} row count tests failed")
else:
    print("\n✓ All row count tests passed!")
