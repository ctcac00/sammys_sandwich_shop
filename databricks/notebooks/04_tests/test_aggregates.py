# Databricks notebook source
# MAGIC %md
# MAGIC # Test: Aggregate Consistency
# MAGIC Validates cross-table aggregate consistency.
# MAGIC 
# MAGIC Mirrors: `dbt/tests/assert_daily_summary_order_count_matches_fact_sales.sql`

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

from pyspark.sql.functions import col, sum as spark_sum, count, abs as spark_abs

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test: Daily Summary Order Count vs Fact Sales

# COMMAND ----------

print("=" * 60)
print("Aggregate Consistency Tests")
print("=" * 60)

# Test: fct_daily_summary total_orders should match fct_sales count
daily_summary_orders = spark.table(gold_table("fct_daily_summary")).agg(
    spark_sum("total_orders").alias("total")
).collect()[0]["total"]

fact_sales_orders = spark.table(gold_table("fct_sales")).count()

diff = abs(daily_summary_orders - fact_sales_orders) if daily_summary_orders else 0
diff_pct = (diff / fact_sales_orders * 100) if fact_sales_orders > 0 else 0

# Allow 5% tolerance (matching dbt test)
passed = diff_pct <= 5
message = f"Daily Summary: {daily_summary_orders}, Fact Sales: {fact_sales_orders}, Diff: {diff} ({diff_pct:.2f}%)"

result = TestResult("daily_summary_vs_fact_sales_order_count", passed, message)
test_results.append(result)
print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test: Line Item Revenue vs Sales Revenue

# COMMAND ----------

# Test: sum of line_total in fct_sales_line_item should match sum of subtotal in fct_sales
line_item_revenue = spark.table(gold_table("fct_sales_line_item")).agg(
    spark_sum("line_total").alias("total")
).collect()[0]["total"]

sales_revenue = spark.table(gold_table("fct_sales")).agg(
    spark_sum("subtotal").alias("total")
).collect()[0]["total"]

diff = abs(line_item_revenue - sales_revenue) if (line_item_revenue and sales_revenue) else 0
diff_pct = (diff / sales_revenue * 100) if sales_revenue and sales_revenue > 0 else 0

passed = diff_pct <= 5
message = f"Line Items: {line_item_revenue:.2f}, Sales: {sales_revenue:.2f}, Diff: {diff:.2f} ({diff_pct:.2f}%)"

result = TestResult("line_item_vs_sales_revenue", passed, message)
test_results.append(result)
print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test: Customer Activity Total Spend vs Sales

# COMMAND ----------

# Test: sum of total_spend in fct_customer_activity should be close to non-guest sales
customer_activity_spend = spark.table(gold_table("fct_customer_activity")).agg(
    spark_sum("total_spend").alias("total")
).collect()[0]["total"]

non_guest_sales = spark.table(gold_table("fct_sales")).filter(
    col("is_guest_order") == False
).agg(
    spark_sum("total_amount").alias("total")
).collect()[0]["total"]

diff = abs(customer_activity_spend - non_guest_sales) if (customer_activity_spend and non_guest_sales) else 0
diff_pct = (diff / non_guest_sales * 100) if non_guest_sales and non_guest_sales > 0 else 0

passed = diff_pct <= 5
message = f"Customer Activity: {customer_activity_spend:.2f}, Non-Guest Sales: {non_guest_sales:.2f}, Diff: {diff:.2f} ({diff_pct:.2f}%)"

result = TestResult("customer_activity_vs_non_guest_sales", passed, message)
test_results.append(result)
print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test: Menu Item Performance Total vs Line Items

# COMMAND ----------

# Test: total_revenue in fct_menu_item_performance should match line items
menu_perf_revenue = spark.table(gold_table("fct_menu_item_performance")).agg(
    spark_sum("total_revenue").alias("total")
).collect()[0]["total"]

line_items_revenue = spark.table(gold_table("fct_sales_line_item")).agg(
    spark_sum("line_total").alias("total")
).collect()[0]["total"]

diff = abs(menu_perf_revenue - line_items_revenue) if (menu_perf_revenue and line_items_revenue) else 0
diff_pct = (diff / line_items_revenue * 100) if line_items_revenue and line_items_revenue > 0 else 0

passed = diff_pct <= 5
message = f"Menu Performance: {menu_perf_revenue:.2f}, Line Items: {line_items_revenue:.2f}, Diff: {diff:.2f} ({diff_pct:.2f}%)"

result = TestResult("menu_performance_vs_line_items_revenue", passed, message)
test_results.append(result)
print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

passed_count = sum(1 for r in test_results if r.passed)
failed_count = len(test_results) - passed_count

print(f"\n{'='*60}")
print(f"Aggregate Consistency Tests Summary")
print(f"{'='*60}")
print(f"Total Tests: {len(test_results)}")
print(f"Passed: {passed_count}")
print(f"Failed: {failed_count}")

if failed_count > 0:
    print(f"\nFailed Tests:")
    for r in test_results:
        if not r.passed:
            print(f"  {r}")
    raise Exception(f"{failed_count} aggregate consistency tests failed")
else:
    print("\n✓ All aggregate consistency tests passed!")
