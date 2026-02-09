# Databricks notebook source
# MAGIC %md
# MAGIC # Run All Gold Layer Transformations
# MAGIC Orchestrates running all dimensions, facts, and reports.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension Notebooks (Order matters for dependencies)

# COMMAND ----------

dimension_notebooks = [
    # No dependencies
    "dimensions/dim_date",
    "dimensions/dim_time",
    "dimensions/dim_payment_method",
    "dimensions/dim_order_type",
    # Has dependency on int_employees
    "dimensions/dim_employee",
    # Has dependency on int_locations and int_employees
    "dimensions/dim_location",
    # Has dependency on int_customers
    "dimensions/dim_customer",
    # Has dependency on int_ingredients and int_suppliers
    "dimensions/dim_ingredient",
    # Has dependency on int_menu_items and int_menu_item_ingredients
    "dimensions/dim_menu_item",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Notebooks (Order matters for dependencies)

# COMMAND ----------

fact_notebooks = [
    # Has dependencies on dimensions
    "facts/fct_sales",
    "facts/fct_sales_line_item",
    "facts/fct_daily_summary",
    "facts/fct_inventory_snapshot",
    # Has dependencies on fct_sales and fct_sales_line_item
    "facts/fct_customer_activity",
    "facts/fct_menu_item_performance",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Report Notebooks

# COMMAND ----------

report_notebooks = [
    # Sales reports
    "reports/rpt_daily_sales_summary",
    "reports/rpt_weekly_sales_summary",
    "reports/rpt_company_daily_totals",
    # Customer reports
    "reports/rpt_customer_overview",
    "reports/rpt_rfm_segment_summary",
    "reports/rpt_loyalty_tier_analysis",
    "reports/rpt_top_customers",
    "reports/rpt_customers_at_risk",
    "reports/rpt_customer_cohort_analysis",
    # Menu reports
    "reports/rpt_top_selling_items",
    "reports/rpt_top_items_by_category",
    "reports/rpt_category_performance",
    "reports/rpt_menu_item_profitability",
    "reports/rpt_daily_item_sales",
    # Location reports
    "reports/rpt_location_performance",
    "reports/rpt_location_ranking",
    "reports/rpt_location_daily_comparison",
    "reports/rpt_location_peak_hours",
    "reports/rpt_location_employee_performance",
    "reports/rpt_drive_thru_analysis",
    # Inventory reports
    "reports/rpt_inventory_status",
    "reports/rpt_inventory_alerts",
    "reports/rpt_inventory_value_by_location",
    "reports/rpt_ingredient_cost_breakdown",
    "reports/rpt_estimated_ingredient_usage",
    "reports/rpt_supplier_spend",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute All Notebooks

# COMMAND ----------

import time

# Resolve absolute path for the current notebook's directory.
# This is required because dbutils.notebook.run() with relative paths
# does not work reliably in Databricks Repos.
_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_nb_dir = "/".join(_nb_path.split("/")[:-1])

def run_notebooks(notebooks, layer_name):
    """Run a list of notebooks and return results."""
    start_time = time.time()
    results = []
    
    for notebook in notebooks:
        notebook_start = time.time()
        try:
            dbutils.notebook.run(f"{_nb_dir}/{notebook}", timeout_seconds=600)
            status = "SUCCESS"
        except Exception as e:
            status = f"FAILED: {str(e)}"
        
        elapsed = round(time.time() - notebook_start, 2)
        results.append({"notebook": notebook, "status": status, "elapsed_seconds": elapsed})
        print(f"{notebook}: {status} ({elapsed}s)")
    
    total_time = round(time.time() - start_time, 2)
    success_count = sum(1 for r in results if r["status"] == "SUCCESS")
    failed_count = len(results) - success_count
    
    print(f"\n{layer_name} Complete: {success_count}/{len(results)} successful in {total_time}s")
    
    return results, failed_count

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Dimensions

# COMMAND ----------

print("=" * 50)
print("Running Dimension Notebooks")
print("=" * 50)
dim_results, dim_failed = run_notebooks(dimension_notebooks, "Dimensions")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Facts

# COMMAND ----------

print("\n" + "=" * 50)
print("Running Fact Notebooks")
print("=" * 50)
fact_results, fact_failed = run_notebooks(fact_notebooks, "Facts")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Reports

# COMMAND ----------

print("\n" + "=" * 50)
print("Running Report Notebooks")
print("=" * 50)
report_results, report_failed = run_notebooks(report_notebooks, "Reports")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

total_failed = dim_failed + fact_failed + report_failed
all_results = dim_results + fact_results + report_results

print(f"\n{'='*50}")
print(f"Gold Layer Complete")
print(f"{'='*50}")
print(f"Dimensions: {len(dim_results) - dim_failed}/{len(dim_results)}")
print(f"Facts: {len(fact_results) - fact_failed}/{len(fact_results)}")
print(f"Reports: {len(report_results) - report_failed}/{len(report_results)}")
print(f"Total: {len(all_results) - total_failed}/{len(all_results)}")
print(f"Failed: {total_failed}")

if total_failed > 0:
    print(f"\nFailed notebooks:")
    for r in all_results:
        if r["status"] != "SUCCESS":
            print(f"  - {r['notebook']}: {r['status']}")
    raise Exception(f"{total_failed} notebooks failed")
