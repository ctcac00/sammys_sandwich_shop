# Databricks notebook source
# MAGIC %md
# MAGIC # Run All Silver Layer Transformations
# MAGIC Orchestrates running all staging and enriched transformations.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Staging Notebooks (Order matters for dependencies)

# COMMAND ----------

staging_notebooks = [
    "staging/stg_customers",
    "staging/stg_employees",
    "staging/stg_locations",
    "staging/stg_menu_items",
    "staging/stg_ingredients",
    "staging/stg_menu_item_ingredients",
    "staging/stg_orders",
    "staging/stg_order_items",
    "staging/stg_suppliers",
    "staging/stg_inventory",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enriched Notebooks (Order matters for dependencies)

# COMMAND ----------

enriched_notebooks = [
    # No dependencies on other enriched
    "enriched/int_customers",
    "enriched/int_employees",
    "enriched/int_ingredients",
    "enriched/int_menu_items",
    "enriched/int_order_items",
    "enriched/int_suppliers",
    "enriched/int_inventory",
    # Has dependency on int_employees
    "enriched/int_locations",
    # Has dependency on int_ingredients
    "enriched/int_menu_item_ingredients",
    # Has dependency on stg_order_items
    "enriched/int_orders",
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
            dbutils.notebook.run(f"{_nb_dir}/{notebook}", timeout_seconds=300)
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
# MAGIC ### Run Staging

# COMMAND ----------

print("=" * 50)
print("Running Staging Notebooks")
print("=" * 50)
staging_results, staging_failed = run_notebooks(staging_notebooks, "Staging Layer")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Enriched

# COMMAND ----------

print("\n" + "=" * 50)
print("Running Enriched Notebooks")
print("=" * 50)
enriched_results, enriched_failed = run_notebooks(enriched_notebooks, "Enriched Layer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

total_failed = staging_failed + enriched_failed
all_results = staging_results + enriched_results

print(f"\n{'='*50}")
print(f"Silver Layer Complete")
print(f"{'='*50}")
print(f"Total notebooks: {len(all_results)}")
print(f"Successful: {len(all_results) - total_failed}")
print(f"Failed: {total_failed}")

if total_failed > 0:
    print(f"\nFailed notebooks:")
    for r in all_results:
        if r["status"] != "SUCCESS":
            print(f"  - {r['notebook']}: {r['status']}")
    raise Exception(f"{total_failed} notebooks failed")
