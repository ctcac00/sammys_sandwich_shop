# Databricks notebook source
# MAGIC %md
# MAGIC # Run All Bronze Layer Loads
# MAGIC Orchestrates loading all CSV files into bronze Delta tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load All Tables

# COMMAND ----------

bronze_notebooks = [
    "load_customers",
    "load_employees",
    "load_locations",
    "load_menu_items",
    "load_ingredients",
    "load_menu_item_ingredients",
    "load_orders",
    "load_order_items",
    "load_suppliers",
    "load_inventory",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Notebooks

# COMMAND ----------

import time

start_time = time.time()
results = []

for notebook in bronze_notebooks:
    notebook_start = time.time()
    try:
        dbutils.notebook.run(f"./{notebook}", timeout_seconds=300)
        status = "SUCCESS"
    except Exception as e:
        status = f"FAILED: {str(e)}"
    
    elapsed = round(time.time() - notebook_start, 2)
    results.append({"notebook": notebook, "status": status, "elapsed_seconds": elapsed})
    print(f"{notebook}: {status} ({elapsed}s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

total_time = round(time.time() - start_time, 2)
success_count = sum(1 for r in results if r["status"] == "SUCCESS")
failed_count = len(results) - success_count

print(f"\n{'='*50}")
print(f"Bronze Layer Load Complete")
print(f"{'='*50}")
print(f"Total notebooks: {len(results)}")
print(f"Successful: {success_count}")
print(f"Failed: {failed_count}")
print(f"Total time: {total_time}s")

if failed_count > 0:
    print(f"\nFailed notebooks:")
    for r in results:
        if r["status"] != "SUCCESS":
            print(f"  - {r['notebook']}: {r['status']}")
    raise Exception(f"{failed_count} notebooks failed")
