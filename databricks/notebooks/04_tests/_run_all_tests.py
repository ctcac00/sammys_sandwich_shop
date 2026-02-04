# Databricks notebook source
# MAGIC %md
# MAGIC # Run All Tests
# MAGIC Orchestrates running all data quality tests.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Notebooks

# COMMAND ----------

test_notebooks = [
    "test_row_counts",
    "test_data_quality",
    "test_aggregates",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Tests

# COMMAND ----------

import time

start_time = time.time()
results = []

for notebook in test_notebooks:
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
print(f"Test Suite Complete")
print(f"{'='*50}")
print(f"Total test notebooks: {len(results)}")
print(f"Successful: {success_count}")
print(f"Failed: {failed_count}")
print(f"Total time: {total_time}s")

if failed_count > 0:
    print(f"\nFailed test notebooks:")
    for r in results:
        if r["status"] != "SUCCESS":
            print(f"  - {r['notebook']}: {r['status']}")
    raise Exception(f"{failed_count} test notebooks failed")
else:
    print("\n✓ All test suites passed!")
