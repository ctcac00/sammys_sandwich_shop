# Databricks notebook source
# MAGIC %md
# MAGIC # Sammy's Sandwich Shop - Full Pipeline
# MAGIC Main orchestration notebook that runs the complete ETL pipeline.
# MAGIC 
# MAGIC ## Pipeline Stages
# MAGIC 1. **Setup**: Create catalog and schemas
# MAGIC 2. **Bronze**: Load CSV data into raw tables
# MAGIC 3. **Silver**: Transform and enrich data
# MAGIC 4. **Gold**: Build dimensional model
# MAGIC 5. **Tests**: Run data quality tests

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Set to True to skip tests (useful for faster iteration)
SKIP_TESTS = False

# Timeout settings (in seconds)
SETUP_TIMEOUT = 300
BRONZE_TIMEOUT = 600
SILVER_TIMEOUT = 900
GOLD_TIMEOUT = 1800
TESTS_TIMEOUT = 600

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Execution

# COMMAND ----------

import time

pipeline_start = time.time()
stage_results = []

def run_stage(stage_name, notebook_path, timeout):
    """Run a pipeline stage and record results."""
    print(f"\n{'='*60}")
    print(f"Stage: {stage_name}")
    print(f"{'='*60}")
    
    stage_start = time.time()
    try:
        dbutils.notebook.run(notebook_path, timeout)
        status = "SUCCESS"
    except Exception as e:
        status = f"FAILED: {str(e)}"
    
    elapsed = round(time.time() - stage_start, 2)
    stage_results.append({
        "stage": stage_name,
        "status": status,
        "elapsed_seconds": elapsed
    })
    
    print(f"\n{stage_name}: {status} ({elapsed}s)")
    
    if status != "SUCCESS":
        raise Exception(f"Stage '{stage_name}' failed: {status}")
    
    return status

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stage 1: Setup

# COMMAND ----------

run_stage("Setup", "../00_setup/setup_catalog", SETUP_TIMEOUT)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stage 2: Bronze Layer

# COMMAND ----------

run_stage("Bronze", "../01_bronze/_run_all_bronze", BRONZE_TIMEOUT)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stage 3: Silver Layer

# COMMAND ----------

run_stage("Silver", "../02_silver/_run_all_silver", SILVER_TIMEOUT)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stage 4: Gold Layer

# COMMAND ----------

run_stage("Gold", "../03_gold/_run_all_gold", GOLD_TIMEOUT)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stage 5: Tests

# COMMAND ----------

if not SKIP_TESTS:
    run_stage("Tests", "../04_tests/_run_all_tests", TESTS_TIMEOUT)
else:
    print("\n" + "="*60)
    print("Stage: Tests (SKIPPED)")
    print("="*60)
    stage_results.append({
        "stage": "Tests",
        "status": "SKIPPED",
        "elapsed_seconds": 0
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Summary

# COMMAND ----------

total_time = round(time.time() - pipeline_start, 2)
success_count = sum(1 for r in stage_results if r["status"] == "SUCCESS")
skipped_count = sum(1 for r in stage_results if r["status"] == "SKIPPED")
failed_count = len(stage_results) - success_count - skipped_count

print(f"\n{'='*60}")
print(f"Pipeline Complete")
print(f"{'='*60}")
print(f"\nStage Results:")
for r in stage_results:
    status_icon = "✓" if r["status"] == "SUCCESS" else "○" if r["status"] == "SKIPPED" else "✗"
    print(f"  {status_icon} {r['stage']}: {r['status']} ({r['elapsed_seconds']}s)")

print(f"\nSummary:")
print(f"  Total Stages: {len(stage_results)}")
print(f"  Successful: {success_count}")
print(f"  Skipped: {skipped_count}")
print(f"  Failed: {failed_count}")
print(f"  Total Time: {total_time}s ({total_time/60:.1f} minutes)")

if failed_count > 0:
    raise Exception(f"Pipeline failed with {failed_count} failed stages")
else:
    print(f"\n✓ Pipeline completed successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Lineage Overview
# MAGIC 
# MAGIC ```
# MAGIC CSV Files (data/)
# MAGIC     │
# MAGIC     ▼
# MAGIC ┌──────────────────────────────────────────────────────────────┐
# MAGIC │ BRONZE (10 tables)                                          │
# MAGIC │ customers, employees, locations, menu_items, ingredients,   │
# MAGIC │ menu_item_ingredients, orders, order_items, suppliers,      │
# MAGIC │ inventory                                                   │
# MAGIC └──────────────────────────────────────────────────────────────┘
# MAGIC     │
# MAGIC     ▼
# MAGIC ┌──────────────────────────────────────────────────────────────┐
# MAGIC │ SILVER (20 tables)                                          │
# MAGIC │ ├── Staging: stg_* (10 tables)                              │
# MAGIC │ └── Enriched: int_* (10 tables)                             │
# MAGIC └──────────────────────────────────────────────────────────────┘
# MAGIC     │
# MAGIC     ▼
# MAGIC ┌──────────────────────────────────────────────────────────────┐
# MAGIC │ GOLD (41 tables/views)                                      │
# MAGIC │ ├── Dimensions: dim_* (9 tables)                            │
# MAGIC │ ├── Facts: fct_* (6 tables)                                 │
# MAGIC │ └── Reports: rpt_* (26 views)                               │
# MAGIC └──────────────────────────────────────────────────────────────┘
# MAGIC ```
