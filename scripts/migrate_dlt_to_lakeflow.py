#!/usr/bin/env python3
"""Migrates Databricks DLT Python API to Lakeflow Spark Declarative Pipelines."""
from pathlib import Path

REPLACEMENTS = [
    # Import — must come first
    ("import dlt\n", "from pyspark import pipelines as dp\n"),
    # Decorators (order matters: more specific before less specific)
    ("@dlt.expect_or_drop(", "@dp.expect_or_drop("),
    ("@dlt.expect_or_fail(", "@dp.expect_or_fail("),
    ("@dlt.expect(", "@dp.expect("),
    ("@dlt.append_flow(", "@dp.append_flow("),
    ("@dlt.view(", "@dp.temporary_view("),
    # @dlt.table → @dp.materialized_view (all tables in this project are batch)
    ("@dlt.table(", "@dp.materialized_view("),
    # Functions (order matters: read_stream before read)
    ("dlt.read_stream(", "spark.readStream.table("),
    ("dlt.read(", "spark.read.table("),
    ("dlt.apply_changes(", "dp.create_auto_cdc_flow("),
    ("dlt.create_sink(", "dp.create_sink("),
]

root = Path("databricks/dlt")
changed_files = []

for py_file in root.rglob("*.py"):
    original = py_file.read_text(encoding="utf-8")
    updated = original
    for old, new in REPLACEMENTS:
        updated = updated.replace(old, new)
    if updated != original:
        py_file.write_text(updated, encoding="utf-8")
        changed_files.append(str(py_file))

print(f"Updated {len(changed_files)} files:")
for f in sorted(changed_files):
    print(f"  {f}")
