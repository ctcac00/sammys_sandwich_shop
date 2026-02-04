# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Supplier Spend
# MAGIC Total inventory value by supplier.

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {gold_table('rpt_supplier_spend')} AS
SELECT 
    di.supplier_name,
    di.supplier_id,
    COUNT(DISTINCT di.ingredient_id) as ingredient_count,
    SUM(fis.stock_value) as total_inventory_value,
    ROUND(AVG(di.cost_per_unit), 2) as avg_cost_per_unit,
    SUM(CASE WHEN fis.needs_reorder THEN 1 ELSE 0 END) as items_needing_reorder
FROM {gold_table('fct_inventory_snapshot')} fis
JOIN {gold_table('dim_ingredient')} di ON fis.ingredient_sk = di.ingredient_sk
WHERE di.supplier_name IS NOT NULL
GROUP BY di.supplier_name, di.supplier_id
ORDER BY total_inventory_value DESC
""")

print(f"✓ Created view {gold_table('rpt_supplier_spend')}")

# COMMAND ----------

display(spark.table(gold_table('rpt_supplier_spend')))
