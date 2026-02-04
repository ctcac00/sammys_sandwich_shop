# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Inventory Value by Location
# MAGIC Total inventory value by location and category.

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {gold_table('rpt_inventory_value_by_location')} AS
SELECT 
    dl.location_name,
    di.category as ingredient_category,
    COUNT(DISTINCT di.ingredient_id) as ingredient_count,
    SUM(fis.quantity_on_hand) as total_quantity,
    SUM(fis.stock_value) as total_value,
    SUM(CASE WHEN fis.needs_reorder THEN 1 ELSE 0 END) as items_needing_reorder
FROM {gold_table('fct_inventory_snapshot')} fis
JOIN {gold_table('dim_location')} dl ON fis.location_sk = dl.location_sk
JOIN {gold_table('dim_ingredient')} di ON fis.ingredient_sk = di.ingredient_sk
GROUP BY dl.location_name, di.category
ORDER BY dl.location_name, total_value DESC
""")

print(f"✓ Created view {gold_table('rpt_inventory_value_by_location')}")

# COMMAND ----------

display(spark.table(gold_table('rpt_inventory_value_by_location')))
