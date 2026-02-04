# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Ingredient Cost Breakdown
# MAGIC Ingredient costs per menu item.

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {gold_table('rpt_ingredient_cost_breakdown')} AS
SELECT 
    mi.item_name,
    mi.category,
    mi.base_price,
    ii.ingredient_name,
    ii.ingredient_category,
    ii.quantity_required,
    ii.unit_of_measure,
    ii.cost_per_unit,
    ii.ingredient_cost,
    ii.is_optional,
    mi.food_cost as total_item_food_cost,
    ROUND(ii.ingredient_cost * 100.0 / NULLIF(mi.food_cost, 0), 2) as pct_of_item_cost
FROM {silver_table('int_menu_item_ingredients')} ii
JOIN {gold_table('dim_menu_item')} mi ON ii.item_id = mi.item_id
ORDER BY mi.item_name, ii.ingredient_cost DESC
""")

print(f"✓ Created view {gold_table('rpt_ingredient_cost_breakdown')}")

# COMMAND ----------

display(spark.table(gold_table('rpt_ingredient_cost_breakdown')).limit(20))
