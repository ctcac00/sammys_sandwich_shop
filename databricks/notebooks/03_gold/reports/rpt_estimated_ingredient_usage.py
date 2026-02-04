# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Estimated Ingredient Usage
# MAGIC Estimates ingredient usage based on sales.

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {gold_table('rpt_estimated_ingredient_usage')} AS
WITH item_sales AS (
    SELECT 
        menu_item_sk,
        SUM(quantity) as total_sold
    FROM {gold_table('fct_sales_line_item')}
    GROUP BY menu_item_sk
)
SELECT 
    di.ingredient_name,
    di.category as ingredient_category,
    di.unit_of_measure,
    di.cost_per_unit,
    dm.item_name,
    ii.quantity_required,
    is_item.total_sold as item_quantity_sold,
    ROUND(ii.quantity_required * is_item.total_sold, 2) as estimated_usage,
    ROUND(ii.quantity_required * is_item.total_sold * di.cost_per_unit, 2) as estimated_cost
FROM {silver_table('int_menu_item_ingredients')} ii
JOIN {gold_table('dim_menu_item')} dm ON ii.item_id = dm.item_id
JOIN {gold_table('dim_ingredient')} di ON ii.ingredient_id = di.ingredient_id
JOIN item_sales is_item ON dm.menu_item_sk = is_item.menu_item_sk
WHERE ii.is_optional = false
ORDER BY estimated_cost DESC
""")

print(f"✓ Created view {gold_table('rpt_estimated_ingredient_usage')}")

# COMMAND ----------

display(spark.table(gold_table('rpt_estimated_ingredient_usage')).limit(20))
