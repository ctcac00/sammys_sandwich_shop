# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Top Items by Category
# MAGIC Top 5 items per category by revenue.

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {gold_table('rpt_top_items_by_category')} AS
WITH ranked_items AS (
    SELECT 
        dm.category,
        dm.item_name,
        dm.base_price,
        fmp.total_quantity_sold,
        fmp.total_revenue,
        fmp.total_gross_profit,
        ROW_NUMBER() OVER (PARTITION BY dm.category ORDER BY fmp.total_revenue DESC) as category_rank
    FROM {gold_table('fct_menu_item_performance')} fmp
    JOIN {gold_table('dim_menu_item')} dm ON fmp.menu_item_sk = dm.menu_item_sk
)
SELECT * FROM ranked_items
WHERE category_rank <= 5
ORDER BY category, category_rank
""")

print(f"✓ Created view {gold_table('rpt_top_items_by_category')}")

# COMMAND ----------

display(spark.table(gold_table('rpt_top_items_by_category')))
