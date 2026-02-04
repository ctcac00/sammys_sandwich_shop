# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Category Performance
# MAGIC Summary performance by menu category.

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {gold_table('rpt_category_performance')} AS
SELECT 
    dm.category,
    COUNT(DISTINCT dm.item_id) as item_count,
    SUM(fmp.total_quantity_sold) as total_quantity_sold,
    SUM(fmp.total_orders) as total_orders,
    SUM(fmp.total_revenue) as total_revenue,
    SUM(fmp.total_food_cost) as total_food_cost,
    SUM(fmp.total_gross_profit) as total_gross_profit,
    ROUND(SUM(fmp.total_gross_profit) * 100.0 / NULLIF(SUM(fmp.total_revenue), 0), 2) as category_margin_pct,
    ROUND(SUM(fmp.total_revenue) * 100.0 / NULLIF(SUM(SUM(fmp.total_revenue)) OVER(), 0), 2) as revenue_share_pct,
    ROUND(AVG(dm.base_price), 2) as avg_item_price
FROM {gold_table('fct_menu_item_performance')} fmp
JOIN {gold_table('dim_menu_item')} dm ON fmp.menu_item_sk = dm.menu_item_sk
GROUP BY dm.category
ORDER BY total_revenue DESC
""")

print(f"✓ Created view {gold_table('rpt_category_performance')}")

# COMMAND ----------

display(spark.table(gold_table('rpt_category_performance')))
