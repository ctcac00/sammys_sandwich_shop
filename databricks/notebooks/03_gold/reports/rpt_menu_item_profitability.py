# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Menu Item Profitability
# MAGIC Quadrant analysis: Stars, Workhorses, Puzzles, Dogs.

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {gold_table('rpt_menu_item_profitability')} AS
WITH median_values AS (
    SELECT 
        PERCENTILE_APPROX(fmp.total_quantity_sold, 0.5) as median_quantity,
        PERCENTILE_APPROX(fmp.gross_margin_pct, 0.5) as median_margin
    FROM {gold_table('fct_menu_item_performance')} fmp
    WHERE fmp.total_quantity_sold > 0
)
SELECT 
    dm.item_name,
    dm.category,
    dm.base_price,
    dm.food_cost,
    fmp.total_quantity_sold,
    fmp.total_revenue,
    fmp.total_gross_profit,
    fmp.gross_margin_pct,
    mv.median_quantity,
    mv.median_margin,
    CASE
        WHEN fmp.total_quantity_sold >= mv.median_quantity AND fmp.gross_margin_pct >= mv.median_margin THEN 'Stars'
        WHEN fmp.total_quantity_sold >= mv.median_quantity AND fmp.gross_margin_pct < mv.median_margin THEN 'Workhorses'
        WHEN fmp.total_quantity_sold < mv.median_quantity AND fmp.gross_margin_pct >= mv.median_margin THEN 'Puzzles'
        ELSE 'Dogs'
    END as profitability_quadrant
FROM {gold_table('fct_menu_item_performance')} fmp
JOIN {gold_table('dim_menu_item')} dm ON fmp.menu_item_sk = dm.menu_item_sk
CROSS JOIN median_values mv
ORDER BY profitability_quadrant, fmp.total_revenue DESC
""")

print(f"✓ Created view {gold_table('rpt_menu_item_profitability')}")

# COMMAND ----------

display(spark.table(gold_table('rpt_menu_item_profitability')).limit(20))
