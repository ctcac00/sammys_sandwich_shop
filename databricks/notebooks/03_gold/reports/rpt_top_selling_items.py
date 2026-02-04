# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Top Selling Items
# MAGIC Analyzes menu item performance with Pareto analysis.

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {gold_table('rpt_top_selling_items')} AS
SELECT 
    dm.item_name,
    dm.category,
    dm.subcategory,
    dm.base_price,
    dm.price_tier,
    dm.calories,
    dm.calorie_category,
    dm.food_cost,
    dm.gross_margin_pct as menu_gross_margin_pct,
    
    -- Performance metrics
    fmp.total_quantity_sold,
    fmp.total_orders,
    fmp.total_revenue,
    fmp.total_food_cost,
    fmp.total_gross_profit,
    fmp.avg_daily_quantity,
    fmp.gross_margin_pct as actual_gross_margin_pct,
    
    -- Rankings
    fmp.revenue_rank,
    fmp.quantity_rank,
    fmp.profit_rank,
    
    -- Revenue share
    ROUND(fmp.total_revenue * 100.0 / NULLIF(SUM(fmp.total_revenue) OVER(), 0), 2) as revenue_share_pct,
    
    -- Cumulative revenue (for Pareto analysis)
    SUM(fmp.total_revenue) OVER (ORDER BY fmp.total_revenue DESC) as cumulative_revenue,
    ROUND(SUM(fmp.total_revenue) OVER (ORDER BY fmp.total_revenue DESC) * 100.0 / 
          NULLIF(SUM(fmp.total_revenue) OVER(), 0), 2) as cumulative_revenue_pct
          
FROM {gold_table('fct_menu_item_performance')} fmp
JOIN {gold_table('dim_menu_item')} dm ON fmp.menu_item_sk = dm.menu_item_sk
ORDER BY fmp.total_revenue DESC
""")

print(f"✓ Created view {gold_table('rpt_top_selling_items')}")

# COMMAND ----------

display(spark.table(gold_table('rpt_top_selling_items')).limit(20))
