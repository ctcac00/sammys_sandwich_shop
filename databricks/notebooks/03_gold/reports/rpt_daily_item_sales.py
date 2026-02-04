# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Daily Item Sales
# MAGIC Daily breakdown of item sales.

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {gold_table('rpt_daily_item_sales')} AS
SELECT 
    dd.full_date as order_date,
    dd.day_name,
    dd.is_weekend,
    dm.item_name,
    dm.category,
    SUM(fsl.quantity) as quantity_sold,
    SUM(fsl.line_total) as revenue,
    SUM(fsl.food_cost) as food_cost,
    SUM(fsl.gross_profit) as gross_profit,
    COUNT(DISTINCT fsl.order_id) as order_count
FROM {gold_table('fct_sales_line_item')} fsl
JOIN {gold_table('dim_date')} dd ON fsl.date_key = dd.date_key
JOIN {gold_table('dim_menu_item')} dm ON fsl.menu_item_sk = dm.menu_item_sk
GROUP BY dd.full_date, dd.day_name, dd.is_weekend, dm.item_name, dm.category
ORDER BY dd.full_date DESC, revenue DESC
""")

print(f"✓ Created view {gold_table('rpt_daily_item_sales')}")

# COMMAND ----------

display(spark.table(gold_table('rpt_daily_item_sales')).limit(20))
