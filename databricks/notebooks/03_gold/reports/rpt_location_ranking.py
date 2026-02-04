# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Location Ranking
# MAGIC Ranks locations by various metrics.

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {gold_table('rpt_location_ranking')} AS
WITH location_totals AS (
    SELECT 
        dl.location_name,
        dl.region,
        SUM(fds.total_orders) as total_orders,
        SUM(fds.total_revenue) as total_revenue,
        SUM(fds.total_items_sold) as total_items_sold,
        ROUND(AVG(fds.avg_order_value), 2) as avg_order_value
    FROM {gold_table('fct_daily_summary')} fds
    JOIN {gold_table('dim_location')} dl ON fds.location_sk = dl.location_sk
    GROUP BY dl.location_name, dl.region
)
SELECT 
    location_name,
    region,
    total_orders,
    total_revenue,
    total_items_sold,
    avg_order_value,
    RANK() OVER (ORDER BY total_revenue DESC) as revenue_rank,
    RANK() OVER (ORDER BY total_orders DESC) as orders_rank,
    RANK() OVER (ORDER BY avg_order_value DESC) as aov_rank
FROM location_totals
ORDER BY revenue_rank
""")

print(f"✓ Created view {gold_table('rpt_location_ranking')}")

# COMMAND ----------

display(spark.table(gold_table('rpt_location_ranking')))
