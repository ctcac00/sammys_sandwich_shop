# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Location Employee Performance
# MAGIC Employee performance metrics by location.

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {gold_table('rpt_location_employee_performance')} AS
SELECT 
    dl.location_name,
    de.full_name as employee_name,
    de.job_title,
    de.tenure_months,
    COUNT(DISTINCT fs.order_id) as orders_processed,
    SUM(fs.total_amount) as total_revenue,
    ROUND(AVG(fs.total_amount), 2) as avg_order_value,
    SUM(fs.tip_amount) as total_tips,
    ROUND(AVG(fs.tip_amount), 2) as avg_tip
FROM {gold_table('fct_sales')} fs
JOIN {gold_table('dim_employee')} de ON fs.employee_sk = de.employee_sk
JOIN {gold_table('dim_location')} dl ON fs.location_sk = dl.location_sk
WHERE de.employee_id != '{UNKNOWN_EMPLOYEE_ID}'
GROUP BY dl.location_name, de.full_name, de.job_title, de.tenure_months
ORDER BY dl.location_name, total_revenue DESC
""")

print(f"✓ Created view {gold_table('rpt_location_employee_performance')}")

# COMMAND ----------

display(spark.table(gold_table('rpt_location_employee_performance')).limit(20))
