# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Customer Cohort Analysis
# MAGIC Analyzes customers by signup cohort.

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {gold_table('rpt_customer_cohort_analysis')} AS
WITH customer_cohorts AS (
    SELECT
        dc.customer_sk,
        YEAR(dc.signup_date) as signup_year,
        MONTH(dc.signup_date) as signup_month,
        DATE_TRUNC('month', dc.signup_date) as cohort_month
    FROM {gold_table('dim_customer')} dc
    WHERE dc.is_current = true 
      AND dc.customer_id != '{UNKNOWN_CUSTOMER_ID}'
      AND dc.signup_date IS NOT NULL
)
SELECT 
    cc.cohort_month,
    COUNT(DISTINCT cc.customer_sk) as cohort_size,
    COUNT(DISTINCT fca.customer_sk) as active_customers,
    ROUND(COUNT(DISTINCT fca.customer_sk) * 100.0 / NULLIF(COUNT(DISTINCT cc.customer_sk), 0), 2) as retention_rate,
    SUM(fca.total_orders) as total_orders,
    SUM(fca.total_spend) as total_spend,
    ROUND(SUM(fca.total_spend) / NULLIF(COUNT(DISTINCT fca.customer_sk), 0), 2) as avg_ltv
FROM customer_cohorts cc
LEFT JOIN {gold_table('fct_customer_activity')} fca ON cc.customer_sk = fca.customer_sk
GROUP BY cc.cohort_month
ORDER BY cc.cohort_month DESC
""")

print(f"✓ Created view {gold_table('rpt_customer_cohort_analysis')}")

# COMMAND ----------

display(spark.table(gold_table('rpt_customer_cohort_analysis')).limit(20))
