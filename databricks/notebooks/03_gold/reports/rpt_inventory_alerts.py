# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Inventory Alerts
# MAGIC Identifies inventory items needing attention.

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {gold_table('rpt_inventory_alerts')} AS
SELECT 
    dl.location_name,
    di.ingredient_name,
    di.category as ingredient_category,
    di.supplier_name,
    fis.quantity_on_hand,
    fis.quantity_available,
    fis.reorder_point,
    fis.stock_status,
    fis.days_until_expiration,
    fis.expiration_risk,
    CASE 
        WHEN fis.stock_status IN ('Out of Stock', 'Low Stock') AND fis.expiration_risk IN ('Expired', 'Critical') THEN 'Critical'
        WHEN fis.stock_status IN ('Out of Stock', 'Low Stock') OR fis.expiration_risk IN ('Expired', 'Critical') THEN 'High'
        WHEN fis.expiration_risk = 'Warning' THEN 'Medium'
        ELSE 'Low'
    END as alert_priority
FROM {gold_table('fct_inventory_snapshot')} fis
JOIN {gold_table('dim_location')} dl ON fis.location_sk = dl.location_sk
JOIN {gold_table('dim_ingredient')} di ON fis.ingredient_sk = di.ingredient_sk
WHERE fis.needs_reorder = true 
   OR fis.stock_status IN ('Out of Stock', 'Low Stock')
   OR fis.expiration_risk IN ('Expired', 'Critical', 'Warning')
ORDER BY alert_priority, dl.location_name, di.ingredient_name
""")

print(f"✓ Created view {gold_table('rpt_inventory_alerts')}")

# COMMAND ----------

display(spark.table(gold_table('rpt_inventory_alerts')).limit(20))
