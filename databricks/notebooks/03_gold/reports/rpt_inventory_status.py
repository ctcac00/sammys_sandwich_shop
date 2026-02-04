# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Inventory Status
# MAGIC Current inventory status by location and ingredient.

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {gold_table('rpt_inventory_status')} AS
SELECT 
    dl.location_name,
    di.ingredient_name,
    di.category as ingredient_category,
    di.storage_type,
    di.supplier_name,
    
    -- Quantities
    fis.quantity_on_hand,
    fis.quantity_reserved,
    fis.quantity_available,
    fis.reorder_point,
    
    -- Values
    fis.stock_value,
    fis.days_of_supply,
    
    -- Status
    fis.stock_status,
    fis.needs_reorder,
    fis.days_until_expiration,
    fis.expiration_risk,
    
    -- Allergen info
    di.is_allergen,
    di.allergen_type
    
FROM {gold_table('fct_inventory_snapshot')} fis
JOIN {gold_table('dim_location')} dl ON fis.location_sk = dl.location_sk
JOIN {gold_table('dim_ingredient')} di ON fis.ingredient_sk = di.ingredient_sk
ORDER BY dl.location_name, fis.stock_status, di.ingredient_name
""")

print(f"✓ Created view {gold_table('rpt_inventory_status')}")

# COMMAND ----------

display(spark.table(gold_table('rpt_inventory_status')).limit(20))
