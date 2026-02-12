# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Inventory Alerts
# MAGIC Items needing reorder or expiring soon.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, when, lit

# COMMAND ----------

@dlt.view(
    name="rpt_inventory_alerts",
    comment="Items needing reorder or expiring soon"
)
def rpt_inventory_alerts():
    fi = dlt.read("fct_inventory_snapshot")
    dl = dlt.read("dim_location").filter(col("is_current") == True)
    di = dlt.read("dim_ingredient").filter(col("is_current") == True)
    
    return (
        fi
        .filter(col("needs_reorder") | col("expiring_soon"))
        .join(dl, fi.location_sk == dl.location_sk)
        .join(di, fi.ingredient_sk == di.ingredient_sk)
        .select(
            dl.location_name,
            dl.region,
            di.ingredient_name,
            di.supplier_name,
            fi.quantity_on_hand,
            fi.reorder_level,
            fi.reorder_quantity,
            fi.needs_reorder,
            fi.days_until_expiration,
            fi.expiring_soon,
            when(col("needs_reorder") & col("expiring_soon"), "Critical")
                .when(col("expiring_soon"), "Expiring")
                .otherwise("Reorder").alias("alert_type"),
            when(col("needs_reorder") & col("expiring_soon"), 1)
                .when(col("expiring_soon"), 2)
                .otherwise(3).alias("alert_priority")
        )
        .orderBy("alert_priority", "location_name", "ingredient_name")
    )
