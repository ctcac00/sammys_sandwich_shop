# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Inventory Alerts
# MAGIC Items needing reorder or expiring soon.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import col, when, lit

# COMMAND ----------

@dp.temporary_view(
    name="rpt_inventory_alerts",
    comment="Items needing reorder or expiring soon"
)
def rpt_inventory_alerts():
    fi = spark.read.table("fct_inventory_snapshot")
    dl = spark.read.table("dim_location").filter(col("is_current") == True)
    di = spark.read.table("dim_ingredient").filter(col("is_current") == True)
    
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
