# Databricks notebook source
# MAGIC %md
# MAGIC # Report: Inventory Status
# MAGIC Current inventory status by location and ingredient.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col

# COMMAND ----------

@dlt.view(
    name="rpt_inventory_status",
    comment="Current inventory status by location and ingredient"
)
def rpt_inventory_status():
    fi = dlt.read("fct_inventory_snapshot")
    dl = dlt.read("dim_location").filter(col("is_current") == True)
    di = dlt.read("dim_ingredient").filter(col("is_current") == True)
    
    return (
        fi
        .join(dl, fi.location_sk == dl.location_sk)
        .join(di, fi.ingredient_sk == di.ingredient_sk)
        .select(
            dl.location_name,
            dl.region,
            di.ingredient_name,
            di.category.alias("ingredient_category"),
            fi.quantity_on_hand,
            di.unit_of_measure,
            fi.inventory_value,
            fi.reorder_level,
            fi.needs_reorder,
            fi.days_since_restock,
            fi.days_until_expiration,
            fi.expiring_soon,
            fi.last_restock_date,
            fi.expiration_date
        )
        .orderBy("location_name", "ingredient_category", "ingredient_name")
    )
