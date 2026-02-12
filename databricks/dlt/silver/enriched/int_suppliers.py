# Databricks notebook source
# MAGIC %md
# MAGIC # Intermediate: Suppliers
# MAGIC Enriches suppliers with ingredient counts.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, concat_ws, coalesce, lit, current_timestamp, count
)

# COMMAND ----------

@dlt.table(
    name="int_suppliers",
    comment="Suppliers with ingredient counts",
    table_properties={"quality": "silver"}
)
def int_suppliers():
    suppliers = dlt.read("stg_suppliers")
    ingredients = dlt.read("stg_ingredients")
    
    # Count ingredients per supplier
    ingredient_counts = (
        ingredients.groupBy("supplier_id")
        .agg(count("*").alias("ingredient_count"))
    )
    
    return (
        suppliers.alias("s")
        .join(ingredient_counts.alias("ic"), col("s.supplier_id") == col("ic.supplier_id"), "left")
        .select(
            col("s.supplier_id"),
            col("s.supplier_name"),
            col("s.contact_name"),
            col("s.email"),
            col("s.phone"),
            col("s.address"),
            col("s.city"),
            col("s.state"),
            concat_ws(", ", col("s.city"), col("s.state")).alias("city_state"),
            col("s.zip_code"),
            col("s.payment_terms"),
            col("s.lead_time_days"),
            col("s.is_active"),
            coalesce(col("ic.ingredient_count"), lit(0)).alias("ingredient_count"),
            current_timestamp().alias("_enriched_at")
        )
    )
