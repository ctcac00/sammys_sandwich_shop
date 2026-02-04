# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Enriched Tables
# MAGIC Adds derived fields and business logic enrichments.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, concat_ws, datediff, current_date, current_timestamp, floor, when, upper,
    sum as spark_sum, count, coalesce, lit, dayofweek, hour
)
from pyspark.sql.types import IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Intermediate: Customers

# COMMAND ----------

@dlt.table(
    name="int_customers",
    comment="Customer data with derived fields",
    table_properties={"quality": "silver"}
)
def int_customers():
    return (
        dlt.read("stg_customers")
        .select(
            col("customer_id"),
            col("first_name"),
            col("last_name"),
            concat_ws(" ", col("first_name"), col("last_name")).alias("full_name"),
            col("email"),
            col("phone"),
            col("address"),
            col("city"),
            col("state"),
            col("zip_code"),
            col("birth_date"),
            col("signup_date"),
            col("loyalty_tier"),
            col("loyalty_points"),
            col("preferred_location_id"),
            col("marketing_opt_in"),
            # Age calculation
            floor(datediff(current_date(), col("birth_date")) / 365.25).cast(IntegerType()).alias("age"),
            # Loyalty tier rank
            when(upper(col("loyalty_tier")) == "PLATINUM", 4)
            .when(upper(col("loyalty_tier")) == "GOLD", 3)
            .when(upper(col("loyalty_tier")) == "SILVER", 2)
            .when(upper(col("loyalty_tier")) == "BRONZE", 1)
            .otherwise(0).alias("loyalty_tier_rank"),
            # Customer tenure
            datediff(current_date(), col("signup_date")).alias("customer_tenure_days"),
            current_timestamp().alias("_enriched_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Intermediate: Employees

# COMMAND ----------

@dlt.table(
    name="int_employees",
    comment="Employee data with derived fields",
    table_properties={"quality": "silver"}
)
def int_employees():
    return (
        dlt.read("stg_employees")
        .select(
            col("employee_id"),
            col("first_name"),
            col("last_name"),
            concat_ws(" ", col("first_name"), col("last_name")).alias("full_name"),
            col("email"),
            col("phone"),
            col("hire_date"),
            col("termination_date"),
            col("job_title"),
            col("department"),
            col("location_id"),
            col("hourly_rate"),
            col("is_active"),
            # Tenure calculation
            datediff(current_date(), col("hire_date")).alias("tenure_days"),
            floor(datediff(current_date(), col("hire_date")) / 30.0).alias("tenure_months"),
            floor(datediff(current_date(), col("hire_date")) / 365.25).alias("tenure_years"),
            current_timestamp().alias("_enriched_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Intermediate: Locations

# COMMAND ----------

@dlt.table(
    name="int_locations",
    comment="Location data with manager info",
    table_properties={"quality": "silver"}
)
def int_locations():
    locations = dlt.read("stg_locations")
    employees = dlt.read("int_employees")
    
    return (
        locations.alias("l")
        .join(
            employees.select(
                col("employee_id"),
                col("full_name").alias("manager_name")
            ).alias("e"),
            col("l.manager_id") == col("e.employee_id"),
            "left"
        )
        .select(
            col("l.location_id"),
            col("l.location_name"),
            col("l.address"),
            col("l.city"),
            col("l.state"),
            col("l.zip_code"),
            concat_ws(", ", col("l.city"), col("l.state")).alias("city_state"),
            col("l.region"),
            col("l.district"),
            col("l.open_date"),
            datediff(current_date(), col("l.open_date")).alias("days_open"),
            col("l.manager_id"),
            coalesce(col("e.manager_name"), lit("Unassigned")).alias("manager_name"),
            col("l.seating_capacity"),
            col("l.has_drive_thru"),
            col("l.is_active"),
            current_timestamp().alias("_enriched_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Intermediate: Menu Items

# COMMAND ----------

@dlt.table(
    name="int_menu_items",
    comment="Menu item data with profit margins",
    table_properties={"quality": "silver"}
)
def int_menu_items():
    return (
        dlt.read("stg_menu_items")
        .select(
            col("menu_item_id"),
            col("item_name"),
            col("description"),
            col("category"),
            col("subcategory"),
            col("price"),
            col("cost"),
            (col("price") - col("cost")).alias("profit"),
            when(col("price") > 0, ((col("price") - col("cost")) / col("price") * 100))
                .otherwise(0).alias("profit_margin_pct"),
            col("calories"),
            col("is_vegetarian"),
            col("is_gluten_free"),
            col("is_available"),
            current_timestamp().alias("_enriched_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Intermediate: Ingredients

# COMMAND ----------

@dlt.table(
    name="int_ingredients",
    comment="Ingredient data with supplier info",
    table_properties={"quality": "silver"}
)
def int_ingredients():
    ingredients = dlt.read("stg_ingredients")
    suppliers = dlt.read("stg_suppliers")
    
    return (
        ingredients.alias("i")
        .join(
            suppliers.select(
                col("supplier_id"),
                col("supplier_name"),
                col("lead_time_days")
            ).alias("s"),
            col("i.supplier_id") == col("s.supplier_id"),
            "left"
        )
        .select(
            col("i.ingredient_id"),
            col("i.ingredient_name"),
            col("i.category"),
            col("i.unit_of_measure"),
            col("i.unit_cost"),
            col("i.supplier_id"),
            col("s.supplier_name"),
            col("s.lead_time_days").alias("supplier_lead_time_days"),
            col("i.reorder_level"),
            col("i.reorder_quantity"),
            col("i.is_perishable"),
            col("i.shelf_life_days"),
            current_timestamp().alias("_enriched_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Intermediate: Menu Item Ingredients

# COMMAND ----------

@dlt.table(
    name="int_menu_item_ingredients",
    comment="Menu item ingredients with cost calculations",
    table_properties={"quality": "silver"}
)
def int_menu_item_ingredients():
    mappings = dlt.read("stg_menu_item_ingredients")
    ingredients = dlt.read("int_ingredients")
    
    return (
        mappings.alias("m")
        .join(
            ingredients.select(
                col("ingredient_id"),
                col("ingredient_name"),
                col("unit_cost")
            ).alias("i"),
            col("m.ingredient_id") == col("i.ingredient_id"),
            "left"
        )
        .select(
            col("m.menu_item_id"),
            col("m.ingredient_id"),
            col("i.ingredient_name"),
            col("m.quantity"),
            col("m.unit_of_measure"),
            col("i.unit_cost"),
            (col("m.quantity") * col("i.unit_cost")).alias("ingredient_cost"),
            current_timestamp().alias("_enriched_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Intermediate: Order Items

# COMMAND ----------

@dlt.table(
    name="int_order_items",
    comment="Order items with menu item details",
    table_properties={"quality": "silver"}
)
def int_order_items():
    order_items = dlt.read("stg_order_items")
    menu_items = dlt.read("int_menu_items")
    
    return (
        order_items.alias("oi")
        .join(
            menu_items.select(
                col("menu_item_id"),
                col("item_name"),
                col("category"),
                col("cost").alias("item_cost"),
                col("profit_margin_pct")
            ).alias("mi"),
            col("oi.menu_item_id") == col("mi.menu_item_id"),
            "left"
        )
        .select(
            col("oi.order_item_id"),
            col("oi.order_id"),
            col("oi.menu_item_id"),
            col("mi.item_name"),
            col("mi.category"),
            col("oi.quantity"),
            col("oi.unit_price"),
            col("oi.line_total"),
            col("mi.item_cost"),
            (col("oi.quantity") * col("mi.item_cost")).alias("line_cost"),
            (col("oi.line_total") - (col("oi.quantity") * col("mi.item_cost"))).alias("line_profit"),
            col("mi.profit_margin_pct"),
            col("oi.special_instructions"),
            current_timestamp().alias("_enriched_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Intermediate: Orders

# COMMAND ----------

@dlt.table(
    name="int_orders",
    comment="Orders with item counts and derived flags",
    table_properties={"quality": "silver"}
)
def int_orders():
    orders = dlt.read("stg_orders")
    order_items = dlt.read("stg_order_items")
    
    # Aggregate order items
    item_counts = (
        order_items.groupBy("order_id")
        .agg(
            spark_sum("quantity").alias("item_count"),
            count("*").alias("line_item_count")
        )
    )
    
    return (
        orders.alias("o")
        .join(item_counts.alias("ic"), col("o.order_id") == col("ic.order_id"), "left")
        .select(
            col("o.order_id"),
            col("o.customer_id"),
            col("o.employee_id"),
            col("o.location_id"),
            col("o.order_date"),
            col("o.order_datetime"),
            col("o.order_type"),
            col("o.order_status"),
            col("o.payment_method"),
            coalesce(col("ic.item_count"), lit(0)).alias("item_count"),
            coalesce(col("ic.line_item_count"), lit(0)).alias("line_item_count"),
            col("o.subtotal"),
            col("o.tax_amount"),
            col("o.discount_amount"),
            col("o.tip_amount"),
            col("o.total_amount"),
            # Derived flags
            when(col("o.discount_amount") > 0, lit(True)).otherwise(lit(False)).alias("has_discount"),
            when(col("o.tip_amount") > 0, lit(True)).otherwise(lit(False)).alias("has_tip"),
            when(col("o.customer_id").isNull(), lit(True)).otherwise(lit(False)).alias("is_guest_order"),
            when(dayofweek(col("o.order_date")).isin(1, 7), lit(True)).otherwise(lit(False)).alias("is_weekend"),
            # Time period
            when(hour(col("o.order_datetime")).between(6, 10), "Breakfast")
            .when(hour(col("o.order_datetime")).between(11, 14), "Lunch")
            .when(hour(col("o.order_datetime")).between(15, 17), "Afternoon")
            .when(hour(col("o.order_datetime")).between(18, 21), "Dinner")
            .otherwise("Late Night").alias("order_period"),
            current_timestamp().alias("_enriched_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Intermediate: Suppliers

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Intermediate: Inventory

# COMMAND ----------

@dlt.table(
    name="int_inventory",
    comment="Inventory with reorder status",
    table_properties={"quality": "silver"}
)
def int_inventory():
    inventory = dlt.read("stg_inventory")
    ingredients = dlt.read("int_ingredients")
    
    return (
        inventory.alias("i")
        .join(
            ingredients.select(
                col("ingredient_id"),
                col("ingredient_name"),
                col("unit_of_measure"),
                col("unit_cost"),
                col("reorder_level"),
                col("reorder_quantity"),
                col("is_perishable"),
                col("shelf_life_days")
            ).alias("ing"),
            col("i.ingredient_id") == col("ing.ingredient_id"),
            "left"
        )
        .select(
            col("i.inventory_id"),
            col("i.location_id"),
            col("i.ingredient_id"),
            col("ing.ingredient_name"),
            col("i.quantity_on_hand"),
            col("ing.unit_of_measure"),
            col("ing.unit_cost"),
            (col("i.quantity_on_hand") * col("ing.unit_cost")).alias("inventory_value"),
            col("ing.reorder_level"),
            col("ing.reorder_quantity"),
            when(col("i.quantity_on_hand") <= col("ing.reorder_level"), lit(True))
                .otherwise(lit(False)).alias("needs_reorder"),
            col("i.last_restock_date"),
            datediff(current_date(), col("i.last_restock_date")).alias("days_since_restock"),
            col("i.expiration_date"),
            datediff(col("i.expiration_date"), current_date()).alias("days_until_expiration"),
            col("ing.is_perishable"),
            when(
                col("ing.is_perishable") & (datediff(col("i.expiration_date"), current_date()) <= 3),
                lit(True)
            ).otherwise(lit(False)).alias("expiring_soon"),
            current_timestamp().alias("_enriched_at")
        )
    )
