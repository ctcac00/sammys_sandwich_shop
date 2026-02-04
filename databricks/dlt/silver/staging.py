# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Staging Tables
# MAGIC Performs data type casting and basic cleaning on raw data.
# MAGIC Includes data quality expectations for validation.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, trim, lower, upper, initcap, to_date, coalesce, lit, when
)
from pyspark.sql.types import IntegerType, DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Staging: Customers

# COMMAND ----------

@dlt.table(
    name="stg_customers",
    comment="Cleaned and typed customer data",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect("valid_email_format", "email IS NULL OR email LIKE '%@%.%'")
def stg_customers():
    return (
        dlt.read("bronze_customers")
        .select(
            col("customer_id"),
            initcap(trim(col("first_name"))).alias("first_name"),
            initcap(trim(col("last_name"))).alias("last_name"),
            lower(trim(col("email"))).alias("email"),
            trim(col("phone")).alias("phone"),
            trim(col("address")).alias("address"),
            trim(col("city")).alias("city"),
            upper(trim(col("state"))).alias("state"),
            trim(col("zip_code")).alias("zip_code"),
            to_date(col("birth_date"), "yyyy-MM-dd").alias("birth_date"),
            to_date(col("signup_date"), "yyyy-MM-dd").alias("signup_date"),
            coalesce(trim(col("loyalty_tier")), lit("Bronze")).alias("loyalty_tier"),
            coalesce(col("loyalty_points").cast(IntegerType()), lit(0)).alias("loyalty_points"),
            trim(col("preferred_location")).alias("preferred_location_id"),
            when(upper(trim(col("marketing_opt_in"))).isin("TRUE", "YES", "1"), lit(True))
                .otherwise(lit(False)).alias("marketing_opt_in"),
            col("_loaded_at"),
            col("_source_file")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Staging: Employees

# COMMAND ----------

@dlt.table(
    name="stg_employees",
    comment="Cleaned and typed employee data",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_employee_id", "employee_id IS NOT NULL")
@dlt.expect("valid_hire_date", "hire_date IS NOT NULL")
def stg_employees():
    return (
        dlt.read("bronze_employees")
        .select(
            col("employee_id"),
            initcap(trim(col("first_name"))).alias("first_name"),
            initcap(trim(col("last_name"))).alias("last_name"),
            lower(trim(col("email"))).alias("email"),
            trim(col("phone")).alias("phone"),
            to_date(col("hire_date"), "yyyy-MM-dd").alias("hire_date"),
            to_date(col("termination_date"), "yyyy-MM-dd").alias("termination_date"),
            trim(col("job_title")).alias("job_title"),
            trim(col("department")).alias("department"),
            trim(col("location_id")).alias("location_id"),
            coalesce(col("hourly_rate").cast(DoubleType()), lit(0.0)).alias("hourly_rate"),
            when(upper(trim(col("is_active"))).isin("TRUE", "YES", "1"), lit(True))
                .otherwise(lit(False)).alias("is_active"),
            col("_loaded_at"),
            col("_source_file")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Staging: Locations

# COMMAND ----------

@dlt.table(
    name="stg_locations",
    comment="Cleaned and typed location data",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_location_id", "location_id IS NOT NULL")
def stg_locations():
    return (
        dlt.read("bronze_locations")
        .select(
            col("location_id"),
            trim(col("location_name")).alias("location_name"),
            trim(col("address")).alias("address"),
            trim(col("city")).alias("city"),
            upper(trim(col("state"))).alias("state"),
            trim(col("zip_code")).alias("zip_code"),
            trim(col("region")).alias("region"),
            trim(col("district")).alias("district"),
            to_date(col("open_date"), "yyyy-MM-dd").alias("open_date"),
            trim(col("manager_id")).alias("manager_id"),
            col("seating_capacity").cast(IntegerType()).alias("seating_capacity"),
            when(upper(trim(col("has_drive_thru"))).isin("TRUE", "YES", "1"), lit(True))
                .otherwise(lit(False)).alias("has_drive_thru"),
            when(upper(trim(col("is_active"))).isin("TRUE", "YES", "1"), lit(True))
                .otherwise(lit(False)).alias("is_active"),
            col("_loaded_at"),
            col("_source_file")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Staging: Menu Items

# COMMAND ----------

@dlt.table(
    name="stg_menu_items",
    comment="Cleaned and typed menu item data",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_menu_item_id", "menu_item_id IS NOT NULL")
@dlt.expect("positive_price", "price > 0")
def stg_menu_items():
    return (
        dlt.read("bronze_menu_items")
        .select(
            col("menu_item_id"),
            trim(col("item_name")).alias("item_name"),
            trim(col("description")).alias("description"),
            trim(col("category")).alias("category"),
            trim(col("subcategory")).alias("subcategory"),
            col("price").cast(DoubleType()).alias("price"),
            col("cost").cast(DoubleType()).alias("cost"),
            col("calories").cast(IntegerType()).alias("calories"),
            when(upper(trim(col("is_vegetarian"))).isin("TRUE", "YES", "1"), lit(True))
                .otherwise(lit(False)).alias("is_vegetarian"),
            when(upper(trim(col("is_gluten_free"))).isin("TRUE", "YES", "1"), lit(True))
                .otherwise(lit(False)).alias("is_gluten_free"),
            when(upper(trim(col("is_available"))).isin("TRUE", "YES", "1"), lit(True))
                .otherwise(lit(False)).alias("is_available"),
            col("_loaded_at"),
            col("_source_file")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Staging: Ingredients

# COMMAND ----------

@dlt.table(
    name="stg_ingredients",
    comment="Cleaned and typed ingredient data",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_ingredient_id", "ingredient_id IS NOT NULL")
def stg_ingredients():
    return (
        dlt.read("bronze_ingredients")
        .select(
            col("ingredient_id"),
            trim(col("ingredient_name")).alias("ingredient_name"),
            trim(col("category")).alias("category"),
            trim(col("unit_of_measure")).alias("unit_of_measure"),
            col("unit_cost").cast(DoubleType()).alias("unit_cost"),
            trim(col("supplier_id")).alias("supplier_id"),
            col("reorder_level").cast(IntegerType()).alias("reorder_level"),
            col("reorder_quantity").cast(IntegerType()).alias("reorder_quantity"),
            when(upper(trim(col("is_perishable"))).isin("TRUE", "YES", "1"), lit(True))
                .otherwise(lit(False)).alias("is_perishable"),
            col("shelf_life_days").cast(IntegerType()).alias("shelf_life_days"),
            col("_loaded_at"),
            col("_source_file")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Staging: Menu Item Ingredients

# COMMAND ----------

@dlt.table(
    name="stg_menu_item_ingredients",
    comment="Cleaned menu item to ingredient mappings",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_mapping", "menu_item_id IS NOT NULL AND ingredient_id IS NOT NULL")
def stg_menu_item_ingredients():
    return (
        dlt.read("bronze_menu_item_ingredients")
        .select(
            col("menu_item_id"),
            col("ingredient_id"),
            col("quantity").cast(DoubleType()).alias("quantity"),
            trim(col("unit_of_measure")).alias("unit_of_measure"),
            col("_loaded_at"),
            col("_source_file")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Staging: Orders

# COMMAND ----------

@dlt.table(
    name="stg_orders",
    comment="Cleaned and typed order data",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect("valid_total", "total_amount >= 0")
def stg_orders():
    return (
        dlt.read("bronze_orders")
        .select(
            col("order_id"),
            trim(col("customer_id")).alias("customer_id"),
            trim(col("employee_id")).alias("employee_id"),
            trim(col("location_id")).alias("location_id"),
            to_date(col("order_date"), "yyyy-MM-dd").alias("order_date"),
            col("order_datetime").cast("timestamp").alias("order_datetime"),
            trim(col("order_type")).alias("order_type"),
            trim(col("order_status")).alias("order_status"),
            trim(col("payment_method")).alias("payment_method"),
            col("subtotal").cast(DoubleType()).alias("subtotal"),
            coalesce(col("tax_amount").cast(DoubleType()), lit(0.0)).alias("tax_amount"),
            coalesce(col("discount_amount").cast(DoubleType()), lit(0.0)).alias("discount_amount"),
            coalesce(col("tip_amount").cast(DoubleType()), lit(0.0)).alias("tip_amount"),
            col("total_amount").cast(DoubleType()).alias("total_amount"),
            col("_loaded_at"),
            col("_source_file")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Staging: Order Items

# COMMAND ----------

@dlt.table(
    name="stg_order_items",
    comment="Cleaned and typed order item data",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_order_item", "order_item_id IS NOT NULL")
@dlt.expect("valid_quantity", "quantity > 0")
def stg_order_items():
    return (
        dlt.read("bronze_order_items")
        .select(
            col("order_item_id"),
            col("order_id"),
            col("menu_item_id"),
            col("quantity").cast(IntegerType()).alias("quantity"),
            col("unit_price").cast(DoubleType()).alias("unit_price"),
            col("line_total").cast(DoubleType()).alias("line_total"),
            trim(col("special_instructions")).alias("special_instructions"),
            col("_loaded_at"),
            col("_source_file")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Staging: Suppliers

# COMMAND ----------

@dlt.table(
    name="stg_suppliers",
    comment="Cleaned and typed supplier data",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_supplier_id", "supplier_id IS NOT NULL")
def stg_suppliers():
    return (
        dlt.read("bronze_suppliers")
        .select(
            col("supplier_id"),
            trim(col("supplier_name")).alias("supplier_name"),
            trim(col("contact_name")).alias("contact_name"),
            lower(trim(col("email"))).alias("email"),
            trim(col("phone")).alias("phone"),
            trim(col("address")).alias("address"),
            trim(col("city")).alias("city"),
            upper(trim(col("state"))).alias("state"),
            trim(col("zip_code")).alias("zip_code"),
            trim(col("payment_terms")).alias("payment_terms"),
            col("lead_time_days").cast(IntegerType()).alias("lead_time_days"),
            when(upper(trim(col("is_active"))).isin("TRUE", "YES", "1"), lit(True))
                .otherwise(lit(False)).alias("is_active"),
            col("_loaded_at"),
            col("_source_file")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Staging: Inventory

# COMMAND ----------

@dlt.table(
    name="stg_inventory",
    comment="Cleaned and typed inventory data",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_inventory_record", "location_id IS NOT NULL AND ingredient_id IS NOT NULL")
@dlt.expect("non_negative_quantity", "quantity_on_hand >= 0")
def stg_inventory():
    return (
        dlt.read("bronze_inventory")
        .select(
            col("inventory_id"),
            col("location_id"),
            col("ingredient_id"),
            col("quantity_on_hand").cast(DoubleType()).alias("quantity_on_hand"),
            to_date(col("last_restock_date"), "yyyy-MM-dd").alias("last_restock_date"),
            to_date(col("expiration_date"), "yyyy-MM-dd").alias("expiration_date"),
            col("_loaded_at"),
            col("_source_file")
        )
    )
