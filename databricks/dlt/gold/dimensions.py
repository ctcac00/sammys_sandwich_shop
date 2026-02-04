# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Dimension Tables
# MAGIC SCD Type 1 dimension tables for the dimensional model.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, lit, when, current_timestamp, current_date, floor, md5, concat_ws,
    explode, sequence, to_date, year, month, dayofmonth, dayofweek, dayofyear,
    weekofyear, quarter, date_format, hour, minute
)
from pyspark.sql.types import StringType, IntegerType

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension: Date
# MAGIC Date spine with calendar attributes.

# COMMAND ----------

@dlt.table(
    name="dim_date",
    comment="Date dimension with calendar attributes",
    table_properties={"quality": "gold"}
)
def dim_date():
    # Generate date spine
    date_spine = (
        spark.range(1)
        .select(
            explode(
                sequence(
                    to_date(lit(DATE_SPINE_START)),
                    to_date(lit(DATE_SPINE_END))
                )
            ).alias("full_date")
        )
    )
    
    return (
        date_spine
        .select(
            (year(col("full_date")) * 10000 + 
             month(col("full_date")) * 100 + 
             dayofmonth(col("full_date"))).cast(IntegerType()).alias("date_key"),
            col("full_date"),
            year(col("full_date")).alias("year"),
            quarter(col("full_date")).alias("quarter"),
            month(col("full_date")).alias("month"),
            date_format(col("full_date"), "MMMM").alias("month_name"),
            date_format(col("full_date"), "MMM").alias("month_short"),
            weekofyear(col("full_date")).alias("week_of_year"),
            dayofmonth(col("full_date")).alias("day_of_month"),
            dayofyear(col("full_date")).alias("day_of_year"),
            dayofweek(col("full_date")).alias("day_of_week"),
            date_format(col("full_date"), "EEEE").alias("day_name"),
            date_format(col("full_date"), "E").alias("day_short"),
            when(dayofweek(col("full_date")).isin(1, 7), lit(True))
                .otherwise(lit(False)).alias("is_weekend"),
            # Fiscal year (assuming Oct start)
            when(month(col("full_date")) >= 10, year(col("full_date")) + 1)
                .otherwise(year(col("full_date"))).alias("fiscal_year"),
            when(month(col("full_date")) >= 10, quarter(col("full_date")) - 3)
                .otherwise(quarter(col("full_date")) + 1).alias("fiscal_quarter")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension: Time
# MAGIC Time of day dimension.

# COMMAND ----------

@dlt.table(
    name="dim_time",
    comment="Time of day dimension",
    table_properties={"quality": "gold"}
)
def dim_time():
    # Generate time spine (every minute)
    time_spine = (
        spark.range(24 * 60)
        .select(
            (col("id") / 60).cast(IntegerType()).alias("hour"),
            (col("id") % 60).cast(IntegerType()).alias("minute")
        )
    )
    
    return (
        time_spine
        .select(
            (col("hour") * 100 + col("minute")).cast(IntegerType()).alias("time_key"),
            col("hour"),
            col("minute"),
            when(col("hour") < 12, "AM").otherwise("PM").alias("am_pm"),
            when(col("hour") == 0, 12)
                .when(col("hour") <= 12, col("hour"))
                .otherwise(col("hour") - 12).alias("hour_12"),
            when(col("hour").between(6, 10), "Breakfast")
                .when(col("hour").between(11, 14), "Lunch")
                .when(col("hour").between(15, 17), "Afternoon")
                .when(col("hour").between(18, 21), "Dinner")
                .otherwise("Late Night").alias("time_period"),
            when(col("hour").between(11, 13), lit(True))
                .otherwise(lit(False)).alias("is_lunch_rush"),
            when(col("hour").between(17, 19), lit(True))
                .otherwise(lit(False)).alias("is_dinner_rush")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension: Payment Method

# COMMAND ----------

@dlt.table(
    name="dim_payment_method",
    comment="Payment method reference dimension",
    table_properties={"quality": "gold"}
)
def dim_payment_method():
    payment_methods = [
        ("Cash", "Cash", True),
        ("Credit Card", "Card", False),
        ("Debit Card", "Card", False),
        ("Gift Card", "Other", False),
        ("Mobile Payment", "Digital", False),
        ("Unknown", "Unknown", False),
    ]
    
    return (
        spark.createDataFrame(payment_methods, ["payment_method", "payment_category", "is_cash"])
        .select(
            md5(col("payment_method")).alias("payment_method_sk"),
            col("payment_method"),
            col("payment_category"),
            col("is_cash")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension: Order Type

# COMMAND ----------

@dlt.table(
    name="dim_order_type",
    comment="Order type reference dimension",
    table_properties={"quality": "gold"}
)
def dim_order_type():
    order_types = [
        ("Dine-In", True, False),
        ("Takeout", False, False),
        ("Drive-Thru", False, True),
        ("Delivery", False, False),
        ("Catering", False, False),
        ("Unknown", False, False),
    ]
    
    return (
        spark.createDataFrame(order_types, ["order_type", "is_dine_in", "is_drive_thru"])
        .select(
            md5(col("order_type")).alias("order_type_sk"),
            col("order_type"),
            col("is_dine_in"),
            col("is_drive_thru")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension: Customer

# COMMAND ----------

@dlt.table(
    name="dim_customer",
    comment="Customer dimension with demographics and loyalty info",
    table_properties={"quality": "gold"}
)
@dlt.expect("valid_customer_sk", "customer_sk IS NOT NULL")
def dim_customer():
    customers = dlt.read("int_customers")
    
    # Build customer dimension
    customer_dim = (
        customers
        .select(
            md5(col("customer_id").cast(StringType())).alias("customer_sk"),
            col("customer_id"),
            col("first_name"),
            col("last_name"),
            col("full_name"),
            col("email"),
            col("phone"),
            col("city"),
            col("state"),
            col("zip_code"),
            # Age grouping
            when(col("age") < 18, "Under 18")
            .when(col("age").between(18, 24), "18-24")
            .when(col("age").between(25, 34), "25-34")
            .when(col("age").between(35, 44), "35-44")
            .when(col("age").between(45, 54), "45-54")
            .when(col("age").between(55, 64), "55-64")
            .otherwise("65+").alias("age_group"),
            col("loyalty_tier"),
            col("loyalty_tier_rank"),
            col("signup_date"),
            floor(col("customer_tenure_days") / 30.0).alias("tenure_months"),
            # Tenure grouping
            when(col("customer_tenure_days") < 90, "New (0-3 mo)")
            .when(col("customer_tenure_days") < 365, "Growing (3-12 mo)")
            .when(col("customer_tenure_days") < 730, "Established (1-2 yr)")
            .otherwise("Loyal (2+ yr)").alias("tenure_group"),
            col("marketing_opt_in"),
            col("preferred_location_id"),
            current_date().alias("effective_date"),
            lit(True).alias("is_current"),
            current_timestamp().alias("_created_at"),
            current_timestamp().alias("_updated_at")
        )
    )
    
    # Create unknown customer record
    unknown_customer = spark.createDataFrame([{
        "customer_sk": md5(lit(UNKNOWN_CUSTOMER_ID)).cast(StringType()),
        "customer_id": UNKNOWN_CUSTOMER_ID,
        "first_name": "Guest",
        "last_name": "Customer",
        "full_name": "Guest Customer",
        "email": None,
        "phone": None,
        "city": None,
        "state": None,
        "zip_code": None,
        "age_group": "Unknown",
        "loyalty_tier": "None",
        "loyalty_tier_rank": 0,
        "signup_date": None,
        "tenure_months": 0,
        "tenure_group": "Unknown",
        "marketing_opt_in": False,
        "preferred_location_id": None,
        "effective_date": "1900-01-01",
        "is_current": True,
    }])
    
    return customer_dim.unionByName(
        unknown_customer.select(
            md5(lit(UNKNOWN_CUSTOMER_ID)).alias("customer_sk"),
            lit(UNKNOWN_CUSTOMER_ID).alias("customer_id"),
            lit("Guest").alias("first_name"),
            lit("Customer").alias("last_name"),
            lit("Guest Customer").alias("full_name"),
            lit(None).cast(StringType()).alias("email"),
            lit(None).cast(StringType()).alias("phone"),
            lit(None).cast(StringType()).alias("city"),
            lit(None).cast(StringType()).alias("state"),
            lit(None).cast(StringType()).alias("zip_code"),
            lit("Unknown").alias("age_group"),
            lit("None").alias("loyalty_tier"),
            lit(0).alias("loyalty_tier_rank"),
            lit(None).cast("date").alias("signup_date"),
            lit(0).cast("long").alias("tenure_months"),
            lit("Unknown").alias("tenure_group"),
            lit(False).alias("marketing_opt_in"),
            lit(None).cast(StringType()).alias("preferred_location_id"),
            to_date(lit("1900-01-01")).alias("effective_date"),
            lit(True).alias("is_current"),
            current_timestamp().alias("_created_at"),
            current_timestamp().alias("_updated_at")
        ),
        allowMissingColumns=True
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension: Employee

# COMMAND ----------

@dlt.table(
    name="dim_employee",
    comment="Employee dimension with tenure and role info",
    table_properties={"quality": "gold"}
)
def dim_employee():
    employees = dlt.read("int_employees")
    
    employee_dim = (
        employees
        .select(
            md5(col("employee_id").cast(StringType())).alias("employee_sk"),
            col("employee_id"),
            col("first_name"),
            col("last_name"),
            col("full_name"),
            col("email"),
            col("job_title"),
            col("department"),
            col("location_id"),
            col("hire_date"),
            col("termination_date"),
            col("hourly_rate"),
            col("is_active"),
            col("tenure_days"),
            col("tenure_months"),
            col("tenure_years"),
            # Tenure grouping
            when(col("tenure_months") < 3, "New (0-3 mo)")
            .when(col("tenure_months") < 12, "Growing (3-12 mo)")
            .when(col("tenure_years") < 3, "Experienced (1-3 yr)")
            .otherwise("Veteran (3+ yr)").alias("tenure_group"),
            current_date().alias("effective_date"),
            lit(True).alias("is_current"),
            current_timestamp().alias("_created_at"),
            current_timestamp().alias("_updated_at")
        )
    )
    
    # Add unknown employee
    unknown_employee = spark.sql(f"""
        SELECT 
            md5('{UNKNOWN_EMPLOYEE_ID}') as employee_sk,
            '{UNKNOWN_EMPLOYEE_ID}' as employee_id,
            'Unknown' as first_name,
            'Employee' as last_name,
            'Unknown Employee' as full_name,
            null as email,
            'Unknown' as job_title,
            'Unknown' as department,
            null as location_id,
            null as hire_date,
            null as termination_date,
            0.0 as hourly_rate,
            false as is_active,
            0 as tenure_days,
            0 as tenure_months,
            0 as tenure_years,
            'Unknown' as tenure_group,
            to_date('1900-01-01') as effective_date,
            true as is_current,
            current_timestamp() as _created_at,
            current_timestamp() as _updated_at
    """)
    
    return employee_dim.unionByName(unknown_employee, allowMissingColumns=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension: Location

# COMMAND ----------

@dlt.table(
    name="dim_location",
    comment="Location dimension with geography and manager info",
    table_properties={"quality": "gold"}
)
def dim_location():
    locations = dlt.read("int_locations")
    
    location_dim = (
        locations
        .select(
            md5(col("location_id").cast(StringType())).alias("location_sk"),
            col("location_id"),
            col("location_name"),
            col("address"),
            col("city"),
            col("state"),
            col("zip_code"),
            col("city_state"),
            col("region"),
            col("district"),
            col("open_date"),
            col("days_open"),
            col("manager_id"),
            col("manager_name"),
            col("seating_capacity"),
            col("has_drive_thru"),
            col("is_active"),
            # Location age grouping
            when(col("days_open") < 365, "New (< 1 yr)")
            .when(col("days_open") < 1095, "Established (1-3 yr)")
            .otherwise("Mature (3+ yr)").alias("location_age_group"),
            current_date().alias("effective_date"),
            lit(True).alias("is_current"),
            current_timestamp().alias("_created_at"),
            current_timestamp().alias("_updated_at")
        )
    )
    
    # Add unknown location
    unknown_location = spark.sql(f"""
        SELECT 
            md5('{UNKNOWN_LOCATION_ID}') as location_sk,
            '{UNKNOWN_LOCATION_ID}' as location_id,
            'Unknown Location' as location_name,
            null as address,
            null as city,
            null as state,
            null as zip_code,
            null as city_state,
            'Unknown' as region,
            'Unknown' as district,
            null as open_date,
            0 as days_open,
            null as manager_id,
            'Unknown' as manager_name,
            0 as seating_capacity,
            false as has_drive_thru,
            false as is_active,
            'Unknown' as location_age_group,
            to_date('1900-01-01') as effective_date,
            true as is_current,
            current_timestamp() as _created_at,
            current_timestamp() as _updated_at
    """)
    
    return location_dim.unionByName(unknown_location, allowMissingColumns=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension: Ingredient

# COMMAND ----------

@dlt.table(
    name="dim_ingredient",
    comment="Ingredient dimension with supplier and cost info",
    table_properties={"quality": "gold"}
)
def dim_ingredient():
    return (
        dlt.read("int_ingredients")
        .select(
            md5(col("ingredient_id").cast(StringType())).alias("ingredient_sk"),
            col("ingredient_id"),
            col("ingredient_name"),
            col("category"),
            col("unit_of_measure"),
            col("unit_cost"),
            col("supplier_id"),
            col("supplier_name"),
            col("supplier_lead_time_days"),
            col("reorder_level"),
            col("reorder_quantity"),
            col("is_perishable"),
            col("shelf_life_days"),
            current_date().alias("effective_date"),
            lit(True).alias("is_current"),
            current_timestamp().alias("_created_at"),
            current_timestamp().alias("_updated_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension: Menu Item

# COMMAND ----------

@dlt.table(
    name="dim_menu_item",
    comment="Menu item dimension with profitability info",
    table_properties={"quality": "gold"}
)
def dim_menu_item():
    return (
        dlt.read("int_menu_items")
        .select(
            md5(col("menu_item_id").cast(StringType())).alias("menu_item_sk"),
            col("menu_item_id"),
            col("item_name"),
            col("description"),
            col("category"),
            col("subcategory"),
            col("price"),
            col("cost"),
            col("profit"),
            col("profit_margin_pct"),
            # Margin grouping
            when(col("profit_margin_pct") < 20, "Low (< 20%)")
            .when(col("profit_margin_pct") < 40, "Medium (20-40%)")
            .when(col("profit_margin_pct") < 60, "High (40-60%)")
            .otherwise("Premium (60%+)").alias("margin_group"),
            col("calories"),
            # Calorie grouping
            when(col("calories") < 300, "Light (< 300)")
            .when(col("calories") < 600, "Medium (300-600)")
            .when(col("calories") < 900, "Hearty (600-900)")
            .otherwise("Indulgent (900+)").alias("calorie_group"),
            col("is_vegetarian"),
            col("is_gluten_free"),
            col("is_available"),
            current_date().alias("effective_date"),
            lit(True).alias("is_current"),
            current_timestamp().alias("_created_at"),
            current_timestamp().alias("_updated_at")
        )
    )
