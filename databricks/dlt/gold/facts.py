# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Fact Tables
# MAGIC Transactional and aggregated fact tables.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, lit, when, coalesce, current_timestamp, hour, minute, year, month, dayofmonth,
    round as spark_round, sum as spark_sum, count, countDistinct, avg, max as spark_max,
    min as spark_min, md5, datediff, current_date, row_number, dense_rank, ntile
)
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact: Sales (Order Level)
# MAGIC Grain: one row per order.

# COMMAND ----------

@dlt.table(
    name="fct_sales",
    comment="Order-level sales fact table",
    table_properties={"quality": "gold"}
)
@dlt.expect("valid_order_id", "order_id IS NOT NULL")
@dlt.expect("valid_date_key", "date_key IS NOT NULL")
def fct_sales():
    orders = dlt.read("int_orders").filter(col("order_status") == "Completed")
    dim_customer = dlt.read("dim_customer").filter(col("is_current") == True)
    dim_employee = dlt.read("dim_employee").filter(col("is_current") == True)
    dim_location = dlt.read("dim_location").filter(col("is_current") == True)
    dim_payment = dlt.read("dim_payment_method")
    dim_order_type = dlt.read("dim_order_type")
    
    # Get unknown customer SK for guest orders
    unknown_customer_sk = md5(lit(UNKNOWN_CUSTOMER_ID))
    
    return (
        orders.alias("o")
        .join(dim_customer.alias("dc"), col("o.customer_id") == col("dc.customer_id"), "left")
        .join(dim_employee.alias("de"), col("o.employee_id") == col("de.employee_id"), "left")
        .join(dim_location.alias("dl"), col("o.location_id") == col("dl.location_id"), "left")
        .join(dim_payment.alias("pm"), col("o.payment_method") == col("pm.payment_method"), "left")
        .join(dim_order_type.alias("ot"), col("o.order_type") == col("ot.order_type"), "left")
        .select(
            col("o.order_id"),
            # Dimension keys
            (year(col("o.order_date")) * 10000 + 
             month(col("o.order_date")) * 100 + 
             dayofmonth(col("o.order_date"))).cast(IntegerType()).alias("date_key"),
            (hour(col("o.order_datetime")) * 100 + 
             minute(col("o.order_datetime"))).cast(IntegerType()).alias("time_key"),
            coalesce(col("dc.customer_sk"), unknown_customer_sk).alias("customer_sk"),
            col("de.employee_sk"),
            col("dl.location_sk"),
            col("pm.payment_method_sk"),
            col("ot.order_type_sk"),
            # Degenerate dimensions
            col("o.order_status"),
            col("o.order_period"),
            # Measures
            col("o.item_count"),
            col("o.line_item_count"),
            col("o.subtotal"),
            col("o.tax_amount"),
            col("o.discount_amount"),
            col("o.tip_amount"),
            col("o.total_amount"),
            (col("o.subtotal") - col("o.discount_amount")).alias("net_sales"),
            # Derived measures
            when(col("o.subtotal") > 0, 
                 spark_round(col("o.discount_amount") / col("o.subtotal") * 100, 2))
                .otherwise(0).alias("discount_pct"),
            when(col("o.subtotal") > 0,
                 spark_round(col("o.tip_amount") / col("o.subtotal") * 100, 2))
                .otherwise(0).alias("tip_pct"),
            when(col("o.item_count") > 0,
                 spark_round(col("o.subtotal") / col("o.item_count"), 2))
                .otherwise(0).alias("avg_item_price"),
            # Flags
            col("o.has_discount"),
            col("o.has_tip"),
            col("o.is_guest_order"),
            col("o.is_weekend"),
            # Metadata
            current_timestamp().alias("_created_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact: Sales Line Item
# MAGIC Grain: one row per order line item.

# COMMAND ----------

@dlt.table(
    name="fct_sales_line_item",
    comment="Order line item detail fact table with profitability",
    table_properties={"quality": "gold"}
)
def fct_sales_line_item():
    order_items = dlt.read("int_order_items")
    orders = dlt.read("int_orders").filter(col("order_status") == "Completed")
    dim_menu_item = dlt.read("dim_menu_item").filter(col("is_current") == True)
    fct_sales = dlt.read("fct_sales")
    
    return (
        order_items.alias("oi")
        .join(orders.select("order_id", "order_date", "order_datetime", "location_id").alias("o"),
              col("oi.order_id") == col("o.order_id"), "inner")
        .join(fct_sales.select("order_id", "date_key", "time_key", "customer_sk", 
                               "employee_sk", "location_sk").alias("fs"),
              col("oi.order_id") == col("fs.order_id"), "inner")
        .join(dim_menu_item.alias("dm"),
              col("oi.menu_item_id") == col("dm.menu_item_id"), "left")
        .select(
            col("oi.order_item_id"),
            col("oi.order_id"),
            # Dimension keys
            col("fs.date_key"),
            col("fs.time_key"),
            col("fs.customer_sk"),
            col("fs.employee_sk"),
            col("fs.location_sk"),
            col("dm.menu_item_sk"),
            # Item attributes
            col("oi.menu_item_id"),
            col("oi.item_name"),
            col("oi.category"),
            # Measures
            col("oi.quantity"),
            col("oi.unit_price"),
            col("oi.line_total"),
            col("oi.item_cost"),
            col("oi.line_cost"),
            col("oi.line_profit"),
            col("oi.profit_margin_pct"),
            # Metadata
            current_timestamp().alias("_created_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact: Daily Summary
# MAGIC Grain: one row per location per day.

# COMMAND ----------

@dlt.table(
    name="fct_daily_summary",
    comment="Daily aggregated sales metrics per location",
    table_properties={"quality": "gold"}
)
def fct_daily_summary():
    fct_sales = dlt.read("fct_sales")
    
    return (
        fct_sales
        .groupBy("date_key", "location_sk")
        .agg(
            # Volume metrics
            count("*").alias("total_orders"),
            spark_sum("item_count").alias("total_items_sold"),
            countDistinct("customer_sk").alias("unique_customers"),
            spark_sum(when(col("is_guest_order"), 1).otherwise(0)).alias("guest_orders"),
            # Revenue metrics
            spark_sum("subtotal").alias("gross_sales"),
            spark_sum("discount_amount").alias("discounts_given"),
            spark_sum("net_sales").alias("net_sales"),
            spark_sum("tax_amount").alias("tax_collected"),
            spark_sum("tip_amount").alias("tips_received"),
            spark_sum("total_amount").alias("total_revenue"),
            # Performance metrics
            avg("total_amount").alias("avg_order_value"),
            avg("item_count").alias("avg_items_per_order"),
            avg("discount_pct").alias("discount_rate"),
            # Order type breakdown
            spark_sum(when(col("order_type_sk").isNotNull(), 1).otherwise(0)).alias("typed_orders"),
            # Weekend flag
            spark_max("is_weekend").alias("is_weekend"),
            # Metadata
            current_timestamp().alias("_created_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact: Inventory Snapshot
# MAGIC Grain: one row per location per ingredient.

# COMMAND ----------

@dlt.table(
    name="fct_inventory_snapshot",
    comment="Current inventory status by location and ingredient",
    table_properties={"quality": "gold"}
)
def fct_inventory_snapshot():
    inventory = dlt.read("int_inventory")
    dim_location = dlt.read("dim_location").filter(col("is_current") == True)
    dim_ingredient = dlt.read("dim_ingredient").filter(col("is_current") == True)
    
    return (
        inventory.alias("i")
        .join(dim_location.select("location_id", "location_sk").alias("dl"),
              col("i.location_id") == col("dl.location_id"), "left")
        .join(dim_ingredient.select("ingredient_id", "ingredient_sk").alias("di"),
              col("i.ingredient_id") == col("di.ingredient_id"), "left")
        .select(
            col("i.inventory_id"),
            # Dimension keys
            col("dl.location_sk"),
            col("di.ingredient_sk"),
            # Snapshot date
            current_date().alias("snapshot_date"),
            (year(current_date()) * 10000 + 
             month(current_date()) * 100 + 
             dayofmonth(current_date())).cast(IntegerType()).alias("date_key"),
            # Measures
            col("i.quantity_on_hand"),
            col("i.inventory_value"),
            col("i.reorder_level"),
            col("i.reorder_quantity"),
            col("i.needs_reorder"),
            col("i.days_since_restock"),
            col("i.days_until_expiration"),
            col("i.expiring_soon"),
            col("i.last_restock_date"),
            col("i.expiration_date"),
            # Metadata
            current_timestamp().alias("_created_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact: Customer Activity
# MAGIC Grain: one row per customer with lifetime metrics and RFM scores.

# COMMAND ----------

@dlt.table(
    name="fct_customer_activity",
    comment="Customer lifetime metrics and RFM segmentation",
    table_properties={"quality": "gold"}
)
def fct_customer_activity():
    fct_sales = dlt.read("fct_sales").filter(~col("is_guest_order"))
    fct_line_items = dlt.read("fct_sales_line_item")
    dim_date = dlt.read("dim_date")
    
    # Get max date from sales
    max_date = fct_sales.join(dim_date, "date_key").agg(spark_max("full_date").alias("max_date"))
    
    # Customer aggregates
    customer_metrics = (
        fct_sales
        .join(dim_date, "date_key")
        .groupBy("customer_sk")
        .agg(
            count("*").alias("total_orders"),
            spark_sum("total_amount").alias("total_revenue"),
            spark_sum("item_count").alias("total_items"),
            avg("total_amount").alias("avg_order_value"),
            spark_min("full_date").alias("first_order_date"),
            spark_max("full_date").alias("last_order_date"),
            countDistinct("location_sk").alias("locations_visited"),
            avg("tip_pct").alias("avg_tip_pct"),
            spark_sum(when(col("has_discount"), 1).otherwise(0)).alias("orders_with_discount")
        )
    )
    
    # Calculate RFM scores using window functions
    window_spec = Window.orderBy("customer_sk")
    
    return (
        customer_metrics
        .crossJoin(max_date)
        .select(
            col("customer_sk"),
            col("total_orders"),
            col("total_revenue"),
            col("total_items"),
            col("avg_order_value"),
            col("first_order_date"),
            col("last_order_date"),
            datediff(col("max_date"), col("last_order_date")).alias("days_since_last_order"),
            datediff(col("last_order_date"), col("first_order_date")).alias("customer_lifespan_days"),
            col("locations_visited"),
            col("avg_tip_pct"),
            col("orders_with_discount"),
            spark_round(col("orders_with_discount") / col("total_orders") * 100, 2).alias("discount_usage_rate"),
            # RFM scores (using ntile for quintiles)
            ntile(5).over(Window.orderBy(col("days_since_last_order").desc())).alias("recency_score"),
            ntile(5).over(Window.orderBy("total_orders")).alias("frequency_score"),
            ntile(5).over(Window.orderBy("total_revenue")).alias("monetary_score"),
            current_timestamp().alias("_created_at")
        )
        .withColumn("rfm_segment",
            when((col("recency_score") >= 4) & (col("frequency_score") >= 4) & (col("monetary_score") >= 4), "Champions")
            .when((col("recency_score") >= 4) & (col("frequency_score") >= 3), "Loyal Customers")
            .when((col("recency_score") >= 4), "Recent Customers")
            .when((col("frequency_score") >= 4), "Frequent Buyers")
            .when((col("monetary_score") >= 4), "Big Spenders")
            .when((col("recency_score") <= 2) & (col("frequency_score") <= 2), "At Risk")
            .when((col("recency_score") <= 2), "Hibernating")
            .otherwise("Average"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact: Menu Item Performance
# MAGIC Grain: one row per menu item with performance rankings.

# COMMAND ----------

@dlt.table(
    name="fct_menu_item_performance",
    comment="Menu item performance metrics and rankings",
    table_properties={"quality": "gold"}
)
def fct_menu_item_performance():
    fct_line_items = dlt.read("fct_sales_line_item")
    dim_menu_item = dlt.read("dim_menu_item").filter(col("is_current") == True)
    
    # Menu item aggregates
    item_metrics = (
        fct_line_items
        .groupBy("menu_item_sk")
        .agg(
            count("*").alias("times_ordered"),
            spark_sum("quantity").alias("total_quantity_sold"),
            spark_sum("line_total").alias("total_revenue"),
            spark_sum("line_cost").alias("total_cost"),
            spark_sum("line_profit").alias("total_profit"),
            avg("line_profit").alias("avg_profit_per_sale"),
            countDistinct("order_id").alias("unique_orders"),
            countDistinct("customer_sk").alias("unique_customers")
        )
    )
    
    # Add rankings
    return (
        item_metrics
        .join(dim_menu_item.select("menu_item_sk", "menu_item_id", "item_name", 
                                    "category", "profit_margin_pct"),
              "menu_item_sk", "inner")
        .select(
            col("menu_item_sk"),
            col("menu_item_id"),
            col("item_name"),
            col("category"),
            col("times_ordered"),
            col("total_quantity_sold"),
            col("total_revenue"),
            col("total_cost"),
            col("total_profit"),
            col("profit_margin_pct"),
            col("avg_profit_per_sale"),
            col("unique_orders"),
            col("unique_customers"),
            # Rankings
            dense_rank().over(Window.orderBy(col("total_quantity_sold").desc())).alias("sales_rank"),
            dense_rank().over(Window.orderBy(col("total_revenue").desc())).alias("revenue_rank"),
            dense_rank().over(Window.orderBy(col("total_profit").desc())).alias("profit_rank"),
            dense_rank().over(
                Window.partitionBy("category").orderBy(col("total_quantity_sold").desc())
            ).alias("category_sales_rank"),
            current_timestamp().alias("_created_at")
        )
    )
