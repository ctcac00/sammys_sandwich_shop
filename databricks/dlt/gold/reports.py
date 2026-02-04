# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Report Views
# MAGIC Analytical views for business reporting.
# MAGIC 
# MAGIC DLT views are computed at query time (not materialized).

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, lit, when, sum as spark_sum, count, countDistinct, avg, 
    max as spark_max, min as spark_min, round as spark_round,
    lag, dense_rank, row_number, percent_rank
)
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Sales Reports

# COMMAND ----------

# MAGIC %md
# MAGIC ### Daily Sales Summary

# COMMAND ----------

@dlt.view(
    name="rpt_daily_sales_summary",
    comment="Daily sales metrics with day-over-day and week-over-week comparisons"
)
def rpt_daily_sales_summary():
    fds = dlt.read("fct_daily_summary")
    dd = dlt.read("dim_date")
    dl = dlt.read("dim_location").filter(col("is_current") == True)
    
    window_spec = Window.partitionBy("location_sk").orderBy("full_date")
    
    return (
        fds
        .join(dd, fds.date_key == dd.date_key)
        .join(dl, fds.location_sk == dl.location_sk)
        .select(
            dd.full_date.alias("order_date"),
            dd.day_name,
            dd.is_weekend,
            dl.location_name,
            dl.region,
            fds.total_orders,
            fds.total_items_sold,
            fds.unique_customers,
            fds.guest_orders,
            fds.gross_sales,
            fds.discounts_given,
            fds.net_sales,
            fds.tax_collected,
            fds.tips_received,
            fds.total_revenue,
            fds.avg_order_value,
            fds.avg_items_per_order,
            fds.discount_rate,
            # Day over day comparison
            lag("total_revenue", 1).over(window_spec).alias("prev_day_revenue"),
            (fds.total_revenue - lag("total_revenue", 1).over(window_spec)).alias("revenue_change"),
            # Week over week comparison
            lag("total_revenue", 7).over(window_spec).alias("same_day_last_week_revenue")
        )
        .orderBy(col("order_date").desc(), "location_name")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Weekly Sales Summary

# COMMAND ----------

@dlt.view(
    name="rpt_weekly_sales_summary",
    comment="Weekly aggregated sales metrics"
)
def rpt_weekly_sales_summary():
    fds = dlt.read("fct_daily_summary")
    dd = dlt.read("dim_date")
    dl = dlt.read("dim_location").filter(col("is_current") == True)
    
    return (
        fds
        .join(dd, fds.date_key == dd.date_key)
        .join(dl, fds.location_sk == dl.location_sk)
        .groupBy(dd.year, dd.week_of_year, dl.location_name, dl.region)
        .agg(
            spark_sum("total_orders").alias("total_orders"),
            spark_sum("total_items_sold").alias("total_items_sold"),
            spark_sum("gross_sales").alias("gross_sales"),
            spark_sum("net_sales").alias("net_sales"),
            spark_sum("total_revenue").alias("total_revenue"),
            avg("avg_order_value").alias("avg_order_value"),
            count("*").alias("days_with_sales")
        )
        .orderBy(col("year").desc(), col("week_of_year").desc(), "location_name")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Company Daily Totals

# COMMAND ----------

@dlt.view(
    name="rpt_company_daily_totals",
    comment="Company-wide daily totals across all locations"
)
def rpt_company_daily_totals():
    fds = dlt.read("fct_daily_summary")
    dd = dlt.read("dim_date")
    
    return (
        fds
        .join(dd, fds.date_key == dd.date_key)
        .groupBy(dd.date_key, dd.full_date, dd.day_name, dd.is_weekend)
        .agg(
            countDistinct("location_sk").alias("locations_reporting"),
            spark_sum("total_orders").alias("total_orders"),
            spark_sum("total_items_sold").alias("total_items_sold"),
            spark_sum("unique_customers").alias("total_customers"),
            spark_sum("gross_sales").alias("gross_sales"),
            spark_sum("net_sales").alias("net_sales"),
            spark_sum("total_revenue").alias("total_revenue"),
            avg("avg_order_value").alias("avg_order_value")
        )
        .orderBy(col("full_date").desc())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Customer Reports

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer Overview

# COMMAND ----------

@dlt.view(
    name="rpt_customer_overview",
    comment="Customer summary with lifetime metrics"
)
def rpt_customer_overview():
    fca = dlt.read("fct_customer_activity")
    dc = dlt.read("dim_customer").filter(col("is_current") == True)
    
    return (
        fca
        .join(dc, fca.customer_sk == dc.customer_sk)
        .select(
            dc.customer_id,
            dc.full_name,
            dc.email,
            dc.city,
            dc.state,
            dc.loyalty_tier,
            dc.age_group,
            dc.tenure_group,
            fca.total_orders,
            fca.total_revenue,
            fca.avg_order_value,
            fca.first_order_date,
            fca.last_order_date,
            fca.days_since_last_order,
            fca.recency_score,
            fca.frequency_score,
            fca.monetary_score,
            fca.rfm_segment
        )
        .orderBy(col("total_revenue").desc())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### RFM Segment Summary

# COMMAND ----------

@dlt.view(
    name="rpt_rfm_segment_summary",
    comment="RFM segmentation analysis"
)
def rpt_rfm_segment_summary():
    fca = dlt.read("fct_customer_activity")
    
    return (
        fca
        .groupBy("rfm_segment")
        .agg(
            count("*").alias("customer_count"),
            spark_sum("total_orders").alias("total_orders"),
            spark_sum("total_revenue").alias("total_revenue"),
            avg("total_orders").alias("avg_orders_per_customer"),
            avg("total_revenue").alias("avg_revenue_per_customer"),
            avg("avg_order_value").alias("avg_order_value"),
            avg("days_since_last_order").alias("avg_days_since_last_order")
        )
        .withColumn("revenue_pct", 
            spark_round(col("total_revenue") / spark_sum("total_revenue").over(Window.partitionBy()) * 100, 2))
        .orderBy(col("total_revenue").desc())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top Customers

# COMMAND ----------

@dlt.view(
    name="rpt_top_customers",
    comment="Top customers by revenue"
)
def rpt_top_customers():
    fca = dlt.read("fct_customer_activity")
    dc = dlt.read("dim_customer").filter(col("is_current") == True)
    
    return (
        fca
        .join(dc, fca.customer_sk == dc.customer_sk)
        .select(
            dc.customer_id,
            dc.full_name,
            dc.loyalty_tier,
            fca.total_orders,
            fca.total_revenue,
            fca.avg_order_value,
            fca.rfm_segment,
            dense_rank().over(Window.orderBy(col("total_revenue").desc())).alias("revenue_rank")
        )
        .filter(col("revenue_rank") <= 100)
        .orderBy("revenue_rank")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customers at Risk

# COMMAND ----------

@dlt.view(
    name="rpt_customers_at_risk",
    comment="Customers showing signs of churn"
)
def rpt_customers_at_risk():
    fca = dlt.read("fct_customer_activity")
    dc = dlt.read("dim_customer").filter(col("is_current") == True)
    
    return (
        fca
        .filter(col("rfm_segment").isin("At Risk", "Hibernating"))
        .join(dc, fca.customer_sk == dc.customer_sk)
        .select(
            dc.customer_id,
            dc.full_name,
            dc.email,
            dc.loyalty_tier,
            fca.total_orders,
            fca.total_revenue,
            fca.days_since_last_order,
            fca.rfm_segment
        )
        .orderBy(col("total_revenue").desc())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Loyalty Tier Analysis

# COMMAND ----------

@dlt.view(
    name="rpt_loyalty_tier_analysis",
    comment="Performance metrics by loyalty tier"
)
def rpt_loyalty_tier_analysis():
    fca = dlt.read("fct_customer_activity")
    dc = dlt.read("dim_customer").filter(col("is_current") == True)
    
    return (
        fca
        .join(dc, fca.customer_sk == dc.customer_sk)
        .groupBy(dc.loyalty_tier, dc.loyalty_tier_rank)
        .agg(
            count("*").alias("customer_count"),
            spark_sum("total_orders").alias("total_orders"),
            spark_sum("total_revenue").alias("total_revenue"),
            avg("total_orders").alias("avg_orders"),
            avg("total_revenue").alias("avg_revenue"),
            avg("avg_order_value").alias("avg_order_value")
        )
        .orderBy("loyalty_tier_rank")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Menu Item Reports

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top Selling Items

# COMMAND ----------

@dlt.view(
    name="rpt_top_selling_items",
    comment="Best-selling menu items"
)
def rpt_top_selling_items():
    fmp = dlt.read("fct_menu_item_performance")
    
    return (
        fmp
        .select(
            "menu_item_id",
            "item_name",
            "category",
            "times_ordered",
            "total_quantity_sold",
            "total_revenue",
            "total_profit",
            "profit_margin_pct",
            "sales_rank",
            "revenue_rank",
            "profit_rank"
        )
        .filter(col("sales_rank") <= 20)
        .orderBy("sales_rank")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Category Performance

# COMMAND ----------

@dlt.view(
    name="rpt_category_performance",
    comment="Performance metrics by menu category"
)
def rpt_category_performance():
    fmp = dlt.read("fct_menu_item_performance")
    
    return (
        fmp
        .groupBy("category")
        .agg(
            count("*").alias("item_count"),
            spark_sum("times_ordered").alias("total_orders"),
            spark_sum("total_quantity_sold").alias("total_quantity"),
            spark_sum("total_revenue").alias("total_revenue"),
            spark_sum("total_profit").alias("total_profit"),
            avg("profit_margin_pct").alias("avg_profit_margin")
        )
        .withColumn("revenue_share_pct",
            spark_round(col("total_revenue") / spark_sum("total_revenue").over(Window.partitionBy()) * 100, 2))
        .orderBy(col("total_revenue").desc())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Menu Item Profitability

# COMMAND ----------

@dlt.view(
    name="rpt_menu_item_profitability",
    comment="Menu item profitability analysis"
)
def rpt_menu_item_profitability():
    fmp = dlt.read("fct_menu_item_performance")
    
    return (
        fmp
        .select(
            "menu_item_id",
            "item_name",
            "category",
            "total_revenue",
            "total_cost",
            "total_profit",
            "profit_margin_pct",
            "avg_profit_per_sale",
            "total_quantity_sold",
            "profit_rank"
        )
        .orderBy("profit_rank")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Location Reports

# COMMAND ----------

# MAGIC %md
# MAGIC ### Location Performance

# COMMAND ----------

@dlt.view(
    name="rpt_location_performance",
    comment="Location performance summary"
)
def rpt_location_performance():
    fds = dlt.read("fct_daily_summary")
    dl = dlt.read("dim_location").filter(col("is_current") == True)
    
    return (
        fds
        .groupBy("location_sk")
        .agg(
            count("*").alias("days_operating"),
            spark_sum("total_orders").alias("total_orders"),
            spark_sum("total_items_sold").alias("total_items"),
            spark_sum("total_revenue").alias("total_revenue"),
            spark_sum("net_sales").alias("net_sales"),
            avg("total_orders").alias("avg_daily_orders"),
            avg("total_revenue").alias("avg_daily_revenue"),
            avg("avg_order_value").alias("avg_order_value")
        )
        .join(dl, "location_sk")
        .select(
            dl.location_id,
            dl.location_name,
            dl.region,
            dl.district,
            dl.has_drive_thru,
            "days_operating",
            "total_orders",
            "total_items",
            "total_revenue",
            "net_sales",
            "avg_daily_orders",
            "avg_daily_revenue",
            "avg_order_value",
            dense_rank().over(Window.orderBy(col("total_revenue").desc())).alias("revenue_rank")
        )
        .orderBy("revenue_rank")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Location Ranking

# COMMAND ----------

@dlt.view(
    name="rpt_location_ranking",
    comment="Location rankings by various metrics"
)
def rpt_location_ranking():
    rpt = dlt.read("rpt_location_performance")
    
    return (
        rpt
        .select(
            "location_id",
            "location_name",
            "region",
            "total_revenue",
            "total_orders",
            "avg_order_value",
            dense_rank().over(Window.orderBy(col("total_revenue").desc())).alias("revenue_rank"),
            dense_rank().over(Window.orderBy(col("total_orders").desc())).alias("orders_rank"),
            dense_rank().over(Window.orderBy(col("avg_order_value").desc())).alias("aov_rank"),
            dense_rank().over(
                Window.partitionBy("region").orderBy(col("total_revenue").desc())
            ).alias("region_revenue_rank")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Inventory Reports

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inventory Status

# COMMAND ----------

@dlt.view(
    name="rpt_inventory_status",
    comment="Current inventory status across all locations"
)
def rpt_inventory_status():
    fis = dlt.read("fct_inventory_snapshot")
    dl = dlt.read("dim_location").filter(col("is_current") == True)
    di = dlt.read("dim_ingredient").filter(col("is_current") == True)
    
    return (
        fis
        .join(dl, "location_sk")
        .join(di, "ingredient_sk")
        .select(
            dl.location_name,
            di.ingredient_name,
            di.category.alias("ingredient_category"),
            fis.quantity_on_hand,
            di.unit_of_measure,
            fis.inventory_value,
            fis.reorder_level,
            fis.needs_reorder,
            fis.days_until_expiration,
            fis.expiring_soon,
            fis.last_restock_date
        )
        .orderBy("location_name", "ingredient_category", "ingredient_name")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inventory Alerts

# COMMAND ----------

@dlt.view(
    name="rpt_inventory_alerts",
    comment="Low stock and expiring inventory alerts"
)
def rpt_inventory_alerts():
    fis = dlt.read("fct_inventory_snapshot")
    dl = dlt.read("dim_location").filter(col("is_current") == True)
    di = dlt.read("dim_ingredient").filter(col("is_current") == True)
    
    return (
        fis
        .filter((col("needs_reorder") == True) | (col("expiring_soon") == True))
        .join(dl, "location_sk")
        .join(di, "ingredient_sk")
        .select(
            dl.location_name,
            di.ingredient_name,
            fis.quantity_on_hand,
            fis.reorder_level,
            fis.needs_reorder,
            fis.days_until_expiration,
            fis.expiring_soon,
            when(col("needs_reorder") & col("expiring_soon"), "CRITICAL")
                .when(col("needs_reorder"), "LOW STOCK")
                .when(col("expiring_soon"), "EXPIRING")
                .otherwise("OK").alias("alert_type")
        )
        .orderBy(
            when(col("alert_type") == "CRITICAL", 1)
                .when(col("alert_type") == "LOW STOCK", 2)
                .otherwise(3),
            "location_name"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inventory Value by Location

# COMMAND ----------

@dlt.view(
    name="rpt_inventory_value_by_location",
    comment="Inventory value summary by location"
)
def rpt_inventory_value_by_location():
    fis = dlt.read("fct_inventory_snapshot")
    dl = dlt.read("dim_location").filter(col("is_current") == True)
    
    return (
        fis
        .groupBy("location_sk")
        .agg(
            count("*").alias("ingredient_count"),
            spark_sum("inventory_value").alias("total_inventory_value"),
            spark_sum(when(col("needs_reorder"), 1).otherwise(0)).alias("items_needing_reorder"),
            spark_sum(when(col("expiring_soon"), 1).otherwise(0)).alias("items_expiring_soon")
        )
        .join(dl, "location_sk")
        .select(
            dl.location_name,
            dl.region,
            "ingredient_count",
            "total_inventory_value",
            "items_needing_reorder",
            "items_expiring_soon"
        )
        .orderBy(col("total_inventory_value").desc())
    )
