/*
================================================================================
  STORED PROCEDURE: SP_LOAD_FACT_DAILY_SUMMARY
================================================================================
  Loads the fact_daily_summary table with aggregated daily metrics per location.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

CREATE OR REPLACE PROCEDURE sp_load_fact_daily_summary()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INTEGER;
BEGIN
    TRUNCATE TABLE fact_daily_summary;
    
    INSERT INTO fact_daily_summary (
        daily_summary_id,
        date_key,
        location_sk,
        total_orders,
        total_items_sold,
        unique_customers,
        guest_orders,
        gross_sales,
        discounts_given,
        net_sales,
        tax_collected,
        tips_received,
        total_revenue,
        avg_order_value,
        avg_items_per_order,
        discount_rate,
        dine_in_orders,
        takeout_orders,
        drive_thru_orders,
        breakfast_orders,
        lunch_orders,
        dinner_orders,
        _created_at
    )
    SELECT 
        o.location_id || '_' || TO_CHAR(o.order_date, 'YYYYMMDD') AS daily_summary_id,
        TO_NUMBER(TO_CHAR(o.order_date, 'YYYYMMDD')) AS date_key,
        dl.location_sk,
        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(o.item_count) AS total_items_sold,
        COUNT(DISTINCT CASE WHEN NOT o.is_guest_order THEN o.customer_id END) AS unique_customers,
        COUNT(CASE WHEN o.is_guest_order THEN 1 END) AS guest_orders,
        SUM(o.subtotal) AS gross_sales,
        SUM(o.discount_amount) AS discounts_given,
        SUM(o.subtotal - o.discount_amount) AS net_sales,
        SUM(o.tax_amount) AS tax_collected,
        SUM(o.tip_amount) AS tips_received,
        SUM(o.total_amount) AS total_revenue,
        ROUND(AVG(o.total_amount), 2) AS avg_order_value,
        ROUND(AVG(o.item_count), 2) AS avg_items_per_order,
        ROUND(SUM(o.discount_amount) / NULLIF(SUM(o.subtotal), 0) * 100, 2) AS discount_rate,
        COUNT(CASE WHEN o.order_type = 'Dine-In' THEN 1 END) AS dine_in_orders,
        COUNT(CASE WHEN o.order_type = 'Takeout' THEN 1 END) AS takeout_orders,
        COUNT(CASE WHEN o.order_type = 'Drive-Thru' THEN 1 END) AS drive_thru_orders,
        COUNT(CASE WHEN o.order_period = 'Breakfast' THEN 1 END) AS breakfast_orders,
        COUNT(CASE WHEN o.order_period = 'Lunch' THEN 1 END) AS lunch_orders,
        COUNT(CASE WHEN o.order_period = 'Dinner' THEN 1 END) AS dinner_orders,
        CURRENT_TIMESTAMP() AS _created_at
    FROM SAMMYS_ENRICHED.enriched_orders o
    LEFT JOIN dim_location dl 
        ON o.location_id = dl.location_id AND dl.is_current = TRUE
    WHERE o.order_status = 'Completed'
    GROUP BY o.location_id, o.order_date, dl.location_sk;
    
    v_rows_inserted := SQLROWCOUNT;
    
    RETURN 'SUCCESS: Loaded ' || v_rows_inserted || ' records into fact_daily_summary';
END;
$$;
