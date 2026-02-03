/*
================================================================================
  STORED PROCEDURE: SP_ENRICH_ORDERS
================================================================================
  Transforms raw order data into enriched format with:
  - Proper data type casting
  - Derived fields (day_of_week, hour, period, weekend flag)
  - Order metrics (item_count)
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_ENRICHED;

CREATE OR REPLACE PROCEDURE sp_enrich_orders()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INTEGER;
BEGIN
    TRUNCATE TABLE SAMMYS_ENRICHED.enriched_orders;
    
    INSERT INTO SAMMYS_ENRICHED.enriched_orders (
        order_id,
        customer_id,
        employee_id,
        location_id,
        order_date,
        order_time,
        order_datetime,
        order_type,
        order_status,
        subtotal,
        tax_amount,
        discount_amount,
        tip_amount,
        total_amount,
        payment_method,
        order_day_of_week,
        order_hour,
        order_period,
        is_weekend,
        has_discount,
        has_tip,
        is_guest_order,
        item_count,
        _enriched_at,
        _source_loaded_at
    )
    SELECT 
        o.order_id,
        NULLIF(o.customer_id, 'NULL') AS customer_id,
        o.employee_id,
        o.location_id,
        TRY_TO_DATE(o.order_date, 'YYYY-MM-DD') AS order_date,
        TRY_TO_TIME(o.order_time) AS order_time,
        TRY_TO_TIMESTAMP(o.order_date || ' ' || o.order_time) AS order_datetime,
        o.order_type,
        COALESCE(o.order_status, 'Unknown') AS order_status,
        TRY_TO_NUMBER(o.subtotal, 10, 2) AS subtotal,
        TRY_TO_NUMBER(o.tax_amount, 10, 2) AS tax_amount,
        COALESCE(TRY_TO_NUMBER(o.discount_amount, 10, 2), 0) AS discount_amount,
        COALESCE(TRY_TO_NUMBER(o.tip_amount, 10, 2), 0) AS tip_amount,
        TRY_TO_NUMBER(o.total_amount, 10, 2) AS total_amount,
        o.payment_method,
        -- Day of week name
        DAYNAME(TRY_TO_DATE(o.order_date, 'YYYY-MM-DD')) AS order_day_of_week,
        -- Hour of day
        HOUR(TRY_TO_TIME(o.order_time)) AS order_hour,
        -- Time period
        CASE 
            WHEN HOUR(TRY_TO_TIME(o.order_time)) BETWEEN 6 AND 10 THEN 'Breakfast'
            WHEN HOUR(TRY_TO_TIME(o.order_time)) BETWEEN 11 AND 14 THEN 'Lunch'
            WHEN HOUR(TRY_TO_TIME(o.order_time)) BETWEEN 15 AND 17 THEN 'Afternoon'
            WHEN HOUR(TRY_TO_TIME(o.order_time)) BETWEEN 18 AND 21 THEN 'Dinner'
            ELSE 'Late Night'
        END AS order_period,
        -- Weekend flag
        CASE 
            WHEN DAYOFWEEK(TRY_TO_DATE(o.order_date, 'YYYY-MM-DD')) IN (0, 6) THEN TRUE 
            ELSE FALSE 
        END AS is_weekend,
        -- Has discount
        CASE 
            WHEN TRY_TO_NUMBER(o.discount_amount, 10, 2) > 0 THEN TRUE 
            ELSE FALSE 
        END AS has_discount,
        -- Has tip
        CASE 
            WHEN TRY_TO_NUMBER(o.tip_amount, 10, 2) > 0 THEN TRUE 
            ELSE FALSE 
        END AS has_tip,
        -- Guest order (no customer_id)
        CASE 
            WHEN o.customer_id IS NULL OR o.customer_id = 'NULL' THEN TRUE 
            ELSE FALSE 
        END AS is_guest_order,
        -- Item count from order_items
        COALESCE(ic.item_count, 0) AS item_count,
        CURRENT_TIMESTAMP() AS _enriched_at,
        o._loaded_at AS _source_loaded_at
    FROM SAMMYS_RAW.orders o
    LEFT JOIN (
        SELECT 
            order_id, 
            SUM(TRY_TO_NUMBER(quantity)) AS item_count
        FROM SAMMYS_RAW.order_items
        GROUP BY order_id
    ) ic ON o.order_id = ic.order_id
    WHERE o.order_id IS NOT NULL;
    
    v_rows_inserted := SQLROWCOUNT;
    
    RETURN 'SUCCESS: Enriched ' || v_rows_inserted || ' order records';
END;
$$;

-- Enrich order items
CREATE OR REPLACE PROCEDURE sp_enrich_order_items()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INTEGER;
BEGIN
    TRUNCATE TABLE SAMMYS_ENRICHED.enriched_order_items;
    
    INSERT INTO SAMMYS_ENRICHED.enriched_order_items (
        order_item_id,
        order_id,
        item_id,
        quantity,
        unit_price,
        customizations,
        line_total,
        has_customization,
        _enriched_at,
        _source_loaded_at
    )
    SELECT 
        order_item_id,
        order_id,
        item_id,
        TRY_TO_NUMBER(quantity) AS quantity,
        TRY_TO_NUMBER(unit_price, 10, 2) AS unit_price,
        NULLIF(TRIM(customizations), '') AS customizations,
        TRY_TO_NUMBER(line_total, 10, 2) AS line_total,
        CASE 
            WHEN customizations IS NOT NULL AND TRIM(customizations) != '' THEN TRUE 
            ELSE FALSE 
        END AS has_customization,
        CURRENT_TIMESTAMP() AS _enriched_at,
        _loaded_at AS _source_loaded_at
    FROM SAMMYS_RAW.order_items
    WHERE order_item_id IS NOT NULL;
    
    v_rows_inserted := SQLROWCOUNT;
    
    RETURN 'SUCCESS: Enriched ' || v_rows_inserted || ' order item records';
END;
$$;
