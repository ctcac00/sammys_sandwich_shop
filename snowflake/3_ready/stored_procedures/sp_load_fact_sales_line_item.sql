/*
================================================================================
  STORED PROCEDURE: SP_LOAD_FACT_SALES_LINE_ITEM
================================================================================
  Loads the fact_sales_line_item table from enriched order items.
  Links to dimension tables and calculates food cost and gross profit.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

CREATE OR REPLACE PROCEDURE sp_load_fact_sales_line_item()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INTEGER;
BEGIN
    TRUNCATE TABLE fact_sales_line_item;
    
    INSERT INTO fact_sales_line_item (
        order_item_id,
        order_id,
        date_key,
        customer_sk,
        employee_sk,
        location_sk,
        menu_item_sk,
        quantity,
        unit_price,
        line_total,
        food_cost,
        gross_profit,
        gross_margin_pct,
        has_customization,
        _created_at
    )
    SELECT 
        oi.order_item_id,
        oi.order_id,
        TO_NUMBER(TO_CHAR(o.order_date, 'YYYYMMDD')) AS date_key,
        COALESCE(dc.customer_sk, dc_unknown.customer_sk) AS customer_sk,
        de.employee_sk,
        dl.location_sk,
        dm.menu_item_sk,
        oi.quantity,
        oi.unit_price,
        oi.line_total,
        -- Food cost = quantity * item food cost
        oi.quantity * COALESCE(dm.food_cost, 0) AS food_cost,
        -- Gross profit = line total - food cost
        oi.line_total - (oi.quantity * COALESCE(dm.food_cost, 0)) AS gross_profit,
        -- Gross margin %
        ROUND((oi.line_total - (oi.quantity * COALESCE(dm.food_cost, 0))) / 
              NULLIF(oi.line_total, 0) * 100, 2) AS gross_margin_pct,
        oi.has_customization,
        CURRENT_TIMESTAMP() AS _created_at
    FROM SAMMYS_ENRICHED.enriched_order_items oi
    JOIN SAMMYS_ENRICHED.enriched_orders o ON oi.order_id = o.order_id
    LEFT JOIN dim_customer dc 
        ON o.customer_id = dc.customer_id AND dc.is_current = TRUE
    LEFT JOIN dim_customer dc_unknown 
        ON dc_unknown.customer_id = 'UNKNOWN' AND dc_unknown.is_current = TRUE
    LEFT JOIN dim_employee de 
        ON o.employee_id = de.employee_id AND de.is_current = TRUE
    LEFT JOIN dim_location dl 
        ON o.location_id = dl.location_id AND dl.is_current = TRUE
    LEFT JOIN dim_menu_item dm 
        ON oi.item_id = dm.item_id AND dm.is_current = TRUE
    WHERE o.order_status = 'Completed';
    
    v_rows_inserted := SQLROWCOUNT;
    
    RETURN 'SUCCESS: Loaded ' || v_rows_inserted || ' records into fact_sales_line_item';
END;
$$;
