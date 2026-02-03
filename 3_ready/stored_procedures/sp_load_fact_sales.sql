/*
================================================================================
  STORED PROCEDURE: SP_LOAD_FACT_SALES
================================================================================
  Loads the fact_sales and fact_sales_line_item tables from enriched data.
  Links to dimension tables using surrogate keys.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

CREATE OR REPLACE PROCEDURE sp_load_fact_sales()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INTEGER;
BEGIN
    -- Full refresh of fact_sales
    TRUNCATE TABLE fact_sales;
    
    INSERT INTO fact_sales (
        order_id,
        date_key,
        time_key,
        customer_sk,
        employee_sk,
        location_sk,
        payment_method_sk,
        order_type_sk,
        order_status,
        item_count,
        subtotal,
        tax_amount,
        discount_amount,
        tip_amount,
        total_amount,
        net_sales,
        discount_pct,
        tip_pct,
        avg_item_price,
        has_discount,
        has_tip,
        is_guest_order,
        is_weekend,
        _created_at
    )
    SELECT 
        o.order_id,
        -- Date key (YYYYMMDD)
        TO_NUMBER(TO_CHAR(o.order_date, 'YYYYMMDD')) AS date_key,
        -- Time key (HHMM)
        HOUR(o.order_time) * 100 + MINUTE(o.order_time) AS time_key,
        -- Customer SK (use unknown customer for guest orders)
        COALESCE(dc.customer_sk, dc_unknown.customer_sk) AS customer_sk,
        de.employee_sk,
        dl.location_sk,
        pm.payment_method_sk,
        ot.order_type_sk,
        o.order_status,
        o.item_count,
        o.subtotal,
        o.tax_amount,
        o.discount_amount,
        o.tip_amount,
        o.total_amount,
        -- Net sales = subtotal - discount
        o.subtotal - o.discount_amount AS net_sales,
        -- Discount percentage
        ROUND(o.discount_amount / NULLIF(o.subtotal, 0) * 100, 2) AS discount_pct,
        -- Tip percentage
        ROUND(o.tip_amount / NULLIF(o.subtotal, 0) * 100, 2) AS tip_pct,
        -- Average item price
        ROUND(o.subtotal / NULLIF(o.item_count, 0), 2) AS avg_item_price,
        o.has_discount,
        o.has_tip,
        o.is_guest_order,
        o.is_weekend,
        CURRENT_TIMESTAMP() AS _created_at
    FROM SAMMYS_ENRICHED.enriched_orders o
    LEFT JOIN dim_customer dc 
        ON o.customer_id = dc.customer_id AND dc.is_current = TRUE
    LEFT JOIN dim_customer dc_unknown 
        ON dc_unknown.customer_id = 'UNKNOWN' AND dc_unknown.is_current = TRUE
    LEFT JOIN dim_employee de 
        ON o.employee_id = de.employee_id AND de.is_current = TRUE
    LEFT JOIN dim_location dl 
        ON o.location_id = dl.location_id AND dl.is_current = TRUE
    LEFT JOIN dim_payment_method pm 
        ON o.payment_method = pm.payment_method
    LEFT JOIN dim_order_type ot 
        ON o.order_type = ot.order_type
    WHERE o.order_status = 'Completed';
    
    v_rows_inserted := SQLROWCOUNT;
    
    RETURN 'SUCCESS: Loaded ' || v_rows_inserted || ' records into fact_sales';
END;
$$;

-- Load fact_sales_line_item
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
