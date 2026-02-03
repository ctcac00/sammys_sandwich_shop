/*
================================================================================
  STORED PROCEDURE: SP_LOAD_FACT_INVENTORY
================================================================================
  Loads the fact_inventory_snapshot table from enriched inventory data.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

CREATE OR REPLACE PROCEDURE sp_load_fact_inventory()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INTEGER;
BEGIN
    -- Append or merge inventory snapshots
    MERGE INTO fact_inventory_snapshot tgt
    USING (
        SELECT 
            inv.location_id || '_' || inv.ingredient_id || '_' || 
                TO_CHAR(inv.snapshot_date, 'YYYYMMDD') AS inventory_snapshot_id,
            TO_NUMBER(TO_CHAR(inv.snapshot_date, 'YYYYMMDD')) AS date_key,
            dl.location_sk,
            di.ingredient_sk,
            inv.quantity_on_hand,
            inv.quantity_reserved,
            inv.quantity_available,
            inv.reorder_point,
            inv.days_until_expiration,
            -- Stock value = quantity on hand * cost per unit
            inv.quantity_on_hand * di.cost_per_unit AS stock_value,
            -- Days of supply (simplified - assuming 10 units used per day)
            ROUND(inv.quantity_available / 10.0, 2) AS days_of_supply,
            inv.needs_reorder,
            inv.stock_status,
            -- Expiration risk
            CASE 
                WHEN inv.days_until_expiration < 0 THEN 'Expired'
                WHEN inv.days_until_expiration <= 1 THEN 'Critical'
                WHEN inv.days_until_expiration <= 3 THEN 'Warning'
                WHEN inv.days_until_expiration <= 7 THEN 'Monitor'
                ELSE 'OK'
            END AS expiration_risk
        FROM SAMMYS_ENRICHED.enriched_inventory inv
        LEFT JOIN dim_location dl 
            ON inv.location_id = dl.location_id AND dl.is_current = TRUE
        LEFT JOIN dim_ingredient di 
            ON inv.ingredient_id = di.ingredient_id AND di.is_current = TRUE
    ) src
    ON tgt.inventory_snapshot_id = src.inventory_snapshot_id
    WHEN MATCHED THEN UPDATE SET
        quantity_on_hand = src.quantity_on_hand,
        quantity_reserved = src.quantity_reserved,
        quantity_available = src.quantity_available,
        reorder_point = src.reorder_point,
        days_until_expiration = src.days_until_expiration,
        stock_value = src.stock_value,
        days_of_supply = src.days_of_supply,
        needs_reorder = src.needs_reorder,
        stock_status = src.stock_status,
        expiration_risk = src.expiration_risk
    WHEN NOT MATCHED THEN INSERT (
        inventory_snapshot_id, date_key, location_sk, ingredient_sk,
        quantity_on_hand, quantity_reserved, quantity_available, reorder_point,
        days_until_expiration, stock_value, days_of_supply, needs_reorder,
        stock_status, expiration_risk, _created_at
    ) VALUES (
        src.inventory_snapshot_id, src.date_key, src.location_sk, src.ingredient_sk,
        src.quantity_on_hand, src.quantity_reserved, src.quantity_available, 
        src.reorder_point, src.days_until_expiration, src.stock_value, 
        src.days_of_supply, src.needs_reorder, src.stock_status, 
        src.expiration_risk, CURRENT_TIMESTAMP()
    );
    
    SELECT COUNT(*) INTO v_rows_inserted FROM fact_inventory_snapshot;
    
    RETURN 'SUCCESS: fact_inventory_snapshot now has ' || v_rows_inserted || ' records';
END;
$$;

-- Load daily summary fact
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
