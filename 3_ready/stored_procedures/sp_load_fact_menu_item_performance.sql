/*
================================================================================
  STORED PROCEDURE: SP_LOAD_FACT_MENU_ITEM_PERFORMANCE
================================================================================
  Loads the fact_menu_item_performance table with aggregated menu item metrics.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

CREATE OR REPLACE PROCEDURE sp_load_fact_menu_item_performance()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INTEGER;
BEGIN
    TRUNCATE TABLE fact_menu_item_performance;
    
    INSERT INTO fact_menu_item_performance (
        menu_item_sk,
        item_id,
        total_quantity_sold,
        total_orders,
        total_revenue,
        total_food_cost,
        total_gross_profit,
        avg_daily_quantity,
        revenue_per_unit,
        gross_margin_pct,
        revenue_rank,
        quantity_rank,
        profit_rank,
        _created_at,
        _updated_at
    )
    WITH item_metrics AS (
        SELECT 
            fsl.menu_item_sk,
            dm.item_id,
            SUM(fsl.quantity) AS total_quantity_sold,
            COUNT(DISTINCT fsl.order_id) AS total_orders,
            SUM(fsl.line_total) AS total_revenue,
            SUM(fsl.food_cost) AS total_food_cost,
            SUM(fsl.gross_profit) AS total_gross_profit,
            -- Date range for daily average
            MIN(fsl.date_key) AS first_sale_date,
            MAX(fsl.date_key) AS last_sale_date
        FROM fact_sales_line_item fsl
        JOIN dim_menu_item dm ON fsl.menu_item_sk = dm.menu_item_sk
        GROUP BY fsl.menu_item_sk, dm.item_id
    )
    SELECT 
        menu_item_sk,
        item_id,
        total_quantity_sold,
        total_orders,
        total_revenue,
        total_food_cost,
        total_gross_profit,
        -- Average daily quantity
        ROUND(total_quantity_sold / NULLIF(
            DATEDIFF('day', 
                TO_DATE(first_sale_date::VARCHAR, 'YYYYMMDD'),
                TO_DATE(last_sale_date::VARCHAR, 'YYYYMMDD')
            ) + 1, 0), 2) AS avg_daily_quantity,
        -- Revenue per unit
        ROUND(total_revenue / NULLIF(total_quantity_sold, 0), 2) AS revenue_per_unit,
        -- Gross margin percentage
        ROUND(total_gross_profit / NULLIF(total_revenue, 0) * 100, 2) AS gross_margin_pct,
        -- Rankings
        RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank,
        RANK() OVER (ORDER BY total_quantity_sold DESC) AS quantity_rank,
        RANK() OVER (ORDER BY total_gross_profit DESC) AS profit_rank,
        CURRENT_TIMESTAMP() AS _created_at,
        CURRENT_TIMESTAMP() AS _updated_at
    FROM item_metrics;
    
    v_rows_inserted := SQLROWCOUNT;
    
    RETURN 'SUCCESS: Loaded ' || v_rows_inserted || ' records into fact_menu_item_performance';
END;
$$;
