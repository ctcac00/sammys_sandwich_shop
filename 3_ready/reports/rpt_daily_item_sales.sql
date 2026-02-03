/*
================================================================================
  REPORT: DAILY ITEM SALES
================================================================================
  Daily sales breakdown by menu item.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

CREATE OR REPLACE VIEW v_rpt_daily_item_sales AS
SELECT 
    dd.full_date,
    dd.day_name,
    dm.item_name,
    dm.category,
    SUM(fsl.quantity) AS quantity_sold,
    SUM(fsl.line_total) AS revenue,
    SUM(fsl.gross_profit) AS profit,
    COUNT(DISTINCT fsl.order_id) AS orders_with_item
FROM fact_sales_line_item fsl
JOIN dim_date dd ON fsl.date_key = dd.date_key
JOIN dim_menu_item dm ON fsl.menu_item_sk = dm.menu_item_sk
GROUP BY dd.full_date, dd.day_name, dm.item_name, dm.category
ORDER BY dd.full_date DESC, revenue DESC;
