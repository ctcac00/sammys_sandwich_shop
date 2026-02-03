/*
================================================================================
  REPORT: CATEGORY PERFORMANCE
================================================================================
  Menu category performance summary.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

CREATE OR REPLACE VIEW v_rpt_category_performance AS
SELECT 
    dm.category,
    COUNT(DISTINCT dm.item_id) AS item_count,
    SUM(fmp.total_quantity_sold) AS total_quantity,
    SUM(fmp.total_revenue) AS total_revenue,
    SUM(fmp.total_gross_profit) AS total_profit,
    ROUND(AVG(fmp.gross_margin_pct), 2) AS avg_margin_pct,
    -- Category contribution
    ROUND(SUM(fmp.total_revenue) / NULLIF((SELECT SUM(total_revenue) FROM fact_menu_item_performance), 0) * 100, 2) AS revenue_contribution_pct
FROM fact_menu_item_performance fmp
JOIN dim_menu_item dm ON fmp.menu_item_sk = dm.menu_item_sk
GROUP BY dm.category
ORDER BY total_revenue DESC;
