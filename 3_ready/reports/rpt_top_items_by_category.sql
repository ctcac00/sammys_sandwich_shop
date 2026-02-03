/*
================================================================================
  REPORT: TOP ITEMS BY CATEGORY
================================================================================
  Top selling items ranked within each category.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

CREATE OR REPLACE VIEW v_rpt_top_items_by_category AS
SELECT 
    dm.category,
    dm.item_name,
    dm.subcategory,
    dm.base_price,
    fmp.total_quantity_sold,
    fmp.total_revenue,
    fmp.total_gross_profit,
    fmp.gross_margin_pct,
    -- Rank within category
    RANK() OVER (PARTITION BY dm.category ORDER BY fmp.total_revenue DESC) AS category_revenue_rank,
    RANK() OVER (PARTITION BY dm.category ORDER BY fmp.total_quantity_sold DESC) AS category_qty_rank,
    -- Category totals
    SUM(fmp.total_revenue) OVER (PARTITION BY dm.category) AS category_total_revenue,
    -- Share of category
    ROUND(fmp.total_revenue / NULLIF(SUM(fmp.total_revenue) OVER (PARTITION BY dm.category), 0) * 100, 2) AS category_share_pct
FROM fact_menu_item_performance fmp
JOIN dim_menu_item dm ON fmp.menu_item_sk = dm.menu_item_sk
ORDER BY dm.category, fmp.total_revenue DESC;
