/*
================================================================================
  REPORT: TOP SELLING ITEMS
================================================================================
  Analyzes menu item performance by various metrics.
  Helps identify best sellers, most profitable items, and opportunities.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

CREATE OR REPLACE VIEW v_rpt_top_selling_items AS
SELECT 
    dm.item_name,
    dm.category,
    dm.subcategory,
    dm.base_price,
    dm.price_tier,
    dm.calories,
    dm.calorie_category,
    dm.food_cost,
    dm.gross_margin_pct AS menu_gross_margin_pct,
    -- Performance metrics
    fmp.total_quantity_sold,
    fmp.total_orders,
    fmp.total_revenue,
    fmp.total_food_cost,
    fmp.total_gross_profit,
    fmp.avg_daily_quantity,
    fmp.gross_margin_pct AS actual_gross_margin_pct,
    -- Rankings
    fmp.revenue_rank,
    fmp.quantity_rank,
    fmp.profit_rank,
    -- Revenue share
    ROUND(fmp.total_revenue / SUM(fmp.total_revenue) OVER () * 100, 2) AS revenue_share_pct,
    -- Cumulative revenue (for Pareto analysis)
    SUM(fmp.total_revenue) OVER (ORDER BY fmp.total_revenue DESC) AS cumulative_revenue,
    ROUND(SUM(fmp.total_revenue) OVER (ORDER BY fmp.total_revenue DESC) / 
          SUM(fmp.total_revenue) OVER () * 100, 2) AS cumulative_revenue_pct
FROM fact_menu_item_performance fmp
JOIN dim_menu_item dm ON fmp.menu_item_sk = dm.menu_item_sk
ORDER BY fmp.total_revenue DESC;
