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

-- Top selling items overall
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

-- Top items by category
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

-- Menu item profitability analysis
CREATE OR REPLACE VIEW v_rpt_menu_item_profitability AS
SELECT 
    dm.item_name,
    dm.category,
    dm.base_price,
    dm.food_cost,
    dm.food_cost_pct,
    dm.gross_margin AS expected_margin,
    dm.gross_margin_pct AS expected_margin_pct,
    fmp.total_quantity_sold,
    fmp.total_revenue,
    fmp.total_food_cost AS actual_food_cost,
    fmp.total_gross_profit AS actual_profit,
    fmp.gross_margin_pct AS actual_margin_pct,
    -- Profit per unit
    ROUND(fmp.total_gross_profit / NULLIF(fmp.total_quantity_sold, 0), 2) AS profit_per_unit,
    -- Profitability quadrant
    CASE 
        WHEN fmp.total_revenue >= (SELECT AVG(total_revenue) FROM fact_menu_item_performance)
             AND fmp.gross_margin_pct >= (SELECT AVG(gross_margin_pct) FROM fact_menu_item_performance)
        THEN 'Star (High Revenue, High Margin)'
        WHEN fmp.total_revenue >= (SELECT AVG(total_revenue) FROM fact_menu_item_performance)
             AND fmp.gross_margin_pct < (SELECT AVG(gross_margin_pct) FROM fact_menu_item_performance)
        THEN 'Workhorse (High Revenue, Low Margin)'
        WHEN fmp.total_revenue < (SELECT AVG(total_revenue) FROM fact_menu_item_performance)
             AND fmp.gross_margin_pct >= (SELECT AVG(gross_margin_pct) FROM fact_menu_item_performance)
        THEN 'Opportunity (Low Revenue, High Margin)'
        ELSE 'Dog (Low Revenue, Low Margin)'
    END AS profitability_quadrant
FROM fact_menu_item_performance fmp
JOIN dim_menu_item dm ON fmp.menu_item_sk = dm.menu_item_sk
ORDER BY fmp.total_gross_profit DESC;

-- Daily item sales breakdown
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

-- Category performance summary
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
