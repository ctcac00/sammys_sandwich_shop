/*
================================================================================
  REPORT: MENU ITEM PROFITABILITY
================================================================================
  Menu item profitability analysis with quadrant classification.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

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
