-- Report: Top Selling Items
-- Analyzes menu item performance by various metrics

{{ config(materialized='view') }}

with fact_menu_item_performance as (
    select * from {{ ref('fct_menu_item_performance') }}
),

dim_menu_item as (
    select * from {{ ref('dim_menu_item') }}
    where is_current = true
)

select 
    dm.item_name,
    dm.category,
    dm.subcategory,
    dm.base_price,
    dm.price_tier,
    dm.calories,
    dm.calorie_category,
    dm.food_cost,
    dm.gross_margin_pct as menu_gross_margin_pct,
    
    -- Performance metrics
    fmp.total_quantity_sold,
    fmp.total_orders,
    fmp.total_revenue,
    fmp.total_food_cost,
    fmp.total_gross_profit,
    fmp.avg_daily_quantity,
    fmp.gross_margin_pct as actual_gross_margin_pct,
    
    -- Rankings
    fmp.revenue_rank,
    fmp.quantity_rank,
    fmp.profit_rank,
    
    -- Revenue share
    round(fmp.total_revenue / nullif(sum(fmp.total_revenue) over (), 0) * 100, 2) as revenue_share_pct,
    
    -- Cumulative revenue (for Pareto analysis)
    sum(fmp.total_revenue) over (order by fmp.total_revenue desc) as cumulative_revenue,
    round(sum(fmp.total_revenue) over (order by fmp.total_revenue desc) / 
          nullif(sum(fmp.total_revenue) over (), 0) * 100, 2) as cumulative_revenue_pct
          
from fact_menu_item_performance fmp
join dim_menu_item dm on fmp.menu_item_sk = dm.menu_item_sk
order by fmp.total_revenue desc
