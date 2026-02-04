-- Report: Category Performance
-- Menu category performance summary

{{ config(materialized='view') }}

with fact_menu_item_performance as (
    select * from {{ ref('fct_menu_item_performance') }}
),

dim_menu_item as (
    select * from {{ ref('dim_menu_item') }}
    where is_current = true
),

total_revenue as (
    select sum(total_revenue) as total from fact_menu_item_performance
)

select 
    dm.category,
    count(distinct dm.item_id) as item_count,
    sum(fmp.total_quantity_sold) as total_quantity,
    sum(fmp.total_revenue) as total_revenue,
    sum(fmp.total_gross_profit) as total_profit,
    round(avg(fmp.gross_margin_pct), 2) as avg_margin_pct,
    
    -- Category contribution
    round(sum(fmp.total_revenue) / nullif((select total from total_revenue), 0) * 100, 2) as revenue_contribution_pct
    
from fact_menu_item_performance fmp
join dim_menu_item dm on fmp.menu_item_sk = dm.menu_item_sk
group by dm.category
order by total_revenue desc
