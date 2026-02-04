-- Report: Menu Item Profitability
-- Profitability quadrant analysis for menu items



with fact_menu_item_performance as (
    select * from SAMMYS_SANDWICH_SHOP.DBT_DEV_marts.fct_menu_item_performance
),

dim_menu_item as (
    select * from SAMMYS_SANDWICH_SHOP.DBT_DEV_marts.dim_menu_item
    where is_current = true
),

-- Calculate averages for quadrant analysis
averages as (
    select
        avg(fmp.total_quantity_sold) as avg_quantity,
        avg(fmp.gross_margin_pct) as avg_margin
    from fact_menu_item_performance fmp
    where fmp.total_quantity_sold > 0
)

select 
    dm.item_name,
    dm.category,
    dm.subcategory,
    dm.base_price,
    dm.food_cost,
    fmp.total_quantity_sold,
    fmp.total_revenue,
    fmp.total_gross_profit,
    fmp.gross_margin_pct,
    
    -- Profitability quadrant
    case
        when fmp.total_quantity_sold >= a.avg_quantity and fmp.gross_margin_pct >= a.avg_margin then 'Stars'
        when fmp.total_quantity_sold >= a.avg_quantity and fmp.gross_margin_pct < a.avg_margin then 'Workhorses'
        when fmp.total_quantity_sold < a.avg_quantity and fmp.gross_margin_pct >= a.avg_margin then 'Puzzles'
        else 'Dogs'
    end as profitability_quadrant,
    
    a.avg_quantity as benchmark_quantity,
    a.avg_margin as benchmark_margin
    
from fact_menu_item_performance fmp
join dim_menu_item dm on fmp.menu_item_sk = dm.menu_item_sk
cross join averages a
order by fmp.total_gross_profit desc