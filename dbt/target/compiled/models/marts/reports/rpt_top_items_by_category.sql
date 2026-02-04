-- Report: Top Items by Category
-- Shows top selling items within each category



with fact_menu_item_performance as (
    select * from SAMMYS_SANDWICH_SHOP.DBT_DEV_marts.fct_menu_item_performance
),

dim_menu_item as (
    select * from SAMMYS_SANDWICH_SHOP.DBT_DEV_marts.dim_menu_item
    where is_current = true
),

ranked as (
    select 
        dm.category,
        dm.item_name,
        dm.subcategory,
        dm.base_price,
        fmp.total_quantity_sold,
        fmp.total_revenue,
        fmp.total_gross_profit,
        fmp.gross_margin_pct,
        row_number() over (partition by dm.category order by fmp.total_revenue desc) as category_rank
        
    from fact_menu_item_performance fmp
    join dim_menu_item dm on fmp.menu_item_sk = dm.menu_item_sk
)

select 
    category,
    item_name,
    subcategory,
    base_price,
    total_quantity_sold,
    total_revenue,
    total_gross_profit,
    gross_margin_pct,
    category_rank
from ranked
where category_rank <= 5
order by category, category_rank