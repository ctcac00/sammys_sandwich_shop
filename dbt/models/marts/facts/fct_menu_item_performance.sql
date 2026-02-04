-- Fact table for menu item performance metrics
-- Grain: one row per menu item

{{ config(materialized='table') }}

with fct_sales_line_item as (
    select * from {{ ref('fct_sales_line_item') }}
),

dim_menu_item as (
    select * from {{ ref('dim_menu_item') }}
    where is_current = true
),

dim_date as (
    select * from {{ ref('dim_date') }}
),

-- Menu item aggregations
item_metrics as (
    select
        li.menu_item_sk,
        sum(li.quantity) as total_quantity_sold,
        count(distinct li.order_id) as total_orders,
        sum(li.line_total) as total_revenue,
        sum(li.food_cost) as total_food_cost,
        sum(li.gross_profit) as total_gross_profit
    from fct_sales_line_item li
    group by li.menu_item_sk
),

-- Daily average calculation
daily_avg as (
    select
        li.menu_item_sk,
        count(distinct li.date_key) as days_sold,
        round(sum(li.quantity) / nullif(count(distinct li.date_key), 0), 2) as avg_daily_quantity
    from fct_sales_line_item li
    group by li.menu_item_sk
),

-- Rankings
ranked_items as (
    select
        menu_item_sk,
        total_quantity_sold,
        total_orders,
        total_revenue,
        total_food_cost,
        total_gross_profit,
        row_number() over (order by total_revenue desc) as revenue_rank,
        row_number() over (order by total_quantity_sold desc) as quantity_rank,
        row_number() over (order by total_gross_profit desc) as profit_rank
    from item_metrics
)

select
    dm.menu_item_sk,
    dm.item_id,
    
    -- Volume metrics
    coalesce(ri.total_quantity_sold, 0) as total_quantity_sold,
    coalesce(ri.total_orders, 0) as total_orders,
    
    -- Revenue metrics
    coalesce(ri.total_revenue, 0) as total_revenue,
    coalesce(ri.total_food_cost, 0) as total_food_cost,
    coalesce(ri.total_gross_profit, 0) as total_gross_profit,
    
    -- Calculated metrics
    coalesce(da.avg_daily_quantity, 0) as avg_daily_quantity,
    round(coalesce(ri.total_revenue, 0) / nullif(coalesce(ri.total_quantity_sold, 0), 0), 2) as revenue_per_unit,
    round(coalesce(ri.total_gross_profit, 0) / nullif(coalesce(ri.total_revenue, 0), 0) * 100, 2) as gross_margin_pct,
    
    -- Rankings
    ri.revenue_rank,
    ri.quantity_rank,
    ri.profit_rank,
    
    -- Metadata
    current_timestamp() as _created_at,
    current_timestamp() as _updated_at
    
from dim_menu_item dm
left join ranked_items ri on dm.menu_item_sk = ri.menu_item_sk
left join daily_avg da on dm.menu_item_sk = da.menu_item_sk
