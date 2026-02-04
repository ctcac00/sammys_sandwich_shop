-- Report: Daily Item Sales
-- Daily sales breakdown by menu item

{{ config(materialized='view') }}

with fct_sales_line_item as (
    select * from {{ ref('fct_sales_line_item') }}
),

dim_date as (
    select * from {{ ref('dim_date') }}
),

dim_menu_item as (
    select * from {{ ref('dim_menu_item') }}
    where is_current = true
)

select 
    dd.full_date as order_date,
    dd.day_name,
    dd.is_weekend,
    dm.item_name,
    dm.category,
    dm.subcategory,
    
    -- Metrics
    sum(li.quantity) as quantity_sold,
    sum(li.line_total) as revenue,
    sum(li.gross_profit) as gross_profit,
    count(distinct li.order_id) as order_count
    
from fct_sales_line_item li
join dim_date dd on li.date_key = dd.date_key
join dim_menu_item dm on li.menu_item_sk = dm.menu_item_sk
group by dd.full_date, dd.day_name, dd.is_weekend, dm.item_name, dm.category, dm.subcategory
order by dd.full_date desc, revenue desc
