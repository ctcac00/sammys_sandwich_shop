-- Report: Estimated Ingredient Usage
-- Estimates ingredient usage based on sales

{{ config(materialized='view') }}

with fct_sales_line_item as (
    select * from {{ ref('fct_sales_line_item') }}
),

int_menu_item_ingredients as (
    select * from {{ ref('int_menu_item_ingredients') }}
),

dim_menu_item as (
    select * from {{ ref('dim_menu_item') }}
    where is_current = true
),

dim_date as (
    select * from {{ ref('dim_date') }}
),

-- Aggregate sales by item
item_sales as (
    select
        li.menu_item_sk,
        sum(li.quantity) as total_quantity_sold
    from fct_sales_line_item li
    group by li.menu_item_sk
)

select 
    mii.ingredient_name,
    mii.ingredient_category,
    mii.unit_of_measure,
    mii.cost_per_unit,
    
    -- Usage metrics
    sum(mii.quantity_required * s.total_quantity_sold) as estimated_usage,
    round(sum(mii.ingredient_cost * s.total_quantity_sold), 2) as estimated_cost,
    
    -- Unique items using this ingredient
    count(distinct mii.item_id) as menu_items_using
    
from int_menu_item_ingredients mii
join dim_menu_item dm on mii.item_id = dm.item_id and dm.is_current = true
join item_sales s on dm.menu_item_sk = s.menu_item_sk
where not mii.is_optional  -- Only required ingredients
group by mii.ingredient_name, mii.ingredient_category, mii.unit_of_measure, mii.cost_per_unit
order by estimated_cost desc
