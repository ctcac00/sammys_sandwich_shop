-- Report: Supplier Spend
-- Summarizes ingredient spend by supplier

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

int_suppliers as (
    select * from {{ ref('int_suppliers') }}
),

int_ingredients as (
    select * from {{ ref('int_ingredients') }}
),

-- Aggregate sales by item
item_sales as (
    select
        dm.item_id,
        sum(li.quantity) as total_quantity_sold
    from fct_sales_line_item li
    join dim_menu_item dm on li.menu_item_sk = dm.menu_item_sk
    group by dm.item_id
)

select 
    s.supplier_name,
    s.contact_name,
    s.email,
    s.phone,
    s.payment_terms,
    s.lead_time_days,
    s.is_active,
    
    -- Ingredient metrics
    count(distinct i.ingredient_id) as ingredient_count,
    
    -- Estimated spend based on sales
    round(sum(mii.ingredient_cost * coalesce(isales.total_quantity_sold, 0)), 2) as estimated_spend,
    
    -- Ingredient categories supplied
    listagg(distinct i.category, ', ') within group (order by i.category) as categories_supplied
    
from int_suppliers s
join int_ingredients i on s.supplier_id = i.supplier_id
join int_menu_item_ingredients mii on i.ingredient_id = mii.ingredient_id
left join item_sales isales on mii.item_id = isales.item_id
where not mii.is_optional
group by s.supplier_name, s.contact_name, s.email, s.phone, s.payment_terms, s.lead_time_days, s.is_active
order by estimated_spend desc
