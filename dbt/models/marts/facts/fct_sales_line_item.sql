-- Fact table for sales at line item level
-- Grain: one row per order item

{{ config(materialized='table') }}

with order_items as (
    select * from {{ ref('int_order_items') }}
),

orders as (
    select * from {{ ref('int_orders') }}
    where order_status = 'Completed'
),

dim_customer as (
    select * from {{ ref('dim_customer') }}
    where is_current = true
),

dim_employee as (
    select * from {{ ref('dim_employee') }}
    where is_current = true
),

dim_location as (
    select * from {{ ref('dim_location') }}
    where is_current = true
),

dim_menu_item as (
    select * from {{ ref('dim_menu_item') }}
    where is_current = true
),

-- Get unknown customer SK for guest orders
unknown_customer as (
    select customer_sk
    from dim_customer
    where customer_id = '{{ var("unknown_customer_id") }}'
),

fact_line_items as (
    select
        -- Keys
        oi.order_item_id,
        oi.order_id,
        to_number(to_char(o.order_date, 'YYYYMMDD')) as date_key,
        coalesce(dc.customer_sk, uc.customer_sk) as customer_sk,
        de.employee_sk,
        dl.location_sk,
        dm.menu_item_sk,
        
        -- Measures
        oi.quantity,
        oi.unit_price,
        oi.line_total,
        coalesce(dm.food_cost, 0) * oi.quantity as food_cost,
        oi.line_total - (coalesce(dm.food_cost, 0) * oi.quantity) as gross_profit,
        round((oi.line_total - (coalesce(dm.food_cost, 0) * oi.quantity)) / nullif(oi.line_total, 0) * 100, 2) as gross_margin_pct,
        
        -- Flags
        oi.has_customization,
        
        -- Metadata
        current_timestamp() as _created_at
        
    from order_items oi
    join orders o on oi.order_id = o.order_id
    cross join unknown_customer uc
    left join dim_customer dc 
        on o.customer_id = dc.customer_id and dc.is_current = true
    left join dim_employee de 
        on o.employee_id = de.employee_id and de.is_current = true
    left join dim_location dl 
        on o.location_id = dl.location_id and dl.is_current = true
    left join dim_menu_item dm 
        on oi.item_id = dm.item_id and dm.is_current = true
)

select * from fact_line_items
