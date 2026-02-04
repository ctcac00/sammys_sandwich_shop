-- Fact table for sales at order level
-- Grain: one row per order

{{ config(materialized='table') }}

with orders as (
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

dim_payment_method as (
    select * from {{ ref('dim_payment_method') }}
),

dim_order_type as (
    select * from {{ ref('dim_order_type') }}
),

-- Get unknown customer SK for guest orders
unknown_customer as (
    select customer_sk
    from dim_customer
    where customer_id = '{{ var("unknown_customer_id") }}'
),

fact_sales as (
    select
        -- Order identifiers
        o.order_id,
        
        -- Dimension keys
        to_number(to_char(o.order_date, 'YYYYMMDD')) as date_key,
        hour(o.order_time) * 100 + minute(o.order_time) as time_key,
        coalesce(dc.customer_sk, uc.customer_sk) as customer_sk,
        de.employee_sk,
        dl.location_sk,
        pm.payment_method_sk,
        ot.order_type_sk,
        
        -- Degenerate dimensions
        o.order_status,
        
        -- Measures
        o.item_count,
        o.subtotal,
        o.tax_amount,
        o.discount_amount,
        o.tip_amount,
        o.total_amount,
        o.subtotal - o.discount_amount as net_sales,
        
        -- Derived measures
        round(o.discount_amount / nullif(o.subtotal, 0) * 100, 2) as discount_pct,
        round(o.tip_amount / nullif(o.subtotal, 0) * 100, 2) as tip_pct,
        round(o.subtotal / nullif(o.item_count, 0), 2) as avg_item_price,
        
        -- Flags
        o.has_discount,
        o.has_tip,
        o.is_guest_order,
        o.is_weekend,
        
        -- Metadata
        current_timestamp() as _created_at
        
    from orders o
    cross join unknown_customer uc
    left join dim_customer dc 
        on o.customer_id = dc.customer_id and dc.is_current = true
    left join dim_employee de 
        on o.employee_id = de.employee_id and de.is_current = true
    left join dim_location dl 
        on o.location_id = dl.location_id and dl.is_current = true
    left join dim_payment_method pm 
        on o.payment_method = pm.payment_method
    left join dim_order_type ot 
        on o.order_type = ot.order_type
)

select * from fact_sales
