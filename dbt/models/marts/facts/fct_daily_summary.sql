-- Fact table for daily summary metrics
-- Grain: one row per location/day

{{ config(materialized='table') }}

with orders as (
    select * from {{ ref('int_orders') }}
    where order_status = 'Completed'
),

order_items as (
    select * from {{ ref('int_order_items') }}
),

dim_location as (
    select * from {{ ref('dim_location') }}
    where is_current = true
),

-- Aggregate order items to get item counts
order_item_counts as (
    select
        oi.order_id,
        sum(oi.quantity) as total_items
    from order_items oi
    group by oi.order_id
),

-- Daily aggregations
daily_metrics as (
    select
        o.order_date,
        o.location_id,
        
        -- Order metrics
        count(distinct o.order_id) as total_orders,
        sum(coalesce(oic.total_items, 0)) as total_items_sold,
        count(distinct case when not o.is_guest_order then o.customer_id end) as unique_customers,
        sum(case when o.is_guest_order then 1 else 0 end) as guest_orders,
        
        -- Revenue metrics
        sum(o.subtotal) as gross_sales,
        sum(o.discount_amount) as discounts_given,
        sum(o.subtotal - o.discount_amount) as net_sales,
        sum(o.tax_amount) as tax_collected,
        sum(o.tip_amount) as tips_received,
        sum(o.total_amount) as total_revenue,
        
        -- Calculated metrics
        round(avg(o.total_amount), 2) as avg_order_value,
        round(avg(coalesce(oic.total_items, 0)), 2) as avg_items_per_order,
        round(sum(o.discount_amount) / nullif(sum(o.subtotal), 0) * 100, 2) as discount_rate,
        
        -- Order type breakdown
        sum(case when o.order_type = 'Dine-In' then 1 else 0 end) as dine_in_orders,
        sum(case when o.order_type = 'Takeout' then 1 else 0 end) as takeout_orders,
        sum(case when o.order_type = 'Drive-Thru' then 1 else 0 end) as drive_thru_orders,
        
        -- Period breakdown
        sum(case when o.order_period = 'Breakfast' then 1 else 0 end) as breakfast_orders,
        sum(case when o.order_period = 'Lunch' then 1 else 0 end) as lunch_orders,
        sum(case when o.order_period = 'Dinner' then 1 else 0 end) as dinner_orders
        
    from orders o
    left join order_item_counts oic on o.order_id = oic.order_id
    group by o.order_date, o.location_id
)

select
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['dm.order_date', 'dm.location_id']) }} as daily_summary_id,
    
    -- Keys
    to_number(to_char(dm.order_date, 'YYYYMMDD')) as date_key,
    dl.location_sk,
    
    -- Metrics
    dm.total_orders,
    dm.total_items_sold,
    dm.unique_customers,
    dm.guest_orders,
    dm.gross_sales,
    dm.discounts_given,
    dm.net_sales,
    dm.tax_collected,
    dm.tips_received,
    dm.total_revenue,
    dm.avg_order_value,
    dm.avg_items_per_order,
    dm.discount_rate,
    dm.dine_in_orders,
    dm.takeout_orders,
    dm.drive_thru_orders,
    dm.breakfast_orders,
    dm.lunch_orders,
    dm.dinner_orders,
    
    -- Metadata
    current_timestamp() as _created_at
    
from daily_metrics dm
left join dim_location dl on dm.location_id = dl.location_id and dl.is_current = true
