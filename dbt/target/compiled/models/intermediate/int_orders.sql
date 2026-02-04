-- Intermediate model for orders
-- Adds derived fields and business logic enrichments

with staged_orders as (
    select * from SAMMYS_SANDWICH_SHOP.DBT_DEV_staging.stg_orders
),

order_items as (
    select 
        order_id,
        count(*) as item_count
    from SAMMYS_SANDWICH_SHOP.DBT_DEV_staging.stg_order_items
    group by order_id
),

enriched as (
    select
        -- Original fields
        o.order_id,
        o.customer_id,
        o.employee_id,
        o.location_id,
        o.order_date,
        o.order_time,
        o.order_type,
        o.order_status,
        o.payment_method,
        o.subtotal,
        o.tax_amount,
        o.discount_amount,
        o.tip_amount,
        o.total_amount,
        
        -- Combined datetime
        to_timestamp_ntz(o.order_date || ' ' || o.order_time) as order_datetime,
        
        -- Day of week
        dayname(o.order_date) as order_day_of_week,
        
        -- Hour of order
        hour(o.order_time) as order_hour,
        
        -- Time period
        case
            when hour(o.order_time) between 6 and 10 then 'Breakfast'
            when hour(o.order_time) between 11 and 14 then 'Lunch'
            when hour(o.order_time) between 15 and 17 then 'Afternoon'
            when hour(o.order_time) between 18 and 21 then 'Dinner'
            else 'Late Night'
        end as order_period,
        
        -- Weekend flag
        dayofweek(o.order_date) in (0, 6) as is_weekend,
        
        -- Boolean flags
        o.discount_amount > 0 as has_discount,
        o.tip_amount > 0 as has_tip,
        o.customer_id is null as is_guest_order,
        
        -- Item count from order items
        coalesce(oi.item_count, 0) as item_count,
        
        -- Metadata
        current_timestamp() as _enriched_at
        
    from staged_orders o
    left join order_items oi on o.order_id = oi.order_id
)

select * from enriched