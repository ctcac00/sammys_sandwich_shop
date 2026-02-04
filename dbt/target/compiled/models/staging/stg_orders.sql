-- Staging model for orders
-- Performs data type casting and basic cleaning

with source as (
    select * from SAMMYS_SANDWICH_SHOP.SAMMYS_RAW.orders
),

staged as (
    select
        -- Primary key
        order_id,
        
        -- Foreign keys
        trim(customer_id) as customer_id,
        trim(employee_id) as employee_id,
        trim(location_id) as location_id,
        
        -- Date/time
        try_to_date(order_date, 'YYYY-MM-DD') as order_date,
        try_to_time(order_time, 'HH24:MI:SS') as order_time,
        
        -- Order details
        trim(order_type) as order_type,
        trim(order_status) as order_status,
        trim(payment_method) as payment_method,
        
        -- Amounts
        coalesce(try_to_number(subtotal, 10, 2), 0) as subtotal,
        coalesce(try_to_number(tax_amount, 10, 2), 0) as tax_amount,
        coalesce(try_to_number(discount_amount, 10, 2), 0) as discount_amount,
        coalesce(try_to_number(tip_amount, 10, 2), 0) as tip_amount,
        coalesce(try_to_number(total_amount, 10, 2), 0) as total_amount,
        
        -- Metadata
        _loaded_at,
        _source_file
        
    from source
    where order_id is not null
)

select * from staged