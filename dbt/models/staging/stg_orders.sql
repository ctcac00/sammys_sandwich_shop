-- Staging model for orders
-- Performs data type casting and basic cleaning

with source as (
    select * from {{ source('raw', 'orders') }}
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
        {{ safe_cast_number('subtotal') }} as subtotal,
        {{ safe_cast_number('tax_amount') }} as tax_amount,
        {{ safe_cast_number('discount_amount') }} as discount_amount,
        {{ safe_cast_number('tip_amount') }} as tip_amount,
        {{ safe_cast_number('total_amount') }} as total_amount,
        
        -- Metadata
        _loaded_at,
        _source_file
        
    from source
    where order_id is not null
)

select * from staged
