-- Staging model for order items (line items)
-- Performs data type casting and basic cleaning

with source as (
    select * from SAMMYS_SANDWICH_SHOP.SAMMYS_RAW.order_items
),

staged as (
    select
        -- Primary key
        order_item_id,
        
        -- Foreign keys
        trim(order_id) as order_id,
        trim(item_id) as item_id,
        
        -- Attributes
        try_to_number(quantity) as quantity,
        try_to_number(unit_price, 10, 2) as unit_price,
        trim(customizations) as customizations,
        try_to_number(line_total, 10, 2) as line_total,
        
        -- Metadata
        _loaded_at,
        _source_file
        
    from source
    where order_item_id is not null
      and order_id is not null
)

select * from staged