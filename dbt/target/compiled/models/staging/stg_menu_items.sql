-- Staging model for menu items
-- Performs data type casting and basic cleaning

with source as (
    select * from SAMMYS_SANDWICH_SHOP.SAMMYS_RAW.menu_items
),

staged as (
    select
        -- Primary key
        item_id,
        
        -- Attributes
        trim(item_name) as item_name,
        trim(category) as category,
        trim(subcategory) as subcategory,
        trim(description) as description,
        
        -- Pricing
        try_to_number(base_price, 10, 2) as base_price,
        
        -- Flags
        case upper(trim(is_active))
            when 'TRUE' then true
            when 'YES' then true
            when '1' then true
            else false
        end as is_active,
        case upper(trim(is_seasonal))
            when 'TRUE' then true
            when 'YES' then true
            when '1' then true
            else false
        end as is_seasonal,
        
        -- Nutritional/operational info
        try_to_number(calories) as calories,
        try_to_number(prep_time_minutes) as prep_time_minutes,
        try_to_date(introduced_date, 'YYYY-MM-DD') as introduced_date,
        
        -- Metadata
        _loaded_at,
        _source_file
        
    from source
    where item_id is not null
)

select * from staged