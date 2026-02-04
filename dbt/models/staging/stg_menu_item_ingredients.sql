-- Staging model for menu item ingredients (recipe BOM)
-- Performs data type casting and basic cleaning

with source as (
    select * from {{ source('raw', 'menu_item_ingredients') }}
),

staged as (
    select
        -- Composite key
        trim(item_id) as item_id,
        trim(ingredient_id) as ingredient_id,
        
        -- Attributes
        try_to_number(quantity_required, 10, 4) as quantity_required,
        case upper(trim(is_optional))
            when 'TRUE' then true
            when 'YES' then true
            when '1' then true
            else false
        end as is_optional,
        coalesce(try_to_number(extra_charge, 10, 2), 0) as extra_charge,
        
        -- Metadata
        _loaded_at,
        _source_file
        
    from source
    where item_id is not null
      and ingredient_id is not null
)

select * from staged
