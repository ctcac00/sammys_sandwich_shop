-- Staging model for ingredients
-- Performs data type casting and basic cleaning

with source as (
    select * from SAMMYS_SANDWICH_SHOP.SAMMYS_RAW.ingredients
),

staged as (
    select
        -- Primary key
        ingredient_id,
        
        -- Attributes
        trim(ingredient_name) as ingredient_name,
        trim(category) as category,
        trim(unit_of_measure) as unit_of_measure,
        try_to_number(cost_per_unit, 10, 2) as cost_per_unit,
        trim(supplier_id) as supplier_id,
        
        -- Allergen info
        case upper(trim(is_allergen))
            when 'TRUE' then true
            when 'YES' then true
            when '1' then true
            else false
        end as is_allergen,
        trim(allergen_type) as allergen_type,
        
        -- Storage info
        try_to_number(shelf_life_days) as shelf_life_days,
        trim(storage_type) as storage_type,
        
        -- Metadata
        _loaded_at,
        _source_file
        
    from source
    where ingredient_id is not null
)

select * from staged