-- Intermediate model for ingredients
-- Adds derived fields and business logic enrichments

with staged_ingredients as (
    select * from {{ ref('stg_ingredients') }}
),

enriched as (
    select
        -- Original fields
        ingredient_id,
        ingredient_name,
        category,
        unit_of_measure,
        cost_per_unit,
        supplier_id,
        is_allergen,
        allergen_type,
        shelf_life_days,
        storage_type,
        
        -- Derived fields - cost tier
        case
            when cost_per_unit < 0.50 then 'Low Cost'
            when cost_per_unit < 2.00 then 'Medium Cost'
            when cost_per_unit < 5.00 then 'High Cost'
            else 'Premium'
        end as cost_tier,
        
        -- Metadata
        current_timestamp() as _enriched_at
        
    from staged_ingredients
)

select * from enriched
