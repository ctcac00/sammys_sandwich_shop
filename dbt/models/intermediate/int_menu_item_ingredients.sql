-- Intermediate model for menu item ingredients (recipe BOM)
-- Adds ingredient cost calculations

with staged_bom as (
    select * from {{ ref('stg_menu_item_ingredients') }}
),

ingredients as (
    select * from {{ ref('int_ingredients') }}
),

enriched as (
    select
        -- Original fields
        bom.item_id,
        bom.ingredient_id,
        bom.quantity_required,
        bom.is_optional,
        bom.extra_charge,
        
        -- Calculate ingredient cost based on quantity
        round(bom.quantity_required * i.cost_per_unit, 4) as ingredient_cost,
        
        -- Ingredient details for reference
        i.ingredient_name,
        i.category as ingredient_category,
        i.unit_of_measure,
        i.cost_per_unit,
        
        -- Metadata
        current_timestamp() as _enriched_at
        
    from staged_bom bom
    left join ingredients i on bom.ingredient_id = i.ingredient_id
)

select * from enriched
