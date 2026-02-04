-- Ingredient dimension table



with enriched_ingredients as (
    select * from SAMMYS_SANDWICH_SHOP.DBT_DEV_intermediate.int_ingredients
),

suppliers as (
    select * from SAMMYS_SANDWICH_SHOP.DBT_DEV_intermediate.int_suppliers
),

ingredient_dimension as (
    select
        i.ingredient_id,
        i.ingredient_name,
        i.category,
        i.unit_of_measure,
        i.cost_per_unit,
        i.cost_tier,
        i.supplier_id,
        s.supplier_name,
        i.is_allergen,
        i.allergen_type,
        i.shelf_life_days,
        i.storage_type,
        
        -- SCD Type 1 fields
        current_date() as effective_date,
        true as is_current
        
    from enriched_ingredients i
    left join suppliers s on i.supplier_id = s.supplier_id
)

select
    md5(cast(coalesce(cast(ingredient_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as ingredient_sk,
    *,
    current_timestamp() as _created_at,
    current_timestamp() as _updated_at
from ingredient_dimension