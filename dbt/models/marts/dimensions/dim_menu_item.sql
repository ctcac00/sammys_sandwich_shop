-- Menu item dimension table
-- Includes food cost and margin calculations

{{ config(materialized='table') }}

with enriched_menu_items as (
    select * from {{ ref('int_menu_items') }}
),

-- Calculate food cost from ingredient BOM
item_costs as (
    select
        item_id,
        sum(ingredient_cost) as food_cost
    from {{ ref('int_menu_item_ingredients') }}
    where is_optional = false  -- Only required ingredients
    group by item_id
),

menu_item_dimension as (
    select
        mi.item_id,
        mi.item_name,
        mi.category,
        mi.subcategory,
        mi.description,
        mi.base_price,
        mi.price_tier,
        mi.calories,
        mi.calorie_category,
        mi.prep_time_minutes,
        mi.is_active,
        mi.is_seasonal,
        mi.introduced_date,
        
        -- Food cost and margins
        coalesce(ic.food_cost, 0) as food_cost,
        round(coalesce(ic.food_cost, 0) / nullif(mi.base_price, 0) * 100, 2) as food_cost_pct,
        round(mi.base_price - coalesce(ic.food_cost, 0), 2) as gross_margin,
        round((mi.base_price - coalesce(ic.food_cost, 0)) / nullif(mi.base_price, 0) * 100, 2) as gross_margin_pct,
        
        -- SCD Type 1 fields
        current_date() as effective_date,
        true as is_current
        
    from enriched_menu_items mi
    left join item_costs ic on mi.item_id = ic.item_id
)

select
    {{ dbt_utils.generate_surrogate_key(['item_id']) }} as menu_item_sk,
    *,
    current_timestamp() as _created_at,
    current_timestamp() as _updated_at
from menu_item_dimension
