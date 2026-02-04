-- Report: Ingredient Cost Breakdown
-- Shows ingredient costs per menu item



with int_menu_item_ingredients as (
    select * from SAMMYS_SANDWICH_SHOP.DBT_DEV_intermediate.int_menu_item_ingredients
),

int_menu_items as (
    select * from SAMMYS_SANDWICH_SHOP.DBT_DEV_intermediate.int_menu_items
)

select 
    mi.item_name,
    mi.category,
    mi.base_price,
    mii.ingredient_name,
    mii.ingredient_category,
    mii.quantity_required,
    mii.unit_of_measure,
    mii.cost_per_unit,
    mii.ingredient_cost,
    mii.is_optional,
    mii.extra_charge,
    
    -- Calculate percentage of total item cost
    round(mii.ingredient_cost / nullif(
        sum(case when not mii.is_optional then mii.ingredient_cost end) 
        over (partition by mi.item_id), 0) * 100, 2) as pct_of_item_cost
    
from int_menu_item_ingredients mii
join int_menu_items mi on mii.item_id = mi.item_id
order by mi.item_name, mii.ingredient_cost desc