-- Fact table for inventory snapshots
-- Grain: one row per location/ingredient/day



with inventory as (
    select * from SAMMYS_SANDWICH_SHOP.DBT_DEV_intermediate.int_inventory
),

dim_location as (
    select * from SAMMYS_SANDWICH_SHOP.DBT_DEV_marts.dim_location
    where is_current = true
),

dim_ingredient as (
    select * from SAMMYS_SANDWICH_SHOP.DBT_DEV_marts.dim_ingredient
    where is_current = true
),

fact_inventory as (
    select
        -- Keys
        i.inventory_id as inventory_snapshot_id,
        to_number(to_char(i.snapshot_date, 'YYYYMMDD')) as date_key,
        dl.location_sk,
        di.ingredient_sk,
        
        -- Measures
        i.quantity_on_hand,
        i.quantity_reserved,
        i.quantity_available,
        i.reorder_point,
        i.days_until_expiration,
        
        -- Calculated measures
        round(i.quantity_on_hand * di.cost_per_unit, 2) as stock_value,
        case 
            when i.reorder_quantity > 0 
            then round(i.quantity_available / nullif(i.reorder_quantity, 0) * 7, 2)  -- Days of supply estimate
            else null 
        end as days_of_supply,
        
        -- Status indicators
        i.needs_reorder,
        i.stock_status,
        case
            when i.days_until_expiration < 0 then 'Expired'
            when i.days_until_expiration <= 3 then 'Critical'
            when i.days_until_expiration <= 7 then 'Warning'
            else 'OK'
        end as expiration_risk,
        
        -- Metadata
        current_timestamp() as _created_at
        
    from inventory i
    left join dim_location dl on i.location_id = dl.location_id and dl.is_current = true
    left join dim_ingredient di on i.ingredient_id = di.ingredient_id and di.is_current = true
)

select * from fact_inventory