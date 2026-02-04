-- Intermediate model for inventory snapshots
-- Adds derived fields and business logic enrichments

with staged_inventory as (
    select * from SAMMYS_SANDWICH_SHOP.DBT_DEV_staging.stg_inventory
),

enriched as (
    select
        -- Original fields
        inventory_id,
        location_id,
        ingredient_id,
        snapshot_date,
        quantity_on_hand,
        quantity_reserved,
        reorder_point,
        reorder_quantity,
        last_restock_date,
        expiration_date,
        
        -- Derived fields
        quantity_on_hand - quantity_reserved as quantity_available,
        datediff('day', current_date(), expiration_date) as days_until_expiration,
        quantity_on_hand <= reorder_point as needs_reorder,
        
        -- Stock status
        case
            when quantity_on_hand = 0 then 'Out of Stock'
            when quantity_on_hand <= reorder_point then 'Low Stock'
            when quantity_on_hand <= reorder_point * 2 then 'Adequate'
            else 'Well Stocked'
        end as stock_status,
        
        -- Metadata
        current_timestamp() as _enriched_at
        
    from staged_inventory
)

select * from enriched