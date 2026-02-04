-- Staging model for inventory snapshots
-- Performs data type casting and basic cleaning

with source as (
    select * from {{ source('raw', 'inventory') }}
),

staged as (
    select
        -- Primary key
        inventory_id,
        
        -- Foreign keys
        trim(location_id) as location_id,
        trim(ingredient_id) as ingredient_id,
        
        -- Dates
        try_to_date(snapshot_date, 'YYYY-MM-DD') as snapshot_date,
        try_to_date(last_restock_date, 'YYYY-MM-DD') as last_restock_date,
        try_to_date(expiration_date, 'YYYY-MM-DD') as expiration_date,
        
        -- Quantities
        coalesce(try_to_number(quantity_on_hand), 0) as quantity_on_hand,
        coalesce(try_to_number(quantity_reserved), 0) as quantity_reserved,
        coalesce(try_to_number(reorder_point), 0) as reorder_point,
        coalesce(try_to_number(reorder_quantity), 0) as reorder_quantity,
        
        -- Metadata
        _loaded_at,
        _source_file
        
    from source
    where inventory_id is not null
)

select * from staged
