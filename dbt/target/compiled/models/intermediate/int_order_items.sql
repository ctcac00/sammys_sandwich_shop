-- Intermediate model for order items
-- Adds derived fields and business logic enrichments

with staged_order_items as (
    select * from SAMMYS_SANDWICH_SHOP.DBT_DEV_staging.stg_order_items
),

enriched as (
    select
        -- Original fields
        order_item_id,
        order_id,
        item_id,
        quantity,
        unit_price,
        customizations,
        line_total,
        
        -- Derived fields
        customizations is not null and trim(customizations) != '' as has_customization,
        
        -- Metadata
        current_timestamp() as _enriched_at
        
    from staged_order_items
)

select * from enriched