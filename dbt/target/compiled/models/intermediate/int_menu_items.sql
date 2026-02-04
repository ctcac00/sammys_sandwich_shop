-- Intermediate model for menu items
-- Adds derived fields and business logic enrichments

with staged_menu_items as (
    select * from SAMMYS_SANDWICH_SHOP.DBT_DEV_staging.stg_menu_items
),

enriched as (
    select
        -- Original fields
        item_id,
        item_name,
        category,
        subcategory,
        description,
        base_price,
        is_active,
        is_seasonal,
        calories,
        prep_time_minutes,
        introduced_date,
        
        -- Derived fields
        datediff('day', introduced_date, current_date()) as days_on_menu,
        
        -- Price tier
        case
            when base_price < 5 then 'Budget'
            when base_price < 8 then 'Standard'
            when base_price < 12 then 'Premium'
            else 'Gourmet'
        end as price_tier,
        
        -- Calorie category
        case
            when calories < 300 then 'Light'
            when calories < 500 then 'Moderate'
            when calories < 700 then 'Hearty'
            else 'Indulgent'
        end as calorie_category,
        
        -- Metadata
        current_timestamp() as _enriched_at
        
    from staged_menu_items
)

select * from enriched