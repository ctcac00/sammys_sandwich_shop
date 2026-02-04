-- Intermediate model for suppliers
-- Adds derived fields and business logic enrichments

with staged_suppliers as (
    select * from SAMMYS_SANDWICH_SHOP.DBT_DEV_staging.stg_suppliers
),

enriched as (
    select
        -- Original fields
        supplier_id,
        supplier_name,
        contact_name,
        email,
        phone,
        address,
        city,
        state,
        zip_code,
        payment_terms,
        lead_time_days,
        is_active,
        
        -- Derived fields - extract payment days from terms
        case
            when payment_terms ilike '%net 10%' then 10
            when payment_terms ilike '%net 15%' then 15
            when payment_terms ilike '%net 30%' then 30
            when payment_terms ilike '%net 45%' then 45
            when payment_terms ilike '%net 60%' then 60
            else 30  -- default
        end as payment_days,
        
        -- Metadata
        current_timestamp() as _enriched_at
        
    from staged_suppliers
)

select * from enriched