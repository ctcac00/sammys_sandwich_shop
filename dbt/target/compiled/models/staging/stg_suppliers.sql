-- Staging model for suppliers
-- Performs data type casting and basic cleaning

with source as (
    select * from SAMMYS_SANDWICH_SHOP.SAMMYS_RAW.suppliers
),

staged as (
    select
        -- Primary key
        supplier_id,
        
        -- Attributes
        trim(supplier_name) as supplier_name,
        trim(contact_name) as contact_name,
        lower(trim(email)) as email,
        trim(phone) as phone,
        trim(address) as address,
        trim(city) as city,
        upper(trim(state)) as state,
        trim(zip_code) as zip_code,
        trim(payment_terms) as payment_terms,
        try_to_number(lead_time_days) as lead_time_days,
        
        -- Flags
        case upper(trim(is_active))
            when 'TRUE' then true
            when 'YES' then true
            when '1' then true
            else false
        end as is_active,
        
        -- Metadata
        _loaded_at,
        _source_file
        
    from source
    where supplier_id is not null
)

select * from staged