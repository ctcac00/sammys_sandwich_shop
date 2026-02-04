-- Staging model for customers
-- Performs data type casting and basic cleaning

with source as (
    select * from SAMMYS_SANDWICH_SHOP.SAMMYS_RAW.customers
),

staged as (
    select
        -- Primary key
        customer_id,
        
        -- Name fields
        initcap(trim(first_name)) as first_name,
        initcap(trim(last_name)) as last_name,
        
        -- Contact info
        lower(trim(email)) as email,
        trim(phone) as phone,
        trim(address) as address,
        trim(city) as city,
        upper(trim(state)) as state,
        trim(zip_code) as zip_code,
        
        -- Dates
        try_to_date(birth_date, 'YYYY-MM-DD') as birth_date,
        try_to_date(signup_date, 'YYYY-MM-DD') as signup_date,
        
        -- Loyalty program
        coalesce(trim(loyalty_tier), 'Bronze') as loyalty_tier,
        coalesce(try_to_number(loyalty_points), 0) as loyalty_points,
        trim(preferred_location) as preferred_location_id,
        
        -- Boolean conversion
        case upper(trim(marketing_opt_in))
            when 'TRUE' then true
            when 'YES' then true
            when '1' then true
            else false
        end as marketing_opt_in,
        
        -- Metadata
        _loaded_at,
        _source_file
        
    from source
    where customer_id is not null
)

select * from staged