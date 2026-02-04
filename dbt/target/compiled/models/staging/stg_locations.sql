-- Staging model for locations
-- Performs data type casting and basic cleaning

with source as (
    select * from SAMMYS_SANDWICH_SHOP.SAMMYS_RAW.locations
),

staged as (
    select
        -- Primary key
        location_id,
        
        -- Attributes
        trim(location_name) as location_name,
        trim(address) as address,
        trim(city) as city,
        upper(trim(state)) as state,
        trim(zip_code) as zip_code,
        trim(phone) as phone,
        trim(manager_employee_id) as manager_employee_id,
        
        -- Type conversions
        try_to_date(open_date, 'YYYY-MM-DD') as open_date,
        try_to_number(seating_capacity) as seating_capacity,
        case upper(trim(has_drive_thru))
            when 'TRUE' then true
            when 'YES' then true
            when '1' then true
            else false
        end as has_drive_thru,
        
        -- Metadata
        _loaded_at,
        _source_file
        
    from source
    where location_id is not null
)

select * from staged